#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import time
import signal
import shutil
import logging
import datetime
from gi.repository import GLib
from apscheduler.schedulers.background import BackgroundScheduler
from mc_util import McUtil
from mc_util import StdoutRedirector
from mc_api_server import McApiServer
from mc_plugin import SourceObjectInnerApi
from mc_plugin import MirrorObjectInnerApi


class McDaemon:

    def __init__(self, param):
        self.param = param
        self.apiServer = None
        self.scheduler = None
        self.sourceObjects = []
        self.helperObjects = dict()         # <helper_name, helper_object>
        self.mirrorRefreshThreads = dict()  # <source_name, mirror_refresh_thread_list>
        self.mirrorObjects = dict()         # <source_name, mirror_object_list>

    def run(self):
        McUtil.mkDirAndClear(self.param.tmpDir)
        try:
            sys.stdout = StdoutRedirector(os.path.join(self.param.tmpDir, "mycdn-controller.out"))
            sys.stderr = sys.stdout

            logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
            logging.getLogger().setLevel(logging.INFO)

            # create scheduler
            self.scheduler = BackgroundScheduler(daemon=False)
            logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)

            # create mainloop
            self.param.mainloop = GLib.MainLoop()

            # write pid file
            with open(os.path.join(self.param.tmpDir, "mycdn-controller.pid"), "w") as f:
                f.write(str(os.getpid()))

            # start api service
            self.apiServer = McApiServer(self.param)
            logging.info("api-service started.")

            # load objects
            self._loadObjects()
            self.scheduler.add_job(self._refreshMirrorObjects, "date")

            # start main loop
            logging.info("Mainloop begins.")
            GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGINT, self._sigHandlerINT, None)
            GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGTERM, self._sigHandlerTERM, None)
            self.scheduler.start()
            self.param.mainloop.run()
            logging.info("Mainloop exits.")
        finally:
            self.scheduler.shutdown()
            logging.shutdown()
            shutil.rmtree(self.param.tmpDir)

    def _sigHandlerINT(self, signum):
        logging.info("SIGINT received.")
        self.param.mainloop.quit()
        return True

    def _sigHandlerTERM(self, signum):
        logging.info("SIGTERM received.")
        self.param.mainloop.quit()
        return True

    def _loadObjects(self):
        # load helper objects
        import helpers
        for basename in os.listdir(self.param.helpersDir):
            if not basename.endswith(".py"):
                continue
            basename = basename[:-3]        # remove ".py" postfix
            exec("from helpers.%s import HelperObject" % (basename))
            hobj = eval("helpers.%s.HelperObject(self.param)" % (basename))
            self.helperObjects[basename] = hobj
            logging.info("Helper \"%s\" loaded." % (basename))

        # load source objects
        import sources
        for basename in os.listdir(self.param.sourcesDir):
            if not basename.endswith(".py"):
                continue
            basename = basename[:-3]        # remove ".py" postfix
            exec("from sources.%s import SourceObject" % (basename))
            sobj = eval("sources.%s.SourceObject()" % (basename))
            self.sourceObjects.append(sobj)
            self.mirrorObjects[McUtil.objpath(sobj)] = []
            logging.info("Source \"%s\" loaded." % (sobj.name))

        # initialize source objects
        for sobj in self.sourceObjects:
            if hasattr(sobj, "helpers_needed"):
                for name in sobj.helpers_needed:
                    hobj = self.helperObjects[name]
                    if McUtil.is_method(hobj, "check_source"):
                        hobj.check_source(sobj)
            api = SourceObjectInnerApi(self.param, self, sobj)
            sobj.init2(api)
            logging.info("Source \"%s\" initialized." % (sobj.name))

        # load mirror refresh threads
        import mirrors
        for sobj in self.sourceObjects:
            mobjDir = os.path.join(self.param.mirrorsDir, McUtil.objpath(sobj))
            self.mirrorRefreshThreads[McUtil.objpath(sobj)] = []
            if os.path.exists(mobjDir):
                for basename in os.listdir(mobjDir):
                    if not basename.endswith(".py"):
                        continue
                    basename = basename[:-3]        # remove ".py" postfix
                    exec("from mirrors.%s.%s import MirrorRefreshThread" % (McUtil.objpath(sobj), basename))
                    cb = _MirrorCallback(self, sobj)
                    mrThread = eval("mirrors.%s.%s.MirrorRefreshThread(cb)" % (McUtil.objpath(sobj), basename))
                    cb.mirrorRefreshThread = mrThread
                    self.mirrorRefreshThreads[McUtil.objpath(sobj)].append(mrThread)

    def _refreshMirrorObjects(self):
        logging.info("Refresh task started.")

        # identify all the mirror objects
        for mobj in McUtil.joinLists(self.mirrorObjects.values()):
            mobj.__old = 1

        # start and wait refresh threads
        for sobj in self.sourceObjects:
            for mthread in self.mirrorRefreshThreads[McUtil.objpath(sobj)]:
                mthread.start()
        for sobj in self.sourceObjects:
            for mthread in self.mirrorRefreshThreads[McUtil.objpath(sobj)]:
                mthread.join()

        # delete unused mirror objects
        for sobj in self.sourceObjects:
            list2 = []
            for mobj in self.mirrorObjects[McUtil.objpath(sobj)]:
                if not hasattr(mobj, "__old"):
                    list2.append(mobj)
            self.mirrorObjects[McUtil.objpath(sobj)] = list2

        logging.info("Refresh task completed.")

        self.scheduler.add_job(self._syncMirrorObjects, "date")

    def _syncMirrorObjects(self):
        syncThreads = dict()

        # start sync threads
        for obj in self.sourceObjects:
            t = obj.getSyncThread()
            if t is None:
                continue
            syncThreads[obj.name] = t
            t.start()
            logging.info("Sync task for source \"%s\" started." % (obj.name))

        # wait sync threads
        while True:
            time.sleep(60)
            exists = False
            for name, tobj in syncThreads.items():
                if tobj is None:
                    continue
                if tobj.is_alive():
                    logging.info("Sync task for source \"%s\" is in progress, %d%%." % (name, tobj.get_progress()))
                    exists = True
                else:
                    syncThreads[name] = None
            if not exists:
                break

        # wait sync threads
        t = datetime.datetime.now()
        t = datetime.datetime(t.year, t.month, t.day)
        t = t + datetime.timedelta(days=1)
        self.scheduler.add_job(self._refreshMirrorObjects, "date", run_date=t)


class _MirrorCallback:

    def __init__(self, daemon, source):
        self.daemon = daemon
        self.source = source
        self.mirrorRefreshThread = None

    def __call__(self, mirror):
        # the mirror already exist
        for mobj in self.daemon.mirrorObjects[McUtil.objpath(self.source)]:
            if mobj.__mirror_refresh_thread == self.mirrorRefreshThread and mobj.name == mirror.name:
                del mobj.__old
                return

        # new mirror
        mirror.__mirror_refresh_thread = self.mirrorRefreshThread
        logging.info("Mirror \"%s\" for source \"%s\" loaded." % (mirror.name, self.source.name))

        if hasattr(self.source, "helpers_needed"):
            for name in self.source.helpers_needed:
                hobj = self.daemon.helperObjects[name]
                if McUtil.is_method(hobj, "check_mirror"):
                    hobj.check_mirror(mirror)
        if hasattr(mirror, "helpers_needed"):
            for name in mirror.helpers_needed:
                hobj = self.daemon.helperObjects[name]
                if McUtil.is_method(hobj, "check_mirror"):
                    hobj.check_mirror(mirror)

        self.daemon.mirrorObjects[McUtil.objpath(self.source)].append(mirror)

        api = MirrorObjectInnerApi(self.daemon.param, self.source, mirror)
        mirror.init2(api)
        logging.info("Mirror \"%s\" for source \"%s\" initialized." % (mirror.name, self.source.name))