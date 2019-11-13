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
            sys.stdout = StdoutRedirector(os.path.join(self.param.tmpDir, "mycdn.out"))
            sys.stderr = sys.stdout

            logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
            logging.getLogger().setLevel(logging.INFO)

            # create scheduler
            self.scheduler = BackgroundScheduler(daemon=False)
            logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)

            # create mainloop
            self.param.mainloop = GLib.MainLoop()

            # write pid file
            with open(os.path.join(self.param.tmpDir, "mycdn.pid"), "w") as f:
                f.write(str(os.getpid()))

            # load plugins
            self._loadPlugins()
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
    
    def _loadPlugins(self):
        for fn in os.listdir(self.etcDir):
            if not fn.endswith(".conf"):            
                continue
            pluginName = fn.replace(".conf", "")
            pluginPath = os.path.join(self.param.pluginsDir, pluginName)
            if not os.path.isdir(pluginPath):
                raise Exception("Invalid configuration file %s" % (fn))
            pluginObj = McPlugin(pluginName, pluginPath)
            self.pluginList.append(pluginObj)

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
