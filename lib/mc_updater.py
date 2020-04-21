#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import json
import struct
import socket
import logging
import subprocess
from datetime import datetime
from gi.repository import GLib
from mc_util import McUtil
from mc_util import RotatingFile
from mc_util import GLibIdleInvoker
from mc_util import GLibCronScheduler
from mc_util import UnixDomainSocketApiServer
from mc_param import McConst


class McMirrorSiteUpdater:

    MIRROR_SITE_UPDATE_STATUS_INIT = 0
    MIRROR_SITE_UPDATE_STATUS_INITING = 1
    MIRROR_SITE_UPDATE_STATUS_INIT_FAIL = 2
    MIRROR_SITE_UPDATE_STATUS_IDLE = 3
    MIRROR_SITE_UPDATE_STATUS_SYNCING = 4
    MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL = 5

    MIRROR_SITE_RE_INIT_INTERVAL = 60

    def __init__(self, param):
        self.param = param

        self.invoker = GLibIdleInvoker()
        self.scheduler = GLibCronScheduler()

        self.updaterDict = dict()                                       # dict<mirror-id,updater-object>
        for ms in self.param.mirrorSiteDict.values():
            # initialize data directory
            fullDir = os.path.join(McConst.cacheDir, ms.dataDir)
            if not os.path.exists(fullDir):
                os.makedirs(fullDir)
                McUtil.touchFile(_initFlagFile(ms))

            # create updater object
            self.updaterDict[ms.id] = _OneMirrorSiteUpdater(self, ms)

        self.apiServer = _ApiServer(self)

    def dispose(self):
        self.apiServer.dispose()
        for updater in self.updaterDict.values():
            if updater.status == self.MIRROR_SITE_UPDATE_STATUS_INITING:
                updater.initStop()
            elif updater.status == self.MIRROR_SITE_UPDATE_STATUS_SYNCING:
                updater.updateStop()
        # FIXME, should use g_main_context_iteration to wait all the updaters to stop
        self.scheduler.dispose()
        self.invoker.dispose()

    def isMirrorSiteInitialized(self, mirrorSiteId):
        ret = self.updaterDict[mirrorSiteId].status
        if self.MIRROR_SITE_UPDATE_STATUS_INIT <= ret <= self.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL:
            return False
        return True

    def getMirrorSiteUpdateStatus(self, mirrorSiteId):
        return self.updaterDict[mirrorSiteId].status


class _OneMirrorSiteUpdater:

    def __init__(self, parent, mirrorSite):
        self.param = parent.param
        self.invoker = parent.invoker
        self.scheduler = parent.scheduler
        self.mirrorSite = mirrorSite

        bInit = os.path.exists(_initFlagFile(self.mirrorSite))

        if bInit:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
        else:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
        self.progress = -1
        self.proc = None
        self.pidWatch = None
        self.stdoutWatch = None
        self.logger = None
        self.excInfo = None
        self.holdFor = None

        if bInit:
            self.invoker.add(self.initStart)
        else:
            self.invoker.add(lambda: self.param.advertiser.advertiseMirrorSite(self.mirrorSite.id))
            self.scheduler.addJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)

    def initStart(self):
        assert self.status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL]

        try:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
            self.progress = 0
            self.proc = self._createInitOrUpdateProc()
            self.pidWatch = GLib.child_watch_add(self.proc.pid, self.initExitCallback)
            self.stdoutWatch = GLib.io_add_watch(self.proc.stdout, GLib.IO_IN, self.stdoutCallback)
            self.logger = RotatingFile(os.path.join(McConst.logDir, "%s.log" % (self.mirrorSite.id)), McConst.updaterLogFileSize, McConst.updaterLogFileCount)
            self.excInfo = None
            self.holdFor = None
            logging.info("Mirror site \"%s\" initialization starts." % (self.mirrorSite.id))
        except Exception:
            self.reInitHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL, self._reInitCallback)
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL)
            logging.error("Mirror site \"%s\" initialization failed, re-initialize in %d seconds." % (self.mirrorSite.id, McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL), exc_info=True)

    def initStop(self):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        self.proc.terminate()

    def initProgressCallback(self, progress):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        assert 0 <= progress <= 100 and progress >= self.progress
        if progress > self.progress:
            self.progress = progress
            logging.info("Mirror site \"%s\" initialization progress %d%%." % (self.mirrorSite.id, self.progress))

    def initErrorCallback(self, exc_info):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        assert self.excInfo is None
        self.excInfo = exc_info

    def initErrorAndHoldForCallback(self, seconds, exc_info):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        assert self.excInfo is None
        self.excInfo = exc_info
        self.holdFor = seconds

    def initExitCallback(self, status, data):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        self.proc = None

        if status == 0:
            McUtil.forceDelete(_initFlagFile(self.mirrorSite))
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            logging.info("Mirror site \"%s\" initialization finished." % (self.mirrorSite.id))
            self.invoker.add(lambda: self.param.advertiser.advertiseMirrorSite(self.mirrorSite.id))
            self.scheduler.addJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)
        else:
            holdFor = self.holdFor
            excInfo = (None, None, None)       # FIXME
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL)
            if holdFor is None:
                logging.error("Mirror site \"%s\" initialization failed, re-initialize in %d seconds." % (self.mirrorSite.id, McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL), exc_info=excInfo)
                self.reInitHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL, self._reInitCallback)
            else:
                logging.error("Mirror site \"%s\" initialization failed, hold for %d seconds before re-initialization." % (self.mirrorSite.id, holdFor), exc_info=excInfo)
                self.reInitHandler = GLib.timeout_add_seconds(holdFor, self._reInitCallback)

    def updateStart(self, schedDatetime):
        assert self.status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL]
        tstr = schedDatetime.strftime("%Y-%m-%d %H:%M")

        if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING:
            logging.info("Mirror site \"%s\" updating ignored on \"%s\", last update is not finished." % (self.mirrorSite.id, tstr))
            return

        try:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
            self.progress = 0
            self.proc = self._createInitOrUpdateProc(schedDatetime)
            self.pidWatch = GLib.child_watch_add(self.proc.pid, self.updateExitCallback)
            self.stdoutWatch = GLib.io_add_watch(self.proc.stdout, GLib.IO_IN, self.stdoutCallback)
            self.logger = RotatingFile(os.path.join(McConst.logDir, "%s.log" % (self.mirrorSite.id)), McConst.updaterLogFileSize, McConst.updaterLogFileCount)
            self.excInfo = None
            self.holdFor = None
            logging.info("Mirror site \"%s\" update triggered on \"%s\"." % (self.mirrorSite.id, tstr))
        except Exception:
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL)
            logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id), exc_info=True)

    def updateStop(self):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
        self.proc.terminate()

    def updateProgressCallback(self, progress):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
        assert 0 <= progress <= 100 and progress >= self.progress
        if progress > self.progress:
            self.progress = progress
            logging.info("Mirror site \"%s\" update progress %d%%." % (self.mirrorSite.id, self.progress))

    def updateErrorCallback(self, exc_info):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
        assert self.excInfo is None
        self.excInfo = exc_info

    def updateErrorAndHoldForCallback(self, seconds, exc_info):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
        assert self.excInfo is None
        self.excInfo = exc_info
        self.holdFor = seconds

    def updateExitCallback(self, status, data):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
        self.proc = None

        if status == 0:
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            logging.info("Mirror site \"%s\" update finished." % (self.mirrorSite.id))
            self.scheduler.addJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)
        else:
            holdFor = self.holdFor
            excInfo = (None, None, None)       # FIXME
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL)
            if holdFor is None:
                logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id), exc_info=excInfo)
            else:
                self.scheduler.pauseJob(self.mirrorSite.id, datetime.now() + datetime.timedelta(seconds=holdFor))
                logging.error("Mirror site \"%s\" updates failed, hold for %d seconds." % (self.mirrorSite.id, holdFor), exc_info=excInfo)

    def stdoutCallback(self, source, cb_condition):
        self.logger.info(source.read())

    def _clearVars(self, status):
        self.holdFor = None
        self.excInfo = None
        if self.logger is not None:
            self.logger.close()
            self.logger = None
        if self.stdoutWatch is not None:
            GLib.source_remove(self.stdoutWatch)
            self.stdoutWatch = None
        if self.pidWatch is not None:
            GLib.source_remove(self.pidWatch)
            self.pidWatch = None
        if self.proc is not None:
            self.proc.terminate()
            self.proc.wait()
            self.proc = None
        self.progress = -1
        self.status = status

    def _createInitOrUpdateProc(self, schedDatetime=None):
        cmd = []

        # executable
        if schedDatetime is None:
            cmd.append(self.mirrorSite.initializerExe)
        else:
            cmd.append(self.mirrorSite.updaterExe)

        # argument
        if True:
            args = {
                "data-directory": self.mirrorSite.dataDir,
                "debug-flag": "",
                "country": "CN",
                "location": "",
            }
            if schedDatetime is not None:
                args["sched-datetime"] = datetime.strftime(schedDatetime, "%Y-%m-%d %H:%M")
        cmd.append(json.dumps(args))

        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    def _reInitCallback(self):
        del self.reInitHandler
        self.initStart()
        return False


class _ApiServer(UnixDomainSocketApiServer):

    def __init__(self, parent):
        self.updaterDict = parent.updaterDict
        super().__init__(McConst.apiServerFile, self._clientInitFunc, self._clientNoitfyFunc)

    def _clientInitFunc(self, sock):
        pid = None
        if True:
            pattern = "=iii"
            length = struct.calcsize(pattern)
            ret = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, length)
            pid, uid, gid = struct.unpack(pattern, ret)

        for mirrorId, obj in self.updaterDict.items():
            if obj.proc is not None and obj.proc.pid == pid:
                return mirrorId
        return None

    def _clientNoitfyFunc(self, mirrorId, data):
        obj = self.updaterDict[mirrorId]
        if obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
            if data["message"] == "progress":
                obj.initProgressCallback(data["data"]["progress"])
            elif data["message"] == "error":
                obj.initErrorCallback(data["data"]["exc_info"])
            elif data["message"] == "error-and-hold-for":
                obj.initErrorAndHoldForCallback(data["data"]["seconds"], data["data"]["exc_info"])
            else:
                assert False
        elif obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING:
            if data["message"] == "progress":
                obj.updateProgressCallback(data["data"]["progress"])
            elif data["message"] == "error":
                obj.updateErrorCallback(data["data"]["exc_info"])
            elif data["message"] == "error-and-hold-for":
                obj.updateErrorAndHoldForCallback(data["seconds"], data["data"]["exc_info"])
            else:
                assert False
        else:
            assert False


def _initFlagFile(mirrorSite):
    return os.path.join(McConst.cacheDir, mirrorSite.dataDir + ".uninitialized")
