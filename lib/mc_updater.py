#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import json
import fcntl
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
        if progress > self.progress:
            self.progress = progress
            logging.info("Mirror site \"%s\" initialization progress %d%%." % (self.mirrorSite.id, self.progress))
        elif progress == self.progress:
            pass
        else:
            raise Exception("invalid progress")

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

        try:
            GLib.spawn_check_exit_status(status)
            bSuccess = True
        except GLib.GError:
            bSuccess = False

        if bSuccess:
            McUtil.forceDelete(_initFlagFile(self.mirrorSite))
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            logging.info("Mirror site \"%s\" initialization finished." % (self.mirrorSite.id))
            self.invoker.add(lambda: self.param.advertiser.advertiseMirrorSite(self.mirrorSite.id))
            self.scheduler.addJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)
        else:
            holdFor = self.holdFor
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL)
            if holdFor is None:
                logging.error("Mirror site \"%s\" initialization failed, re-initialize in %d seconds." % (self.mirrorSite.id, McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL))
                self.reInitHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL, self._reInitCallback)
            else:
                logging.error("Mirror site \"%s\" initialization failed, hold for %d seconds before re-initialization." % (self.mirrorSite.id, holdFor))
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
        if progress > self.progress:
            self.progress = progress
            logging.info("Mirror site \"%s\" update progress %d%%." % (self.mirrorSite.id, self.progress))
        elif progress == self.progress:
            pass
        else:
            raise Exception("invalid progress")

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

        try:
            GLib.spawn_check_exit_status(status)
            bSuccess = True
        except GLib.GError:
            bSuccess = False

        if bSuccess:
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            logging.info("Mirror site \"%s\" update finished." % (self.mirrorSite.id))
            self.scheduler.addJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)
        else:
            holdFor = self.holdFor
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL)
            if holdFor is None:
                logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id))
            else:
                self.scheduler.pauseJob(self.mirrorSite.id, datetime.now() + datetime.timedelta(seconds=holdFor))
                logging.error("Mirror site \"%s\" updates failed, hold for %d seconds." % (self.mirrorSite.id, holdFor))

    def stdoutCallback(self, source, cb_condition):
        try:
            self.logger.write(source.read())
        finally:
            return True

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

        # create log directory
        logDir = os.path.join(McConst.logDir, self.mirrorSite.id)
        McUtil.ensureDir(logDir)

        # executable
        if schedDatetime is None:
            cmd.append(self.mirrorSite.initializerExe)
        else:
            cmd.append(self.mirrorSite.updaterExe)

        # argument
        if True:
            args = {
                "data-directory": self.mirrorSite.dataDir,
                "log-directory": logDir,
                "debug-flag": "",
                "country": "CN",
                "location": "",
            }
            if schedDatetime is not None:
                args["sched-datetime"] = datetime.strftime(schedDatetime, "%Y-%m-%d %H:%M")
        cmd.append(json.dumps(args))

        # create process
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        fcntl.fcntl(proc.stdout, fcntl.F_SETFL, fcntl.fcntl(proc.stdout, fcntl.F_GETFL) | os.O_NONBLOCK)
        return proc

    def _reInitCallback(self):
        del self.reInitHandler
        self.initStart()
        return False


class _ApiServer(UnixDomainSocketApiServer):

    def __init__(self, parent):
        self.updaterDict = parent.updaterDict
        super().__init__(McConst.apiServerFile, self._clientInitFunc, self._clientNoitfyFunc)

    def _clientInitFunc(self, sock):
        pid = McUtil.getUnixDomainSocketPeerInfo(sock)[0]
        for mirrorId, obj in self.updaterDict.items():
            if obj.proc is not None and obj.proc.pid == pid:
                return mirrorId
        raise Exception("client not found")

    def _clientNoitfyFunc(self, mirrorId, data):
        obj = self.updaterDict[mirrorId]

        if "message" not in data:
            raise Exception("\"message\" field does not exist in notification")
        if "data" not in data:
            raise Exception("\"data\" field does not exist in notification")

        if data["message"] == "progress":
            if "progress" not in data["data"]:
                raise Exception("\"data.progress\" field does not exist in notification")
            if not isinstance(data["data"]["progress"], int):
                raise Exception("\"data.progress\" field does not contain an integer value")
            if not (0 <= data["data"]["progress"] <= 100):
                raise Exception("\"data.progress\" must be in range [0,100]")

            if obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
                obj.initProgressCallback(data["data"]["progress"])
            elif obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING:
                obj.updateProgressCallback(data["data"]["progress"])
            else:
                assert False
            return

        if data["message"] == "error":
            if "exc_info" not in data["data"]:
                raise Exception("\"data.exc_info\" field does not exist in notification")

            if obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
                obj.initErrorCallback(data["data"]["exc_info"])
            elif obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING:
                obj.updateErrorCallback(data["data"]["exc_info"])
            else:
                assert False
            return

        if data["message"] == "error-and-hold-for":
            if "seconds" not in data["data"]:
                raise Exception("\"data.seconds\" field does not exist in notification")
            if not isinstance(data["data"]["seconds"], int):
                raise Exception("\"data.seconds\" field does not contain an integer value")
            if data["data"]["seconds"] <= 0:
                raise Exception("\"data.seconds\" must be greater than 0")
            if "exc_info" not in data["data"]:
                raise Exception("\"data.exc_info\" field does not exist in notification")

            if obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
                obj.initErrorAndHoldForCallback(data["data"]["seconds"], data["data"]["exc_info"])
            elif obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING:
                obj.updateErrorAndHoldForCallback(data["data"]["seconds"], data["data"]["exc_info"])
            else:
                assert False
            return

        raise Exception("message type is not supported")


def _initFlagFile(mirrorSite):
    return os.path.join(McConst.cacheDir, mirrorSite.dataDir + ".uninitialized")
