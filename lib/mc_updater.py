#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import math
import json
import fcntl
import logging
import subprocess
import statistics
from datetime import datetime
from croniter import croniter
from collections import OrderedDict
from gi.repository import GLib
from mc_util import McUtil
from mc_util import DynObject
from mc_util import RotatingFile
from mc_util import GLibIdleInvoker
from mc_util import UnixDomainSocketApiServer
from mc_param import McConst


class McMirrorSiteUpdater:

    MIRROR_SITE_UPDATE_STATUS_INIT = 0
    MIRROR_SITE_UPDATE_STATUS_IDLE = 1
    MIRROR_SITE_UPDATE_STATUS_INITING = 2
    MIRROR_SITE_UPDATE_STATUS_INIT_FAIL = 3
    MIRROR_SITE_UPDATE_STATUS_UPDATING = 4
    MIRROR_SITE_UPDATE_STATUS_UPDATE_FAIL = 5
    MIRROR_SITE_UPDATE_STATUS_MAINTAINING = 6

    MIRROR_SITE_RESTART_INTERVAL = 60

    def __init__(self, param):
        self.param = param
        self.invoker = GLibIdleInvoker()
        self.scheduler = _Scheduler()
        self.apiServer = _ApiServer()

        self.updaterDict = dict()                                       # dict<mirror-id,updater-object>
        for ms in self.param.mirrorSiteDict.values():
            self.updaterDict[ms.id] = _OneMirrorSiteUpdater(self, ms)

    def dispose(self):
        self.apiServer.dispose()
        for updater in self.updaterDict.values():
            if updater.status == self.MIRROR_SITE_UPDATE_STATUS_INITING:
                updater.initStop()
            elif updater.status == self.MIRROR_SITE_UPDATE_STATUS_UPDATING:
                updater.updateStop()
            elif updater.status == self.MIRROR_SITE_UPDATE_STATUS_MAINTAINING:
                updater.maintainStop()
        # FIXME, should use g_main_context_iteration to wait all the updaters to stop
        self.scheduler.dispose()
        self.invoker.dispose()

    def isMirrorSiteInitialized(self, mirrorSiteId):
        return self.updaterDict[mirrorSiteId].updateHistory.isInitialized()

    def getMirrorSiteUpdateState(self, mirrorSiteId):
        updater = self.updaterDict[mirrorSiteId]
        ret = dict()
        ret["update_status"] = updater.status
        if mirrorSiteId in self.scheduler.jobDict:
            ret["last_update_time"] = self.scheduler.jobInfoDict[mirrorSiteId][0]
            ret["next_update_time"] = self.scheduler.jobInfoDict[mirrorSiteId][1]
        else:
            ret["last_update_time"] = None
            ret["next_update_time"] = None
        if updater.status in [self.MIRROR_SITE_UPDATE_STATUS_INITING, self.MIRROR_SITE_UPDATE_STATUS_UPDATING]:
            ret["update_progress"] = updater.progress
        return ret

    def updateMirrorSiteNow(self, mirrorSiteId):
        assert self.updaterDict[mirrorSiteId].status in [self.MIRROR_SITE_UPDATE_STATUS_IDLE, self.MIRROR_SITE_UPDATE_STATUS_UPDATE_FAIL]
        self.scheduler.triggerJobNow(mirrorSiteId)


class _OneMirrorSiteUpdater:

    def __init__(self, parent, mirrorSite):
        self.param = parent.param
        self.invoker = parent.invoker
        self.scheduler = parent.scheduler
        self.apiServer = parent.apiServer
        self.mirrorSite = mirrorSite

        self.updateHistory = _UpdateHistory(os.path.join(self.mirrorSite.masterDir, "UPDATE_HISTORY"),
                                            self.mirrorSite.initializerExe is not None)

        if not self.updateHistory.isInitialized():
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
            self.invoker.add(self.initStart)
        else:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            obj = self.updateHistory.getLastUpdateInfo()
            if obj is not None:
                self._postInit(obj.endTime)
            else:
                self._postInit(None)

    def initStart(self):
        assert self.status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL]

        try:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
            self._createVars()
            self.proc = self._createProc()
            self.pidWatch = GLib.child_watch_add(self.proc.pid, self.initExitCallback)
            self.stdoutWatch = GLib.io_add_watch(self.proc.stdout, GLib.IO_IN, self._stdoutCallback)
            self.logger = RotatingFile(os.path.join(McConst.logDir, "%s.log" % (self.mirrorSite.id)), McConst.updaterLogFileSize, McConst.updaterLogFileCount)
            logging.info("Mirror site \"%s\" initialization starts." % (self.mirrorSite.id))
        except Exception:
            self._clearVars()
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL
            logging.error("Mirror site \"%s\" initialization failed, re-initialize in %d seconds." % (self.mirrorSite.id, McMirrorSiteUpdater.MIRROR_SITE_RESTART_INTERVAL), exc_info=True)
            self.reInitHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RESTART_INTERVAL, self._reInitCallback)

    def initStop(self):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        self.bStop = True
        McUtil.procTerminate(self.proc)

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

    def initExitCallback(self, pid, status):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING

        curDt = datetime.now()
        try:
            GLib.spawn_check_exit_status(status)
            # child process returns ok
            bStop = self.bStop
            self.updateHistory.initFinished(curDt)
            self._clearVars()
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            logging.info("Mirror site \"%s\" initialization finished." % (self.mirrorSite.id))
            if not bStop:
                self._postInit(curDt)
        except GLib.Error as e:
            # child process returns failure
            bStop = self.bStop
            holdFor = self.holdFor
            self._clearVars()
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL
            if holdFor is None:
                logging.error("Mirror site \"%s\" initialization failed (code: %d), re-initialize in %d seconds." % (self.mirrorSite.id, e.code, McMirrorSiteUpdater.MIRROR_SITE_RESTART_INTERVAL))
                holdFor = McMirrorSiteUpdater.MIRROR_SITE_RESTART_INTERVAL
            else:
                logging.error("Mirror site \"%s\" initialization failed (code: %d), hold for %d seconds before re-initialization." % (self.mirrorSite.id, e.code, holdFor))
            if not bStop:
                self.reInitHandler = GLib.timeout_add_seconds(holdFor, self._reInitCallback)

    def updateStart(self, schedDatetime):
        assert self.status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATE_FAIL]

        if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING:
            logging.info("Mirror site \"%s\" updating ignored on \"%s\", last update is not finished." % (self.mirrorSite.id, schedDatetime.strftime("%Y-%m-%d %H:%M")))
            return

        try:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING
            self._createVars()
            self.schedDatetime = schedDatetime
            self.proc = self._createProc()
            self.pidWatch = GLib.child_watch_add(self.proc.pid, self.updateExitCallback)
            self.stdoutWatch = GLib.io_add_watch(self.proc.stdout, GLib.IO_IN, self._stdoutCallback)
            self.logger = RotatingFile(os.path.join(McConst.logDir, "%s.log" % (self.mirrorSite.id)), McConst.updaterLogFileSize, McConst.updaterLogFileCount)
            logging.info("Mirror site \"%s\" update triggered on \"%s\"." % (self.mirrorSite.id, self.schedDatetime.strftime("%Y-%m-%d %H:%M")))
        except Exception:
            self._clearVars()
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATE_FAIL
            logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id), exc_info=True)

    def updateStop(self):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING
        self.bStop = True
        McUtil.procTerminate(self.proc)

    def updateProgressCallback(self, progress):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING
        if progress > self.progress:
            self.progress = progress
            logging.info("Mirror site \"%s\" update progress %d%%." % (self.mirrorSite.id, self.progress))
        elif progress == self.progress:
            pass
        else:
            raise Exception("invalid progress")

    def updateErrorCallback(self, exc_info):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING
        assert self.excInfo is None
        self.excInfo = exc_info

    def updateErrorAndHoldForCallback(self, seconds, exc_info):
        # we only use hold-for when initializating
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING
        assert self.excInfo is None
        self.excInfo = exc_info

    def updateExitCallback(self, pid, status):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING

        curDt = datetime.now()
        try:
            GLib.spawn_check_exit_status(status)
            # child process returns ok
            self.updateHistory.updateFinished(True, self.schedDatetime, curDt)
            self._clearVars()
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            logging.info("Mirror site \"%s\" update finished." % (self.mirrorSite.id))
        except GLib.Error as e:
            # child process returns failure
            bStop = self.bStop
            self.updateHistory.updateFinished(False, self.schedDatetime, curDt)
            self._clearVars()
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATE_FAIL
            logging.error("Mirror site \"%s\" update failed (code: %d)." % (self.mirrorSite.id, e.code))
            if not bStop:
                self._retryUpdate(curDt)

    def maintainStart(self):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
        try:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_MAINTAINING
            self._createVars()
            self.proc = self._createProc()
            self.pidWatch = GLib.child_watch_add(self.proc.pid, self.maintainExitCallback)
            self.stdoutWatch = GLib.io_add_watch(self.proc.stdout, GLib.IO_IN, self._stdoutCallback)
            self.logger = RotatingFile(os.path.join(McConst.logDir, "%s.log" % (self.mirrorSite.id)), McConst.updaterLogFileSize, McConst.updaterLogFileCount)
            logging.info("Mirror site \"%s\" maintainer started." % (self.mirrorSite.id))
        except Exception:
            self._clearVars()
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            logging.error("Mirror site \"%s\" maintainer start failed, restart it in %d seconds." % (self.mirrorSite.id, McMirrorSiteUpdater.MIRROR_SITE_RESTART_INTERVAL), exc_info=True)
            self.reMaintainHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RESTART_INTERVAL, self._reMaintainCallback)

    def maintainStop(self):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_MAINTAINING
        self.bStop = True
        McUtil.procTerminate(self.proc)

    def maintainErrorCallback(self, exc_info):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_MAINTAINING
        assert self.excInfo is None
        self.excInfo = exc_info

    def maintainExitCallback(self, pid, status):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_MAINTAINING

        bStop = self.bStop
        code = self.proc.poll()
        self._clearVars()
        self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
        logging.error("Mirror site \"%s\" maintainer exited (code: %d), restart it in %d seconds." % (self.mirrorSite.id, code, McMirrorSiteUpdater.MIRROR_SITE_RESTART_INTERVAL))
        if not bStop:
            self.reMaintainHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RESTART_INTERVAL, self._reMaintainCallback)

    def _stdoutCallback(self, source, cb_condition):
        try:
            self.logger.write(source.read())
        finally:
            return True

    def _createVars(self):
        self.bStop = False
        self.proc = None
        self.pidWatch = None
        self.stdoutWatch = None
        self.logger = None
        self.excInfo = None

        if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
            self.progress = 0
            self.holdFor = None
        elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING:
            self.schedDatetime = None
            self.progress = 0
            self.holdFor = None
        elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_MAINTAINING:
            pass
        else:
            assert False

    def _clearVars(self):
        if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
            del self.holdFor
            del self.progress
        elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING:
            del self.schedDatetime
            del self.holdFor
            del self.progress
        elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_MAINTAINING:
            pass
        else:
            assert False

        del self.excInfo
        if self.logger is not None:
            self.logger.close()
        del self.logger
        if self.stdoutWatch is not None:
            GLib.source_remove(self.stdoutWatch)
        del self.stdoutWatch
        if self.pidWatch is not None:
            GLib.source_remove(self.pidWatch)
        del self.pidWatch
        if self.proc is not None:
            McUtil.procTerminate(self.proc, wait=True)
            self.apiServer.removeMirrorSite(self.mirrorSite.id, self, self.proc.pid)
        del self.proc
        del self.bStop

    def _createProc(self):
        cmd = []

        # create log directory
        logDir = os.path.join(McConst.logDir, self.mirrorSite.id)
        McUtil.ensureDir(logDir)

        # executable
        if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
            cmd.append(self.mirrorSite.initializerExe)
        elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING:
            cmd.append(self.mirrorSite.updaterExe)
        elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_MAINTAINING:
            cmd.append(self.mirrorSite.maintainerExe)
        else:
            assert False

        # argument
        if True:
            args = {
                "id": self.mirrorSite.id,
                "config": self.mirrorSite.cfgDict,
                "state-directory": self.mirrorSite.pluginStateDir,
                "log-directory": logDir,
                "debug-flag": "",
                "country": self.param.mainCfg["country"],
                "location": self.param.mainCfg["location"],
            }
            for storageName, storageObj in self.mirrorSite.storageDict.items():
                args["storage-" + storageName] = storageObj.pluginParam

            if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
                args["run-mode"] = "init"
            elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING:
                args["run-mode"] = "update"
                args["sched-datetime"] = datetime.strftime(self.schedDatetime, "%Y-%m-%d %H:%M")
            elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_MAINTAINING:
                args["run-mode"] = "maintain"
            else:
                assert False

            cmd.append(json.dumps(args))

        # create process
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        fcntl.fcntl(proc.stdout, fcntl.F_SETFL, fcntl.fcntl(proc.stdout, fcntl.F_GETFL) | os.O_NONBLOCK)
        self.apiServer.addMirrorSite(self.mirrorSite.id, self, proc.pid)

        return proc

    def _reInitCallback(self):
        del self.reInitHandler
        self.initStart()
        return False

    def _postInit(self, finishDatetime):
        self.invoker.add(lambda: self.param.advertiser.advertiseMirrorSite(self.mirrorSite.id))
        if self.mirrorSite.updaterExe is not None:
            if self.mirrorSite.schedType == "interval":
                self.scheduler.addIntervalJob(self.mirrorSite.id, finishDatetime, self.mirrorSite.schedInterval, self.updateStart)
            elif self.mirrorSite.schedType == "cronexpr":
                self.scheduler.addCronJob(self.mirrorSite.id, finishDatetime, self.mirrorSite.schedCronExpr, self.updateStart)
            else:
                assert False
        elif self.mirrorSite.maintainerExe is not None:
            self.invoker.add(self.maintainStart)

    def _retryUpdate(self, finishDatetime):
        if self.mirrorSite.updateRetryType == "interval":
            newDt = finishDatetime + self.mirrorSite.updateRetryInterval
            self.scheduler.triggerJobAt(self.mirrorSite.id, newDt)
        elif self.mirrorSite.updateRetryType == "cronexpr":
            newDt = croniter(self.mirrorSite.updateRetryCronExpr, finishDatetime, datetime).get_next()
            self.scheduler.triggerJobAt(self.mirrorSite.id, newDt)

    def _reMaintainCallback(self):
        del self.reMaintainHandler
        self.maintainStart()
        return False


class _ApiServer(UnixDomainSocketApiServer):

    def __init__(self):
        self._clientPidDict = dict()                 # <pid,mirror-id>
        self._mirrorSiteUpdaterDict = dict()         # <mirror-id,mirror-site-updater>
        self._sockDict = dict()                      # <mirror-id,sock>
        super().__init__(McConst.apiServerFile, self._clientAppearFunc, self._clientDisappearFunc, self._clientNoitfyFunc)

    def addMirrorSite(self, mirrorId, mirrorSiteUpdater, pid):
        assert pid not in self._clientPidDict
        assert mirrorId not in self._mirrorSiteUpdaterDict

        self._mirrorSiteUpdaterDict[mirrorId] = mirrorSiteUpdater
        self._clientPidDict[pid] = mirrorId

    def removeMirrorSite(self, mirrorId, mirrorSiteUpdater, pid):
        assert self._clientPidDict[pid] == mirrorId
        assert self._mirrorSiteUpdaterDict[mirrorId] == mirrorSiteUpdater
        assert mirrorId not in self._sockDict

        del self._mirrorSiteUpdaterDict[mirrorId]
        del self._clientPidDict[pid]

    def _clientAppearFunc(self, sock):
        pid = McUtil.getUnixDomainSocketPeerInfo(sock)[0]
        if pid not in self._clientPidDict:
            raise Exception("client not found")
        mirrorId = self._clientPidDict[pid]
        self._sockDict[mirrorId] = sock
        return mirrorId

    def _clientDisappearFunc(self, mirrorId):
        del self._sockDict[mirrorId]

    def _clientNoitfyFunc(self, mirrorId, data):
        obj = self._mirrorSiteUpdaterDict[mirrorId]

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

            if obj.mirrorSite.initializerExe is not None and obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
                obj.initProgressCallback(data["data"]["progress"])
            elif obj.mirrorSite.updaterExe is not None and obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING:
                obj.updateProgressCallback(data["data"]["progress"])
            else:
                assert False

            return

        if data["message"] == "error":
            if "exc_info" not in data["data"]:
                raise Exception("\"data.exc_info\" field does not exist in notification")

            if obj.mirrorSite.initializerExe is not None and obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
                obj.initErrorCallback(data["data"]["exc_info"])
            elif obj.mirrorSite.updaterExe is not None and obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING:
                obj.updateErrorCallback(data["data"]["exc_info"])
            elif obj.mirrorSite.maintainerExe is not None and obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE:
                obj.maintainErrorCallback(data["data"]["exc_info"])
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

            if obj.mirrorSite.initializerExe is not None and obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING:
                obj.initErrorAndHoldForCallback(data["data"]["seconds"], data["data"]["exc_info"])
            elif obj.mirrorSite.updaterExe is not None and obj.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING:
                obj.updateErrorAndHoldForCallback(data["data"]["seconds"], data["data"]["exc_info"])
            else:
                assert False
            return

        raise Exception("message type \"%s\" is not supported" % (data["message"]))


class _Scheduler:

    def __init__(self):
        self.jobDict = OrderedDict()            # dict<id,(type,param,callback)>
        self.jobInfoDict = dict()               # dict<id,[lastSchedDatetime,nextSchedDatetime]>
        self.nextDatetime = datetime.max
        self.timeoutHandler = None

    def dispose(self):
        if self.timeoutHandler is not None:
            GLib.source_remove(self.timeoutHandler)
            self.timeoutHandler = None
        self.nextDatetime = datetime.max
        self.jobInfoDict = dict()
        self.jobDict = OrderedDict()

    def addCronJob(self, jobId, lastSchedDatetime, cronExpr, jobCallback):
        assert jobId not in self.jobDict
        now = datetime.now()
        self.jobDict[jobId] = ("cron", croniter(cronExpr, now, datetime), jobCallback)
        self.jobInfoDict[jobId] = [lastSchedDatetime, self.__cronGetNextDatetime(now, self.jobDict[jobId][1])]
        self._timeoutMayBecomeEarly(jobId)

    def addIntervalJob(self, jobId, lastSchedDatetime, interval, jobCallback):
        assert jobId not in self.jobDict
        now = datetime.now()
        self.jobDict[jobId] = ("interval", interval, jobCallback)
        self.jobInfoDict[jobId] = [lastSchedDatetime, self.__intervalGetNextDatetime(now, lastSchedDatetime, interval)]
        self._timeoutMayBecomeEarly(jobId)

    def pauseJobUntil(self, jobId, untilDatetime):
        assert jobId in self.jobDict
        if untilDatetime > self.jobInfoDict[jobId][1]:
            self.jobInfoDict[jobId][1] = untilDatetime
            self._timeoutMayBecomeLate()

    def triggerJobAt(self, jobId, triggerDatetime):
        assert jobId in self.jobDict
        if triggerDatetime < self.jobInfoDict[jobId][1]:
            self.jobInfoDict[jobId][1] = triggerDatetime
            self._timeoutMayBecomeEarly(jobId)

    def triggerJobNow(self, jobId):
        assert jobId in self.jobDict
        self._execJob(jobId, datetime.now())
        self._timeoutMayBecomeLate()

    def _timeoutMayBecomeLate(self):
        m = min([x[1] for x in self.jobInfoDict.values()])
        if m > self.nextDatetime:
            self.__updateTimeout(m)

    def _timeoutMayBecomeEarly(self, jobId):
        if self.jobInfoDict[jobId][1] < self.nextDatetime:
            self.__updateTimeout(self.jobInfoDict[jobId][1])

    def _jobCallback(self):
        now = datetime.now()

        # execute jobs
        for jobId in self.jobDict:
            if self.jobInfoDict[jobId][1] <= now:
                self._execJob(jobId, self.jobInfoDict[jobId][1])

        # recalculate timeout
        m = min([x[1] for x in self.jobInfoDict.values()])
        assert m > self.nextDatetime
        self.__updateTimeout(m)

        return False

    def _execJob(self, jobId, curDatetime):
        # execute job
        self.jobDict[jobId][2](curDatetime)

        # record last sched time
        self.jobInfoDict[jobId][0] = curDatetime

        # calculate next sched time
        if self.jobDict[jobId][0] == "cron":
            self.jobInfoDict[jobId][1] = self.__cronGetNextDatetime(curDatetime, self.jobDict[jobId][1])
        elif self.jobDict[jobId][0] == "interval":
            self.jobInfoDict[jobId][1] = self.__intervalGetNextDatetime(curDatetime, self.jobInfoDict[jobId][0], self.jobDict[jobId][1])
        else:
            assert False

    def __updateTimeout(self, nextDatetime):
        if self.timeoutHandler is not None:
            GLib.source_remove(self.timeoutHandler)
        self.nextDatetime = nextDatetime
        interval = math.ceil((self.nextDatetime - datetime.now()).total_seconds())
        interval = max(interval, 1)
        self.timeoutHandler = GLib.timeout_add_seconds(interval, self._jobCallback)

    def __cronGetNextDatetime(self, curDatetime, croniterIter):
        while croniterIter.get_current() <= curDatetime:
            croniterIter.get_next()
        return croniterIter.get_current()

    def __intervalGetNextDatetime(self, curDatetime, lastSchedTime, interval):
        if lastSchedTime is None:
            return curDatetime
        else:
            return max(lastSchedTime + interval, curDatetime)


class _UpdateHistory:

    def __init__(self, updateHistoryFilename, needInitialization=True):
        self._updateFn = updateHistoryFilename
        self._needInit = needInitialization
        self._maxLen = 10

        if not self._needInit:
            McUtil.touchFile(self._updateFn)

        self._updateInfoList = []
        if True:
            self._readFromFile()

        self._averageUpdateDuration = None      # unit: seconds
        if True:
            self._calcAverageDuration()

    def isInitialized(self):
        return os.path.exists(self._updateFn)

    def getLastUpdateInfo(self):
        if len(self._updateInfoList) > 0:
            # list order: from old to new
            return self._updateInfoList[-1]
        else:
            return None

    def getAverageUpdateDuration(self):
        return self._averageUpdateDuration

    def initFinished(self, endTime):
        assert self._needInit
        assert len(self._updateInfoList) == 0

        # add init-item
        obj = DynObject()
        obj.startTime = None
        obj.endTime = endTime
        self._updateInfoList.append(obj)

        # save to file
        self._saveToFile()

    def updateFinished(self, bSucceed, startTime, endTime):
        # remove init-item
        if len(self._updateInfoList) > 0 and self._updateInfoList[-1].startTime is None:
            assert len(self._updateInfoList) == 1
            self._updateInfoList = []

        # remove update-failed-item
        # we only store the last one update-failed-item
        if len(self._updateInfoList) > 0 and not self._updateInfoList[-1].bSucceed:
            del self._updateInfoList[-1]

        # add update-item
        obj = DynObject()
        obj.bSucceed = bSucceed
        obj.startTime = startTime
        obj.endTime = endTime
        self._updateInfoList.append(obj)

        # keep list length
        while len(self._updateInfoList) >= self._maxLen:
            self._updateInfoList.pop(0)

        # post processing
        self._calcAverageDuration()
        self._saveToFile()

    def _readFromFile(self):
        if not os.path.exists(self._updateFn):
            return

        for line in McUtil.readFile(self._updateFn).split("\n"):
            m = re.fullmatch(line, " *(\\S+) +(\\S+) +(\\S+) *")
            if m is not None:
                try:
                    obj = DynObject()
                    obj.bSucceed = bool(m.group(1))
                    if m.group(2) == "none":
                        obj.startTime = None
                    else:
                        obj.startTime = datetime.strptime(m.group(2), McUtil.stdTmFmt())
                    obj.endTime = datetime.strptime(m.group(3), McUtil.stdTmFmt())
                    self._updateInfoList.append(obj)
                except ValueError:
                    pass

    def _saveToFile(self):
        with open(self._updateFn, "w") as f:
            f.write("# is-successful             start-time             end-time\n")
            for item in self._updateInfoList:
                if item.bSucceed:
                    f.write("  true                      ")
                else:
                    f.write("  false                     ")
                if item.startTime is None:
                    f.write("  none                   ")
                else:
                    f.write("  " + item.startTime.strftime(McUtil.stdTmFmt()) + "    ")
                if True:
                    f.write(item.endTime.strftime(McUtil.stdTmFmt()) + "\n")

    def _calcAverageDuration(self):
        if len(self._updateInfoList) == 0:
            self._averageUpdateDuration = 1
        elif len(self._updateInfoList) > 0 and self._updateInfoList[-1].startTime is None:
            assert len(self._updateInfoList) == 1
            self._averageUpdateDuration = 1
        else:
            self._averageUpdateDuration = statistics.mean([(x.endTime - x.startTime).total_seconds() // 60 for x in self._updateInfoList if x.bSuccess])
            self._averageUpdateDuration = min(1, int(self._averageUpdateDuration))
