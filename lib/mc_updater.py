#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import math
import json
import fcntl
import logging
import subprocess
from datetime import datetime
from croniter import croniter
from collections import OrderedDict
from gi.repository import GLib
from mc_util import McUtil
from mc_util import RotatingFile
from mc_util import GLibIdleInvoker
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
        self.scheduler = _Scheduler()

        self.updaterDict = dict()                                       # dict<mirror-id,updater-object>
        for ms in self.param.mirrorSiteDict.values():
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
        self.masterDir = os.path.join(McConst.cacheDir, self.mirrorSite.id)
        self.pluginStateDir = os.path.join(self.masterDir, "state")

        # state files
        self.initFlagFile = os.path.join(self.masterDir, "UNINITIALIZED")
        self.lastUpdateSchedTimeFile = os.path.join(self.masterDir, "LAST_UPDATE_SCHED_TIME")

        # initialize master directory
        if not os.path.exists(self.masterDir):
            os.makedirs(self.masterDir)
            if self.mirrorSite.initializerExe is not None:
                self._setUnInitialized()

        # initialize plugin state directory
        if not os.path.exists(self.pluginStateDir):
            os.makedirs(self.pluginStateDir)

        # storage object initialize
        for sobj in self.mirrorSite.storageDict.values():
            sobj.initialize()

        bInit = True
        if self._isInitialized():
            bInit = False
        if self.mirrorSite.initializerExe is None:
            bInit = False

        if bInit:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
        else:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
        self.schedDatetime = None
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
            if self.mirrorSite.schedExpr is not None:
                self.scheduler.addCronJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)

    def initStart(self):
        assert self.status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL]

        try:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
            self.schedDatetime = None       # this variable is for update only
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

    def initExitCallback(self, pid, status):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING

        self.proc = None
        try:
            GLib.spawn_check_exit_status(status)
            # child process returns ok
            self._setInitialized()
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            logging.info("Mirror site \"%s\" initialization finished." % (self.mirrorSite.id))
            self.invoker.add(lambda: self.param.advertiser.advertiseMirrorSite(self.mirrorSite.id))
            self.scheduler.addCronJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)
        except GLib.Error as e:
            # child process returns failure
            holdFor = self.holdFor
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL)
            if holdFor is None:
                logging.error("Mirror site \"%s\" initialization failed (code: %d), re-initialize in %d seconds." % (self.mirrorSite.id, e.code, McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL))
                self.reInitHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL, self._reInitCallback)
            else:
                logging.error("Mirror site \"%s\" initialization failed (code: %d), hold for %d seconds before re-initialization." % (self.mirrorSite.id, e.code, holdFor))
                self.reInitHandler = GLib.timeout_add_seconds(holdFor, self._reInitCallback)

    def updateStart(self, schedDatetime):
        assert self.status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL]

        if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING:
            logging.info("Mirror site \"%s\" updating ignored on \"%s\", last update is not finished." % (self.mirrorSite.id, schedDatetime.strftime("%Y-%m-%d %H:%M")))
            return

        try:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
            self.schedDatetime = schedDatetime
            self.progress = 0
            self.proc = self._createInitOrUpdateProc()
            self.pidWatch = GLib.child_watch_add(self.proc.pid, self.updateExitCallback)
            self.stdoutWatch = GLib.io_add_watch(self.proc.stdout, GLib.IO_IN, self.stdoutCallback)
            self.logger = RotatingFile(os.path.join(McConst.logDir, "%s.log" % (self.mirrorSite.id)), McConst.updaterLogFileSize, McConst.updaterLogFileCount)
            self.excInfo = None
            self.holdFor = None
            logging.info("Mirror site \"%s\" update triggered on \"%s\"." % (self.mirrorSite.id, self.schedDatetime.strftime("%Y-%m-%d %H:%M")))
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

    def updateExitCallback(self, pid, status):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING

        self.proc = None
        try:
            GLib.spawn_check_exit_status(status)
            # child process returns ok
            self._setLastUpdateSchedTime(self.schedDatetime)
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            logging.info("Mirror site \"%s\" update finished." % (self.mirrorSite.id))
        except GLib.Error as e:
            # child process returns failure
            holdFor = self.holdFor
            self._clearVars(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL)
            if holdFor is None:
                logging.error("Mirror site \"%s\" update failed (code: %d)." % (self.mirrorSite.id, e.code))
            else:
                # is there really any effect since the period is always hours?
                self.scheduler.pauseJob(self.mirrorSite.id, datetime.now() + datetime.timedelta(seconds=holdFor))
                logging.error("Mirror site \"%s\" updates failed (code: %d), hold for %d seconds." % (self.mirrorSite.id, e.code, holdFor))

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
        self.schedDatetime = None
        self.status = status

    def _createInitOrUpdateProc(self):
        cmd = []

        # create log directory
        logDir = os.path.join(McConst.logDir, self.mirrorSite.id)
        McUtil.ensureDir(logDir)

        # executable
        if self.schedDatetime is None:
            cmd.append(self.mirrorSite.initializerExe)
        else:
            cmd.append(self.mirrorSite.updaterExe)

        # argument
        if True:
            args = {
                "id": self.mirrorSite.id,
                "config": self.mirrorSite.cfgDict,
                "state-directory": self.pluginStateDir,
                "log-directory": logDir,
                "debug-flag": "",
                "country": self.param.mainCfg["country"],
                "location": self.param.mainCfg["location"],
            }
            for storageName, storageObj in self.mirrorSite.storageDict.items():
                args["storage-" + storageName] = storageObj.getParamForPlugin()
            if self.schedDatetime is not None:
                args["run-mode"] = "update"
                args["sched-datetime"] = datetime.strftime(self.schedDatetime, "%Y-%m-%d %H:%M")
            else:
                args["run-mode"] = "initialize"
        cmd.append(json.dumps(args))

        # create process
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        fcntl.fcntl(proc.stdout, fcntl.F_SETFL, fcntl.fcntl(proc.stdout, fcntl.F_GETFL) | os.O_NONBLOCK)
        return proc

    def _reInitCallback(self):
        del self.reInitHandler
        self.initStart()
        return False

    def _isInitialized(self):
        _oldInitFlagFile = os.path.join(self.masterDir, self.mirrorSite.id + ".uninitialized")       # deprecated
        return not os.path.exists(self.initFlagFile) and not os.path.exists(_oldInitFlagFile)

    def _setUnInitialized(self):
        McUtil.touchFile(self.initFlagFile)

    def _setInitialized(self):
        _oldInitFlagFile = os.path.join(self.masterDir, self.mirrorSite.id + ".uninitialized")       # deprecated
        McUtil.forceDelete(self.initFlagFile)
        McUtil.forceDelete(_oldInitFlagFile)

    def _getLastUpdateSchedTime(self):
        if not os.path.exists(self.lastUpdateSchedTimeFile):
            return datetime.min
        with open(self.lastUpdateSchedTimeFile, "w") as f:
            return datetime.strptime(f.read(), "%Y-%m-%d %H:%M")

    def _setLastUpdateSchedTime(self, schedDatetime):
        with open(self.lastUpdateSchedTimeFile, "w") as f:
            f.write(schedDatetime.strftime("%Y-%m-%d %H:%M"))


class _ApiServer(UnixDomainSocketApiServer):

    def __init__(self, parent):
        self.updaterDict = parent.updaterDict
        super().__init__(McConst.apiServerFile,
                         self._clientAppearFunc,
                         None,                       # we track client life-time by its process object, not by clientDisappearFunc
                         self._clientNoitfyFunc)

    def _clientAppearFunc(self, sock):
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

        raise Exception("message type \"%s\" is not supported" % (data["message"]))


class _Scheduler:

    def __init__(self):
        self.jobDict = OrderedDict()       # dict<id,(type,param,callback)>
        self.jobPauseDict = dict()         # dict<id,datetime>

        self.nextDatetime = None
        self.nextJobList = None

        self.timeoutHandler = None

    def dispose(self):
        if self.timeoutHandler is not None:
            GLib.source_remove(self.timeoutHandler)
            self.timeoutHandler = None
        self.nextJobList = None
        self.nextDatetime = None
        self.jobDict = OrderedDict()

    def addCronJob(self, jobId, cronExpr, jobCallback):
        assert jobId not in self.jobDict

        now = datetime.now()

        # add job
        iter = self._cronCreateIter(cronExpr, now)
        self.jobDict[jobId] = ("cron", iter, jobCallback)

        # add job or recalcluate timeout if it is first job
        if self.nextDatetime is not None:
            now = min(now, self.nextDatetime)
            if self._cronGetNextDatetime(now, iter) < self.nextDatetime:
                self._clearTimeout()
                self._calcTimeout(now)
            elif self._cronGetNextDatetime(now, iter) == self.nextDatetime:
                self.nextJobList.append(jobId)
        else:
            self._calcTimeout(now)

    def addIntervalJob(self, jobId, intervalStr, jobCallback):
        assert jobId not in self.jobDict

        # get timedelta
        m = re.match("([0-9]+)(h|d|w|m)", intervalStr)
        if m is None:
            raise Exception("invalid interval %s" % (intervalStr))
        if m.group(2) == "h":
            interval = datetime.timedelta(hours=int(m.group(1)))
        elif m.group(2) == "d":
            interval = datetime.timedelta(days=int(m.group(1)))
        elif m.group(2) == "w":
            interval = datetime.timedelta(weeks=int(m.group(1)))
        elif m.group(2) == "m":
            interval = datetime.timedelta(months=int(m.group(1)))
        else:
            assert False

        # add job
        self.jobDict[jobId] = ("interval", interval, jobCallback)

        # add job or recalcluate timeout if it is first job
        now = datetime.now()
        if self.nextDatetime is not None:
            now = min(now, self.nextDatetime)
            if self._intervalGetNextDatetime(now, interval) < self.nextDatetime:
                self._clearTimeout()
                self._calcTimeout(now)
            elif self._intervalGetNextDatetime(now, interval) == self.nextDatetime:
                self.nextJobList.append(jobId)
        else:
            self._calcTimeout(now)

    def removeJob(self, jobId):
        assert jobId in self.jobDict

        # remove job
        del self.jobDict[jobId]

        # recalculate timeout if neccessary
        now = datetime.now()
        if self.nextDatetime is not None:
            if jobId in self.nextJobList:
                self.nextJobList.remove(jobId)
                if len(self.nextJobList) == 0:
                    self._clearTimeout()
                    self._calcTimeout(now)
        else:
            assert False

    def pauseJob(self, jobId, datetime):
        assert jobId in self.jobDict
        self.jobPauseDict[jobId] = datetime

    def _calcTimeout(self, now):
        assert self.nextDatetime is None

        for jobId, v in self.jobDict.items():
            if v[0] == "cron":
                iter = v[1]
                if self.nextDatetime is None or self._cronGetNextDatetime(now, iter) < self.nextDatetime:
                    self.nextDatetime = self._cronGetNextDatetime(now, iter)
                    self.nextJobList = [jobId]
                    continue
                if self._cronGetNextDatetime(now, iter) == self.nextDatetime:
                    self.nextJobList.append(jobId)
                    continue
            elif v[0] == "interval":
                interval = v[1]
                if self.nextDatetime is None or self._intervalGetNextDatetime(now, interval) < self.nextDatetime:
                    self.nextDatetime = self._intervalGetNextDatetime(now, interval)
                    self.nextJobList = [jobId]
                if self._intervalGetNextDatetime(now, interval) == self.nextDatetime:
                    self.nextJobList.append(jobId)
                    continue
            else:
                assert False

        if self.nextDatetime is not None:
            interval = math.ceil((self.nextDatetime - now).total_seconds())
            assert interval > 0
            self.timeoutHandler = GLib.timeout_add_seconds(interval, self._jobCallback)

    def _clearTimeout(self):
        assert self.nextDatetime is not None

        GLib.source_remove(self.timeoutHandler)
        self.timeoutHandler = None
        self.nextJobList = None
        self.nextDatetime = None

    def _jobCallback(self):
        for jobId in self.nextJobList:
            if jobId not in self.jobPauseDict:
                self.jobDict[jobId][2](self.nextDatetime)
            else:
                if self.jobPauseDict[jobId] <= self.nextDatetime:
                    del self.jobPauseDict[jobId]
                    self.jobDict[jobId][2](self.nextDatetime)
        self._clearTimeout()
        self._calcTimeout(datetime.now())           # self._calcTimeout(self.nextDatetime) is stricter but less robust
        return False

    def _cronCreateIter(self, cronExpr, curDatetime):
        iter = croniter(cronExpr, curDatetime, datetime)
        iter.get_next()
        return iter

    def _cronGetNextDatetime(self, curDatetime, croniterIter):
        while croniterIter.get_current() < curDatetime:
            croniterIter.get_next()
        return croniterIter.get_current()

    def _intervalGetNextDatetime(self, curDatetime, interval):
        return curDatetime + interval
