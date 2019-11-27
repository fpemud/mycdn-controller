#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import logging
from gi.repository import GLib
from mc_util import McUtil
from mc_util import DynObject
from mc_util import GLibIdleInvoker
from mc_util import GLibCronScheduler


class McMirrorSiteUpdater:

    MIRROR_SITE_UPDATE_STATUS_INIT = 0
    MIRROR_SITE_UPDATE_STATUS_INITING = 1
    MIRROR_SITE_UPDATE_STATUS_INIT_FAIL = 2
    MIRROR_SITE_UPDATE_STATUS_IDLE = 3
    MIRROR_SITE_UPDATE_STATUS_SYNCING = 4
    MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL = 5
    MIRROR_SITE_UPDATE_STATUS_ERROR = 6

    MIRROR_SITE_RE_INIT_INTERVAL = 60

    def __init__(self, param):
        self.param = param
        self.invoker = GLibIdleInvoker()
        self.scheduler = GLibCronScheduler()
        self.updaterDict = dict()           # dict<mirror-id,updater-object>

        for ms in self.param.mirrorSiteList:
            # initialize data directory
            fullDir = os.path.join(self.param.cacheDir, ms.dataDir)
            if not os.path.exists(fullDir):
                os.makedirs(fullDir)
                McUtil.touchFile(_initFlagFile(param, ms))

            # record updater object
            self.updaterDict[ms.id] = _OneMirrorSiteUpdater(self, ms)

    def dispose(self):
        for msId, updater in self.updaterDict.items():
            if updater.status == self.MIRROR_SITE_UPDATE_STATUS_INITING:
                updater.initStop()
            elif updater.status == self.MIRROR_SITE_UPDATE_STATUS_SYNCING:
                updater.updateStop()
        # FIXME, should use g_main_context_iteration to wait them to stop
        self.scheduler.dispose()
        self.invoker.dispose()

    def getMirrorSiteUpdateStatus(self, mirrorSiteId):
        return self.updaterDict[mirrorSiteId].status


class _OneMirrorSiteUpdater:

    def __init__(self, parent, mirrorSite):
        self.param = parent.param
        self.parent = parent
        self.mirrorSite = mirrorSite

        if os.path.exists(_initFlagFile(self.param, self.mirrorSite)):
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
            self.parent.invoker.add(self.initStart)
        else:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            self.parent.scheduler.addJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)

    def initStart(self):
        self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        try:
            self.progress = 0
            self.mirrorSite.updaterObj.init_start(self._createInitApi())
            logging.info("Mirror site \"%s\" initialization starts." % (self.mirrorSite.id))
        except:
            self.reInitHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL, self._reInitCallback)
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL
            logging.error("Mirror site \"%s\" initialization failed, re-initialize in %d seconds." % (self.mirrorSite.id, McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL), exc_info=True)

    def initStop(self):
        self.mirrorSite.updaterObj.init_stop()

    def initProgressCallback(self, progress):
        assert progress >= self.progress
        if progress == self.progress:
            return

        if progress == 100:
            McUtil.forceDelete(_initFlagFile(self.param, self.mirrorSite))
            del self.progress
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            self.parent.scheduler.addJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)
            logging.info("Mirror site \"%s\" initialization finished." % (self.mirrorSite.id))
        else:
            self.progress = progress
            logging.info("Mirror site \"%s\" initialization progress %d%%." % (self.mirrorSite.id, self.progress))

    def initErrorCallback(self, exc_info):
        del self.progress
        self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL
        self.reInitHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL, self._reInitCallback)
        logging.error("Mirror site \"%s\" initialization failed, re-initialize in %d seconds." % (self.mirrorSite.id, McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL), exc_info=True)

    def updateStart(self, schedDatetime):
        tstr = schedDatetime.strftime("%Y-%m-%d %H:%M")
        if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING:
            logging.info("Mirror site \"%s\" updating ignored on \"%s\", last update is not finished." % (self.mirrorSite.id, tstr))
        else:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
            try:
                self.progress = 0
                self.mirrorSite.updaterObj.update_start(self._createUpdateApi(schedDatetime))
                logging.info("Mirror site \"%s\" update triggered on \"%s\"." % (self.mirrorSite.id, tstr))
            except:
                self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL)
                logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id), exc_info=True)

    def updateStop(self):
        self.mirrorSite.updaterObj.update_stop()

    def updateProgressCallback(self, progress):
        assert progress >= self.progress
        if progress == self.progress:
            return

        if progress == 100:
            del self.progress
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            logging.info("Mirror site \"%s\" update finished." % (self.mirrorSite.id))
        else:
            self.progress = progress
            logging.info("Mirror site \"%s\" update progress %d%%." % (self.mirrorSite.id, self.progress))

    def updateErrorCallback(self, exc_info):
        del self.progress
        self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL
        logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id), exc_info)

    def _createInitApi(self):
        api = DynObject()
        api.get_country = lambda: "CN"
        api.get_location = lambda: None
        api.get_data_dir = lambda: self.mirrorSite.dataDir
        api.get_log_dir = lambda: self.param.logDir
        api.progress_changed = self.initProgressCallback
        api.error_occured = self.initErrorCallback
        return api

    def _createUpdateApi(self, schedDatetime):
        api = DynObject()
        api.get_country = lambda: "CN"
        api.get_location = lambda: None
        api.get_data_dir = lambda: self.mirrorSite.dataDir
        api.get_log_dir = lambda: self.param.logDir
        api.get_sched_datetime = lambda: schedDatetime
        api.progress_changed = self.updateProgressCallback
        api.error_occured = self.updateErrorCallback
        return api

    def _reInitCallback(self):
        del self.reInitHandler
        self.initStart()
        return False


def _initFlagFile(param, mirrorSite):
    return os.path.join(param.cacheDir, mirrorSite.dataDir + ".uninitialized")
