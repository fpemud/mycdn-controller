#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import logging
from mc_util import McUtil
from mc_util import DynObject
from mc_util import GLibIdleInvoker
from mc_util import GLibCronScheduler


class McMirrorSiteUpdater:

    MIRROR_SITE_UPDATE_STATUS_INIT = 0
    MIRROR_SITE_UPDATE_STATUS_INITING = 1
    MIRROR_SITE_UPDATE_STATUS_IDLE = 2
    MIRROR_SITE_UPDATE_STATUS_SYNCING = 3
    MIRROR_SITE_UPDATE_STATUS_ERROR = 4

    def __init__(self, param):
        self.param = param
        self.invoker = GLibIdleInvoker()
        self.scheduler = GLibCronScheduler()
        self.updaterDict = dict()           # dict<mirror-site-id,updater-object>

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
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT

        self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        try:
            self.progress = 0
            self.mirrorSite.updaterObj.init_start(self._createInitApi())
            logging.info("Mirror site \"%s\" initialization starts." % (self.mirrorSite.id))
        except:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_ERROR
            logging.error("Mirror site \"%s\" initialization failed." % (self.mirrorSite.id), exc_info=True)

    def initStop(self):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING

        self.mirrorSite.updaterObj.init_stop()

    def initProgressCallback(self, progress, exc_info):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        assert progress >= self.progress

        if exc_info is not None:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_ERROR
            logging.error("Mirror site \"%s\" initialization failed." % (self.mirrorSite.id), exc_info)
            return

        if progress == 100:
            McUtil.forceDelete(_initFlagFile(self.mirrorSite))
            del self.progress
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            self.parent.scheduler.addJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)
            logging.info("Mirror site \"%s\" initialization finished." % (self.mirrorSite.id))
            return

        if progress > self.progress:
            self.progress = progress
            logging.info("Mirror site \"%s\" initialization progress %d%%." % (self.mirrorSite.id, self.progress))

    def updateStart(self, schedDatetime):
        assert self.status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING]

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
                self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
                logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id), exc_info=True)

    def updateStop(self):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING

        self.mirrorSite.updaterObj.update_stop()

    def updateProgressCallback(self, progress, exc_info):
        assert self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
        assert progress >= self.progress

        if exc_info is not None:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id), exc_info)
            return

        if progress == 100:
            del self.progress
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            logging.info("Mirror site \"%s\" update finished." % (self.mirrorSite.id))
            return

        if progress > self.progress:
            self.progress = progress
            logging.info("Mirror site \"%s\" update progress %d%%." % (self.mirrorSite.id, self.progress))

    def _createInitApi(self):
        api = DynObject()
        api.get_country = lambda: "CN"
        api.get_location = lambda: None
        api.get_data_dir = lambda: self.mirrorSite.dataDir
        api.get_log_dir = lambda: self.param.logDir
        api.progress_changed = self.initProgressCallback
        return api

    def _createUpdateApi(self, schedDatetime):
        api = DynObject()
        api.get_country = lambda: "CN"
        api.get_location = lambda: None
        api.get_data_dir = lambda: self.mirrorSite.dataDir
        api.get_log_dir = lambda: self.param.logDir
        api.get_sched_datetime = lambda: schedDatetime
        api.progress_changed = self.updateProgressCallback
        return api


def _initFlagFile(param, mirrorSite):
    return os.path.join(param.cacheDir, mirrorSite.dataDir + ".uninitialized")
