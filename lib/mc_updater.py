#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import logging
from mc_util import McUtil
from mc_util import GLibCronScheduler


class McMirrorSiteUpdater:

    MIRROR_SITE_UPDATE_STATUS_INIT = 0
    MIRROR_SITE_UPDATE_STATUS_IDLE = 1
    MIRROR_SITE_UPDATE_STATUS_SYNC = 2
    MIRROR_SITE_UPDATE_STATUS_ERROR = 3

    def __init__(self, param):
        self.param = param
        self.scheduler = GLibCronScheduler()
        self.updateProxyDict = dict()           # dict<mirror-site-id,update-proxy-object>

        for ms in self.param.mirrorSiteList:
            # initialize data directory
            fullDir = os.path.join(self.param.cacheDir, ms.dataDir)
            if not os.path.exists(fullDir):
                os.makedirs(fullDir)
                McUtil.touchFile(self._initFlagFile(ms))

            if os.path.exists(self._initFlagFile(ms)):
                # initialize data
                proxy = _UpdateProxyMirrorSite(ms, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT)
                proxy.initStart()
            else:
                # add update job
                proxy = _UpdateProxyMirrorSite(ms, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
                self._addScheduleJob(ms, proxy)

            # record proxy object
            self.updateProxyDict[ms.id] = proxy

    def dispose(self):
        self.scheduler.dispose()
        self.scheduler = None

    def getMirrorSiteUpdateStatus(self, mirrorSiteId):
        return self.updateProxyDict[mirrorSiteId].status

    def _initFlagFile(self, mirrorSiteObj):
        return os.path.join(self.param.cacheDir, mirrorSiteObj.dataDir + ".uninitialized")

    def _addScheduleJob(self, mirrorSite, updateProxy):
        self.scheduler.addJob(mirrorSite.id, mirrorSite.schedExpr, updateProxy.updateStart)


class McMirrorSiteUpdaterApi:

    def __init__(self, param, mirror_site):
        self.param = param
        self.mirrorSite = mirror_site
        self.progressNotifier = None    # set by UpdateProxy object

    def get_country(self):
        # FIXME
        return "CN"

    def get_location(self):
        # FIXME
        return None

    def get_data_dir(self):
        return self.mirrorSite.dataDir

    def get_log_dir(self):
        return self.param.logDir

    def notify_progress(self, progress, is_success):
        assert 0 <= progress <= 100
        self.progressNotifier(progress, is_success)


class _UpdateProxyMirrorSite:

    def __init__(self, mirrorSite, status):
        assert status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE]

        self.mirrorSite = mirrorSite
        self.status = status
        self.schedDatetime = None
        self.progress = None

    def initStart(self):
        self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
        self.schedDatetime = None
        self.progress = 0
        self.mirrorSite.api.progressNotifier = self._initProgressCallback
        try:
            self.mirrorSiteObj.updaterObj.init_start()
            logging.info("Mirror site \"%s\" initialization starts." % (self.mirrorSiteObj.id))
        except:
            self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_ERROR)
            logging.error("Mirror site \"%s\" initialization failed." % (self.mirrorSiteObj.id), exc_info=True)

    def initStop(self):
        self.mirrorSiteObj.updaterObj.init_stop()

    def updateStart(self, schedDatetime):
        tstr = schedDatetime.strftime("%Y-%m-%d %H:%M")

        if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT:
            assert False
        elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC:
            logging.info("Mirror site \"%s\" updating ignored on \"%s\", last update is not finished." % (self.mirrorSiteObj.id, tstr))
        elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC
            self.schedDatetime = schedDatetime
            self.progress = 0
            self.mirrorSite.api.progressNotifier = self._updateProgressCallback
            try:
                self.mirrorSiteObj.updaterObj.update_start(self.schedDatetime)
                logging.info("Mirror site \"%s\" update triggered on \"%s\"." % (self.mirrorSiteObj.id, tstr))
            except:
                self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
                logging.error("Mirror site \"%s\" update failed." % (self.mirrorSiteObj.id), exc_info=True)
        elif self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_ERROR:
            assert False
        else:
            assert False

    def updateStop(self):
        self.mirrorSiteObj.updaterObj.update_stop()

    def _initProgressCallback(self, progress, exc_info):
        if exc_info is not None:
            self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_ERROR)
            logging.error("Mirror site \"%s\" initialization failed." % (self.mirrorSiteObj.id), exc_info)
            return

        if progress == 100:
            McUtil.forceDelete(self._initFlagFile(self.mirrorSiteObj))
            self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            self._addScheduleJob(self.mirrorSiteObj)
            logging.info("Mirror site \"%s\" initialization finished." % (self.mirrorSiteObj.id))
            return

        self.progress = progress
        logging.info("Mirror site \"%s\" initialization progress %d%%." % (mirrorSiteObj.id, self.progress))

    def _updateProgressCallback(self, progress, exc_info):
        if exc_info is not None:
            self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            logging.error("Mirror site \"%s\" update failed." % (self.mirrorSiteObj.id), exc_info)
            return

        if progress == 100:
            self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            logging.info("Mirror site \"%s\" update finished." % (self.mirrorSiteObj.id))
            return

        self.progress = progress
        logging.info("Mirror site \"%s\" update progress %d%%." % (self.mirrorSiteObj.id, self.progress))

    def _resetStatus(self, status):
        assert status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_ERROR]
        self.status = status
        self.schedDatetime = None
        self.progress = None
        self.mirrorSite.api.progressNotifier = None


class _UpdateProxyMirrorSiteInWorkerProcess(_UpdateProxyMirrorSite):

    def __init__(self, parent, mirrorSite):
        super().__init__(parent, mirrorSite)

    def initStart(self):
        pass

    def initStop(self):
        pass

    def updateStart(self):
        pass

    def updateStop(self):
        pass
