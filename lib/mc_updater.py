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
                proxy = _UpdateProxyMirrorSite(ms, self._notifyProgress, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT)
                proxy.initStart()
            else:
                # add update job
                proxy = _UpdateProxyMirrorSite(ms, self._notifyProgress, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
                self._addScheduleJob(ms, proxy)

            # record proxy object
            self.updateProxyDict[ms.id] = proxy

    def dispose(self):
        self.scheduler.dispose()
        self.scheduler = None

    def getMirrorSiteUpdateStatus(self, mirrorSiteId):
        return self.updateProxyDict[mirrorSiteId].status

    def _startUpdate(self, mirrorSiteObj, schedDatetime):
        api = mirrorSiteObj.updaterObjApi
        tstr = schedDatetime.strftime("%Y-%m-%d %H:%M")

        if api.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT:
            assert False
        elif api.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC:
            logging.info("Mirror site \"%s\" updating ignored on \"%s\", last update is not finished." % (mirrorSiteObj.id, tstr))
        elif api.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE:
            api.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC
            api.updateDatetime = schedDatetime
            api.progress = 0
            api.progressNotifier = lambda a, b, c: self._notifyProgress("updating", a, b, c)
            try:
                mirrorSiteObj.updaterObj.update_start(schedDatetime)
            except:
                # FIXME, don't know what to do
                raise
            logging.info("Mirror site \"%s\" updating triggered on \"%s\"." % (mirrorSiteObj.id, tstr))
        else:
            assert False

    def _notifyInitProgress(self, mirrorSiteObj, progress, success):
        mirrorSiteObj.updaterObjApi.progress = progress
        if progress == 100:
            assert success is not None
            mirrorSiteObj.updaterObjApi.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            if success:
                McUtil.forceDelete(self._initFlagFile(mirrorSiteObj))
                self._addScheduleJob(mirrorSiteObj)
                logging.info("Mirror site \"%s\" initialization finished." % (mirrorSiteObj.id))
            else:
                self._addScheduleJob(mirrorSiteObj)
                logging.info("Mirror site \"%s\" initialization failed." % (mirrorSiteObj.id))
        else:
            logging.info("Mirror site \"%s\" initialize progress %d%%." % (mirrorSiteObj.id, progress))

    def _notifyUpdateProgress(self, mirrorSiteObj, progress, success):
        mirrorSiteObj.updaterObjApi.progress = progress
        if progress == 100:
            assert success is not None
            if success:
                logging.info("Mirror site \"%s\" update finished." % (mirrorSiteObj.id))
            else:
                logging.info("Mirror site \"%s\" update failed." % (mirrorSiteObj.id))
        else:
            logging.info("Mirror site \"%s\" update progress %d%%." % (mirrorSiteObj.id, progress))

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

    def __init__(self, mirrorSite, status, initProgressCallback, updateProgressCallback):
        assert status in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE]

        self.mirrorSite = mirrorSite
        self.status = status
        self.initProgressCallback = initProgressCallback
        self.updateProgressCallback = updateProgressCallback

        # set by McMirrorSiteUpdater
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
            logging.error("Mirror site \"%s\" initialization failed." % (self.mirrorSiteObj.id))
            # raise

    def initStop(self):
        self.mirrorSiteObj.updaterObj.init_stop()

    def updateStart(self):
        pass

    def updateStop(self):
        pass

    def _initProgressCallback(self, progress, bSuccess):
        if progress == 100:
            self.initProgressCallback(progress, bSuccess)
            self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE)
            if bSuccess:
                McUtil.forceDelete(self._initFlagFile(self.mirrorSiteObj))
                self._addScheduleJob(self.mirrorSiteObj)
                logging.info("Mirror site \"%s\" initialization finished." % (self.mirrorSiteObj.id))
            else:
                logging.info("Mirror site \"%s\" initialization failed." % (self.mirrorSiteObj.id))
        else:
            logging.info("Mirror site \"%s\" initialization progress %d%%." % (mirrorSiteObj.id, progress))

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
