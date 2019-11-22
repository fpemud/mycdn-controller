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

    def __init__(self, param):
        self.param = param
        self.scheduler = GLibCronScheduler()
        self.updateProxyDict = dict()

        for ms in self.param.mirrorSiteList:
            proxy = _UpdateProxyMirrorSite(self, ms)
            self.updateProxyDict[ms] = proxy

            # initialize data directory
            fullDir = os.path.join(self.param.cacheDir, ms.dataDir)
            if not os.path.exists(fullDir):
                os.makedirs(fullDir)
                McUtil.touchFile(self._initFlagFile(ms))
                proxy.initStart()
                continue

            # initialize data
            if os.path.exists(self._initFlagFile(ms)):
                proxy.initStart()
                continue

            # add job
            ms.updaterObjApi.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            self._addScheduleJob(ms)

    def dispose(self):
        self.scheduler.dispose()
        self.scheduler = None

    def getMirrorSiteUpdateStatus(self, mirrorSiteId):
        for ms in self.param.mirrorSiteList:
            if ms.id == mirrorSiteId:
                return ms.updateObjApi.updateStatus
        assert False

    def _startInit(self, mirrorSiteObj):
        api = mirrorSiteObj.updaterObjApi
        api.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
        api.progress = 0
        api.progressNotifier = lambda a, b, c: self._notifyProgress("initializing", a, b, c)
        try:
            mirrorSiteObj.updaterObj.init_start()
        except:
            # FIXME, don't know what to do
            raise
        logging.info("Mirror site \"%s\" initializing starts." % (mirrorSiteObj.id))

    def _startUpdate(self, mirrorSiteObj, schedDatetime):
        api = mirrorSiteObj.updaterObjApi
        tstr = schedDatetime.strftime("%Y-%m-%d %H:%M")

        if api.updateStatus == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT:
            assert False
        elif api.updateStatus == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC:
            logging.info("Mirror site \"%s\" updating ignored on \"%s\", last update is not finished." % (mirrorSiteObj.id, tstr))
        elif api.updateStatus == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE:
            api.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC
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

    def _notifyProgress(self, what, mirrorSiteObj, progress, success):
        assert what in ["initializing", "updating"]

        mirrorSiteObj.updaterObjApi.progress = progress
        if progress == 100:
            assert success is not None
            mirrorSiteObj.updaterObjApi.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            if success:
                if what == "initializing":
                    McUtil.forceDelete(self._initFlagFile(mirrorSiteObj))
                    self._addScheduleJob(mirrorSiteObj)
                logging.info("Mirror site \"%s\" %s finished." % (mirrorSiteObj.id, what))
            else:
                if what == "initializing":
                    self._addScheduleJob(mirrorSiteObj)
                logging.info("Mirror site \"%s\" %s failed." % (mirrorSiteObj.id, what))
        else:
            logging.info("Mirror site \"%s\" %s progress %d%%." % (mirrorSiteObj.id, what, progress))

    def _initFlagFile(self, mirrorSiteObj):
        return os.path.join(self.param.cacheDir, mirrorSiteObj.dataDir + ".uninitialized")

    def _addScheduleJob(self, mirrorSite, updateProxy):
        self.scheduler.addJob(mirrorSite.id, mirrorSite.schedExpr, lambda a: updateProxy.updateStart(mirrorSite, a))


class McMirrorSiteUpdaterApi:

    def __init__(self, param, mirror_site):
        self.param = param
        self.mirrorSite = mirror_site

        # set by McMirrorSiteUpdater
        self.updateStatus = None
        self.updateDatetime = None
        self.progress = None
        self.progressNotifier = None

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
        self.progressNotifier(self.mirrorSite, progress, is_success)


class _UpdateProxyMirrorSite:

    def __init__(self, parent, mirrorSite):
        self.parent = parent
        self.mirrorSite = mirrorSite

    def initStart(self):
        api = self.mirrorSiteObj.updaterObjApi
        api.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
        api.progress = 0
        api.progressNotifier = lambda a, b, c: self.parent._notifyProgress("initializing", a, b, c)
        try:
            self.mirrorSiteObj.updaterObj.init_start()
        except:
            # FIXME, don't know what to do
            raise
        logging.info("Mirror site \"%s\" initializing starts." % (self.mirrorSiteObj.id))

    def initStop(self):
        pass

    def updateStart(self):
        pass

    def updateStop(self):
        pass


class _UpdateProxyMirrorSiteInWorkerProcess:

    def __init__(self, parent, mirrorSite):
        self.parent = parent
        self.mirrorSite = mirrorSite

    def initStart(self):
        pass

    def initStop(self):
        pass

    def updateStart(self):
        pass

    def updateStop(self):
        pass
