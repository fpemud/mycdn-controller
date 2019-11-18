#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import logging
from mc_util import McUtil
from mc_util import GLibCronScheduler
from mc_util import GLibCronSchedulerJobBase
from mc_plugin import McMirrorSite


class McMirrorSiteUpdater:

    MIRROR_SITE_UPDATE_STATUS_INIT = 0
    MIRROR_SITE_UPDATE_STATUS_IDLE = 1
    MIRROR_SITE_UPDATE_STATUS_SYNC = 2

    def __init__(self, param):
        self.param = param
        self.scheduler = GLibCronScheduler()

        for ms in self.param.getMirrorSiteList():
            # initialize data directory
            fullDir = os.path.join(self.param.cacheDir, ms.dataDir)
            McUtil.ensureDir(fullDir)

            # set update status
            ms.updaterObjApi.mcUpdater = self
            if os.path.exists(os.path.join(fullDir, ".uninitialized")):
                ms.updaterObjApi.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
                # FIXME
                assert False
            else:
                ms.updaterObjApi.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE

            # add job
            if ms.sched == McMirrorSite.SCHED_ONESHOT:
                assert False
            elif ms.sched == McMirrorSite.SCHED_PERIODICAL:
                self.scheduler.addJob(ms.id, ms.schedExpr, _JobPeriodical(ms))
            elif ms.sched == McMirrorSite.SCHED_FOLLOW:
                followMsObj = self.param.getMirrorSite(ms.followMirrorSiteId)
                print(ms.followMirrorSiteId)
                assert followMsObj is not None
                self.scheduler.addJob(ms.id, followMsObj.schedExpr, _JobPeriodical(ms))
            elif ms.sched == McMirrorSite.SCHED_PERSIST:
                assert False
            else:
                assert False

    def dispose(self):
        self.scheduler.dispose()
        self.scheduler = None

    def getMirrorSiteUpdateStatus(self, mirrorSiteId):
        msObj = self.param.getMirrorSite(mirrorSiteId)
        return msObj.updaterObjApi.updateStatus

    def _startUpdate(self, mirrorSiteObj, schedDatetime):
        api = mirrorSiteObj.updaterObjApi
        api.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC
        api.updateDatetime = schedDatetime
        api.progress = 0
        mirrorSiteObj.updaterObj.update_start(schedDatetime)
        logging.info("Mirror site \"%s\" updating scheduled on \"%s\" starts." % (mirrorSiteObj.id, api.updateDatetime.strftime("%Y-%m-%d %H:%M")))

    def _notifyProgress(self, mirrorSiteObj, progress, finished):
        mirrorSiteObj.updaterObjApi.updateProgress = progress
        if finished:
            assert progress == 100
            mirrorSiteObj.updaterObjApi.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            logging.info("Mirror site \"%s\" updating finished." % (mirrorSiteObj.id))
        else:
            logging.info("Mirror site \"%s\" updating progress %d%%." % (mirrorSiteObj.id, progress))


class _JobPeriodical(GLibCronSchedulerJob):

    def __init__(self, mirrorSite):
        self.mirrorSite = mirrorSite
        self.api = self.mirrorSite.updaterObjApi

    def is_ready(self):
        return self.api.updateStatus != McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT

    def is_running(self):
        return self.api.updateStatus == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE

    def start(self, schedDatetime):
        self.api.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC
        self.api.updateDatetime = schedDatetime
        self.api.progress = 0
        self.mirrorSite.updaterObj.update_start(schedDatetime)
        logging.info("Mirror site \"%s\" updating scheduled on \"%s\" starts." % (self.mirrorSite.id, api.updateDatetime.strftime("%Y-%m-%d %H:%M")))

                
class _JobFollow:

    def is_ready(self):
        assert False
    def is_running(self):
        assert False
    def start(self):
        assert False
