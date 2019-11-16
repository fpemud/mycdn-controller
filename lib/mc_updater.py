#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
from mc_util import McUtil
from mc_util import CronScheduler
from mc_plugin import McMirrorSite


class McMirrorSiteUpdater:

    MIRROR_SITE_UPDATE_STATUS_INIT = 0
    MIRROR_SITE_UPDATE_STATUS_IDLE = 1
    MIRROR_SITE_UPDATE_STATUS_SYNC = 2

    def __init__(self, param):
        self.param = param
        self.scheduler = CronScheduler()

        for ms in self.param.getMirrorSiteList():
            # initialize data directory
            fullDir = os.path.join(self.param.cacheDir, ms.dataDir)
            McUtil.ensureDir(fullDir)

            # set update status
            ms.api.mcUpdater = self
            if os.path.exists(os.path.join(fullDir, ".uninitialized")):
                ms.api.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
                # FIXME
                assert False
            else:
                ms.api.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE

            # add job
            if ms.sched == McMirrorSite.SCHED_ONESHOT:
                assert False
            elif ms.sched == McMirrorSite.SCHED_PERIODICAL:
                self.sched.addJob(ms.id, ms.schedExpr, lambda schedDatetime: self._startUpdate(ms, schedDatetime))
            elif ms.sched == McMirrorSite.SCHED_FOLLOW:
                followMsObj = self.param.getMirrorSite(ms.followMirrorSiteId)
                assert followMsObj is not None
                self.sched.addJob(ms.id, followMsObj.schedExpr, lambda schedDatetime: self._startUpdate(ms, schedDatetime))
            elif ms.sched == McMirrorSite.SCHED_PERSIST:
                assert False
            else:
                assert False

    def dispose(self):
        self.scheduler.dispose()
        self.scheduler = None

    def getMirrorSiteUpdateStatus(self, mirrorSiteId):
        msObj = self.param.getMirrorSite(mirrorSiteId)
        return msObj.api.updateStatus

    def _startUpdate(self, mirrorSiteObj, schedDatetime):
        mirrorSiteObj.api.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC
        mirrorSiteObj.api.updateDatetime = schedDatetime
        mirrorSiteObj.api.progress = None
        mirrorSiteObj.updateObj.start(schedDatetime)

    def _notifyProgress(self, mirrorSiteObj, progress):
        mirrorSiteObj.api.updateProgress = progress
        if progress == 100:
            mirrorSiteObj.api.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
