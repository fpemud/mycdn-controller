#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import logging
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
                self.scheduler.addJob(ms.id, ms.schedExpr, lambda schedDatetime: self._startUpdate(ms, schedDatetime))
            elif ms.sched == McMirrorSite.SCHED_FOLLOW:
                followMsObj = self.param.getMirrorSite(ms.followMirrorSiteId)
                print(ms.followMirrorSiteId)
                assert followMsObj is not None
                self.scheduler.addJob(ms.id, followMsObj.schedExpr, lambda schedDatetime: self._startUpdate(ms, schedDatetime))
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

    def _startUpdate(self, msObj, schedDatetime):
        msObj.updaterObjApi.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC
        msObj.updaterObjApi.updateDatetime = schedDatetime
        msObj.updaterObjApi.progress = 0
        msObj.updateObj.start(schedDatetime)
        logging.info("Mirror site %s updating scheduled on %s starts." % (msObj.id, msObj.updaterObjApi.updateDatetime.isoformat()))

    def _notifyProgress(self, msObj, progress, finished):
        msObj.updaterObjApi.updateProgress = progress
        if finished:
            assert progress == 100
            msObj.updaterObjApi.updateStatus = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            logging.info("Mirror site %s updating finished." % (msObj.id))
        else:
            logging.info("Mirror site %s updating progress %d." % (msObj.id, progress))
