#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

from apscheduler.schedulers.background import BackgroundScheduler


class McUpdater:

    def __init__(self, param):
        self.param = param
        self.scheduler = CronScheduler()
        self.updateStatusDict = dict()

        # initialize data directory
        for ms in self.param.getMirrorSiteList():
            fullDir = os.path.join(self.param.cacheDir, ms.dataDir)
            McUtil.ensureDir(fullDir)

        # start init jobs if neccessary
        for ms in self.param.getMirrorSiteList():
            if os.path.exists(os.path.join(fullDir, ".not_initialized")):
                assert False

        # start regular update jobs
        for ms in self.param.getMirrorSiteList():
            if ms.sched == McMirorSite.SCHED_ONESHOT:
                assert False
            elif ms.sched == McMirorSite.SCHED_PERIODICAL:
                self.sched.addJob(ms.id, ms.schedExpr, lambda: self._startUpdate(ms.updaterObj))
            elif ms.sched == McMirorSite.SCHED_FOLLOW:
                followMsObj = self.param.getMirrorSite(ms.followMirrorSiteId)
                assert followMirrorSiteId is not None
                self.sched.addJob(ms.id, followMsObj.schedExpr, lambda: self._startUpdate(ms.updaterObj))
            elif ms.sched == McMirorSite.SCHED_PERSIST:
                assert False
            else:
                assert False

    def dispose(self):
        self.scheduler.dispose()
        self.scheduler = None

    def getMirrorSiteUpdateStatus(self):
        return dict()

    def _startUpdate(self, updaterObj):
        updateObj.start()
        # FIXME