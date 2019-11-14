#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

from apscheduler.schedulers.background import BackgroundScheduler


class McUpdater:

    def __init__(self, param):
        self.param = param

        self.sched = CronScheduler()

        self.msDict = dict()
        for ms in self.param.mirrorSiteList:
            if ms.sched == McMirorSite.SCHED_ONESHOT:
                assert False
            elif ms.sched == McMirorSite.SCHED_PERIODICAL:
                self.sched.addJob(ms.id, ms.schedExpr, ms.updaterObj.start)
            elif ms.sched == McMirorSite.SCHED_AFTER:
                self.sched.addJob(ms.id, ms.schedExpr, ms.updaterObj.start)
            elif ms.sched == McMirorSite.SCHED_PERSIST:

            else:
                assert False


    def dispose(self):
        self.scheduler.shutdown()
        self.scheduler = None
