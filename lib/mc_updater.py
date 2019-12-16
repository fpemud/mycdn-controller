#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import logging
from datetime import datetime
from gi.repository import GLib
from mc_util import McUtil
from mc_util import DynObject
from mc_util import GLibIdleInvoker
from mc_util import GLibCronScheduler
from mc_param import McConst


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
            fullDir = os.path.join(McConst.cacheDir, ms.dataDir)
            if not os.path.exists(fullDir):
                os.makedirs(fullDir)
                McUtil.touchFile(_initFlagFile(ms))

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

        if os.path.exists(_initFlagFile(self.mirrorSite)):
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT
            self.parent.invoker.add(self.initStart)
        else:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_IDLE
            self.parent.scheduler.addJob(self.mirrorSite.id, self.mirrorSite.schedExpr, self.updateStart)

    def initStart(self):
        self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INITING
        try:
            self.progress = 0
            self.mirrorSite.initializerObj.start(self._createInitOrUpdateApi())
            logging.info("Mirror site \"%s\" initialization starts." % (self.mirrorSite.id))
        except:
            self.reInitHandler = GLib.timeout_add_seconds(McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL, self._reInitCallback)
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL
            logging.error("Mirror site \"%s\" initialization failed, re-initialize in %d seconds." % (self.mirrorSite.id, McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL), exc_info=True)

    def initStop(self):
        self.mirrorSite.initializerObj.stop()

    def initProgressCallback(self, progress):
        assert progress >= self.progress
        if progress == self.progress:
            return

        if progress == 100:
            McUtil.forceDelete(_initFlagFile(self.mirrorSite))
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
        logging.error("Mirror site \"%s\" initialization failed, re-initialize in %d seconds." % (self.mirrorSite.id, McMirrorSiteUpdater.MIRROR_SITE_RE_INIT_INTERVAL), exc_info=exc_info)

    def initErrorAndHoldForCallback(self, seconds, exc_info):
        del self.progress
        self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_INIT_FAIL
        self.reInitHandler = GLib.timeout_add_seconds(seconds, self._reInitCallback)
        logging.error("Mirror site \"%s\" initialization failed, hold for %d seconds before re-initialization." % (self.mirrorSite.id, seconds), exc_info=exc_info)

    def updateStart(self, schedDatetime):
        tstr = schedDatetime.strftime("%Y-%m-%d %H:%M")
        if self.status == McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING:
            logging.info("Mirror site \"%s\" updating ignored on \"%s\", last update is not finished." % (self.mirrorSite.id, tstr))
        else:
            self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNCING
            try:
                self.progress = 0
                self.mirrorSite.updaterObj.start(self._createInitOrUpdateApi(schedDatetime))
                logging.info("Mirror site \"%s\" update triggered on \"%s\"." % (self.mirrorSite.id, tstr))
            except:
                self._resetStatus(McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL)
                logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id), exc_info=True)

    def updateStop(self):
        self.mirrorSite.updaterObj.stop()

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
        logging.error("Mirror site \"%s\" update failed." % (self.mirrorSite.id), exc_info=exc_info)

    def updateErrorAndHoldForCallback(self, seconds, exc_info):
        del self.progress
        self.status = McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_SYNC_FAIL
        self.parent.scheduler.pauseJob(self.mirrorSite.id, datetime.now() + datetime.timedelta(seconds=seconds))
        logging.error("Mirror site \"%s\" update failed, hold for %d seconds." % (self.mirrorSite.id, seconds), exc_info=exc_info)

    def _createInitOrUpdateApi(self, schedDatetime=None):
        api = DynObject()

        api.get_country = lambda: "CN"
        api.get_location = lambda: None
        api.get_data_dir = lambda: self.mirrorSite.dataDir
        api.get_log_dir = lambda: McConst.logDir
        api.get_public_mirror_database = lambda: _publicMirrorDatabase(self.param, self.mirrorSite)

        if schedDatetime is not None:
            api.get_sched_datetime = lambda: schedDatetime

        api.print_info = lambda message: logging.info(self.mirrorSite.id + ": " + message)
        api.print_error = lambda message: logging.error(self.mirrorSite.id + ": " + message)

        if schedDatetime is None:
            api.progress_changed = self.initProgressCallback
            api.error_occured = self.initErrorCallback
            api.error_occured_and_hold_for = self.initErrorAndHoldForCallback
        else:
            api.progress_changed = self.updateProgressCallback
            api.error_occured = self.updateErrorCallback
            api.error_occured_and_hold_for = self.updateErrorAndHoldForCallback

        return api

    def _reInitCallback(self):
        del self.reInitHandler
        self.initStart()
        return False


def _initFlagFile(mirrorSite):
    return os.path.join(McConst.cacheDir, mirrorSite.dataDir + ".uninitialized")


def _publicMirrorDatabase(param, mirrorSite):
    for pmd in param.publicMirrorDatabaseList:
        if pmd.id == mirrorSite.id:
            return pmd
    return None
