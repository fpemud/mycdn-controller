#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import sys
sys.path.append('/usr/lib64/mirrors')
from mc_util import McUtil
from mc_util import DynObject
from mc_param import McConst
from mc_plugin import McPublicMirrorDatabase


def main():
    bInitOrUpdate = (sys.argv[1] == "1")
    tmpDir = sys.argv[2]
    dataDir = sys.argv[3]
    filename = sys.argv[4]
    classname = sys.argv[5]

    realUpdaterObj = McUtil.loadObject(filename, classname)

    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: dataDir
    api.get_log_dir = lambda: McConst.logDir
    api.get_public_mirror_database = lambda: _publicMirrorDatabase(self.param, self.mirrorSite)
    api.progress_changed = self.initProgressCallback
    api.error_occured = self.initErrorCallback
    api.error_occured_and_hold_for = self.initErrorAndHoldForCallback









    try:
        realUpdaterObj.run(api)
        if self.api is not None:
            self.api.progress_changed(100)
    except:
        if self.api is not None:
            self.api.error_occured(sys.exc_info())




class _UpdaterObjProxyRuntimeThread:

    def __init__(self, filename, classname):
        self.threadObj = None
        self.realProgressChanged = None
        self.realErrorOccured = None
        self.realErrorOccuredAndHoldFor = None
        self.realUpdaterObj = McUtil.loadObject(filename, classname)

    def start(self, api):
        self.realProgressChanged = api.progress_changed
        self.realErrorOccured = api.error_occured
        self.realErrorOccuredAndHoldFor = api.error_occured_and_hold_for
        self.threadObj = _UpdaterObjProxyRuntimeThreadImpl(self, api, self.realUpdaterObj.init)
        self.threadObj.start()

    def stop(self):
        self.threadObj.stopped = True

    def _progressChangedIdleHandler(self, progress):
        self.realProgressChanged(progress)
        if progress == 100:
            self.threadObj = None
            self.realErrorOccuredAndHoldFor = None
            self.realErrorOccured = None
            self.realProgressChanged = None
        return False

    def _errorOccuredIdleHandler(self, exc_info):
        self.realErrorOccured(exc_info)
        self.threadObj = None
        self.realErrorOccuredAndHoldFor = None
        self.realErrorOccured = None
        self.realProgressChanged = None
        return False

    def _errorOccuredAndHoldForIdleHandler(self, seconds, exc_info):
        self.realErrorOccured(seconds, exc_info)
        self.threadObj = None
        self.realErrorOccuredAndHoldFor = None
        self.realErrorOccured = None
        self.realProgressChanged = None
        return False


class _UpdaterObjProxyRuntimeThreadImpl(threading.Thread):

    def __init__(self, parent, api, targetFunc):
        super().__init__()
        self.parent = parent
        self.targetFunc = targetFunc
        self.stopped = False
        self.api = api
        self.api.is_stopped = lambda: self.stopped
        self.api.progress_changed = self._progressChanged
        self.api.error_occured = self._errorOccured
        self.api.error_occured_and_hold_for = self._errorOccuredAndHoldFor

    def run(self):
        try:
            self.targetFunc(self.api)
            if self.api is not None:
                self.api.progress_changed(100)
        except:
            if self.api is not None:
                self.api.error_occured(sys.exc_info())

    def _progressChanged(self, progress):
        if progress == 100:
            self.api = None
        GLib.idle_add(self.parent._progressChangedIdleHandler, progress)

    def _errorOccured(self, exc_info):
        self.api = None
        GLib.idle_add(self.parent._errorOccuredIdleHandler, exc_info)

    def _errorOccuredAndHoldFor(self, seconds, exc_info):
        self.api = None
        GLib.idle_add(self.parent._errorOccuredAndHoldForIdleHandler, seconds, exc_info)





def createInitApi(self):
    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: self.mirrorSite.dataDir
    api.get_log_dir = lambda: McConst.logDir
    api.progress_changed = self.initProgressCallback
    return api

def createUpdateApi(self):
    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: self.mirrorSite.dataDir
    api.get_log_dir = lambda: McConst.logDir
    api.progress_changed = self.initProgressCallback
    return api


if __name__ == "__main__":
    assert False
