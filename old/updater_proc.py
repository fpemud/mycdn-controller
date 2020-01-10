#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import sys
import pickle
from datetime import datetime
sys.path.append('/usr/lib64/mirrors')
from mc_util import McUtil
from mc_util import DynObject
from mc_param import McConst
from mc_plugin import McPublicMirrorDatabase


class Main:

    def __init__(self):
        # get variables from arguments
        self.mirrorSiteId = sys.argv[1]
        if sys.argv[2] == "init":
            self.bInitOrUpdate = True
        elif sys.argv[2] == "update":
            self.bInitOrUpdate = False
        else:
            assert False

        # get variables from stdin
        self.tmpDir = self._readFrom()
        self.mirrorSiteDataDir = self._readFrom()

        self.realUpdaterObj = None
        if True:
            filename = self._readFrom()
            classname = self._readFrom()
            self.realUpdaterObj = McUtil.loadObject(filename, classname)

        self.api = None
        if self.bInitOrUpdate:
            self.api = self._createInitOrUpdateApi()
        else:
            schedDatetime = datetime.strptime(self._readFrom(), "%Y-%m-%d %H:%M")
            self.api = self._createInitOrUpdateApi(schedDatetime)

    def run(self):
        try:
            self.realUpdaterObj.run(self.api)
            if self.api is not None:
                self.api.progress_changed(100)
        except:
            if self.api is not None:
                self.api.error_occured(sys.exc_info())

    def printInfoCallback(self, message):
        obj = ("print-info", message)
        sys.stdout.buffer.write(pickle.dumps(obj))
        sys.stdout.buffer.write(b'\n')
        sys.stdout.buffer.flush()

    def printErrorCallback(self, message):
        pass

    def progressCallback(self, progress):
        # FIXME
        obj = ("progress", progress)
        sys.stdout.buffer.write(pickle.dumps(obj))
        sys.stdout.buffer.write(b'\n')
        sys.stdout.buffer.flush()

    def errorCallback(self, exc_info):
        # FIXME
        obj = ("errror", exc_info)
        sys.stdout.buffer.write(pickle.dumps(obj))
        sys.stdout.buffer.write(b'\n')
        sys.stdout.buffer.flush()

    def errorAndHoldForCallback(self, seconds, exc_info):
        # FIXME
        obj = ("errror-and-hold-for", seconds, exc_info)
        sys.stdout.buffer.write(pickle.dumps(obj))
        sys.stdout.buffer.write(b'\n')
        sys.stdout.buffer.flush()

    def _readFrom(self):
        return sys.stdin.readline().rstrip("\n")

    def _createInitOrUpdateApi(self, schedDatetime=None):
        api = DynObject()
        api.get_country = lambda: "CN"
        api.get_location = lambda: None
        api.get_data_dir = lambda: self.mirrorSiteDataDir
        api.get_log_dir = lambda: McConst.logDir
        api.get_public_mirror_database = lambda: self.db

        if schedDatetime is not None:
            # means update api
            api.get_sched_datetime = lambda: schedDatetime

        api.print_info = self.printInfoCallback
        api.print_error = self.printErrorCallback
        api.progress_changed = self.progressCallback
        api.error_occured = self.errorCallback
        api.error_occured_and_hold_for = self.errorAndHoldForCallback

        return api


###############################################################################

if __name__ == "__main__":
    obj = Main()
    obj.run()