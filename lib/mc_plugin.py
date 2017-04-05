#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
from mc_util import McUtil


class SourceObject:

    def getSyncThread(self):
        assert False


class SourceObjectInnerApi:

    def __init__(self, param, daemon, sobj):
        self.param = param
        self.daemon = daemon
        self.sobj = sobj

    def getCfgDir(self):
        return os.path.join(self.param.etcDir, McUtil.objpath(self.sobj))

    def getCacheDir(self):
        return os.path.join(self.param.cacheDir, McUtil.objpath(self.sobj))

    def getTmpDir(self):
        return self.param.tmpDir

    def getMyMirrors(self):
        return self.daemon.mirrorObjects[McUtil.objpath(self.sobj)]


class _SourceObjectSyncThread():

    def start(self):
        assert False

    def stop(self):
        assert False

    def join(self):
        assert False

    def get_progress(self):
        assert False


class MirrorObject:

    def isOnLine(self):
        assert False


class MirrorObjectInnerApi:

    def __init__(self, param, daemon, mobj):
        self.param = param
        self.daemon = daemon
        self.mobj = mobj

    def getCfgDir(self):
        return os.path.join(self.param.etcDir, McUtil.objpath(self.mobj, 2))

    def getHelper(self, name):
        return self.daemon.helperObjects[name]
