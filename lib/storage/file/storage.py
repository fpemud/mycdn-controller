#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os


class Storage:

    def __init__(self, param):
        self._mirrorSiteDict = param["mirror-sites"]
        for msId in self._mirrorSiteDict:
            _Util.ensureDir(self._dataDir(msId))

    def dispose(self):
        pass

    def get_param(self, mirror_site_id):
        return {
            "data-directory": self._dataDir(mirror_site_id),
        }

    def _dataDir(self, mirrorSiteId):
        return os.path.join(self._mirrorSiteDict[mirrorSiteId]["master-directory"], "storage-file")


class _Util:

    @staticmethod
    def ensureDir(dirname):
        if not os.path.exists(dirname):
            os.makedirs(dirname)
