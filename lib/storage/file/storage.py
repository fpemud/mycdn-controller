#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-


class Storage:

    def __init__(self, param):
        self._mirrorSiteDict = param["mirror-sites"]

    def dispose(self):
        pass

    def get_param(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "data-directory": self._mirrorSiteDict[mirror_site_id]["data-directory"],
        }
