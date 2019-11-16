#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os


class McParam:

    def __init__(self):
        self.etcDir = "/etc/mycdn"
        self.libDir = "/usr/lib/mycdn"
        self.cacheDir = "/var/cache/mycdn"
        self.runDir = "/run/mycdn"
        self.logDir = "/var/log/mycdn"
        self.tmpDir = "/tmp/mycdn"

        self.pluginsDir = os.path.join(self.libDir, "plugins")
        self.pluginList = []

        self.listenIp = "0.0.0.0"

        self.apiPort = 2300
        self.httpPort = 80      # can be "random"
        self.ftpPort = 21       # can be "random"

        self.updater = None
        self.advertiser = None
        self.apiServer = None
        self.mainloop = None

    def getMirrorSiteList(self):
        ret = []
        for plugin in self.pluginList:
            ret += plugin.objMirrorSiteList
        return ret

    def getMirrorSite(self, mirrorSiteId):
        for plugin in self.pluginList:
            for ms in plugin.objMirrorSiteList:
                if ms.id == mirrorSiteId:
                    return ms
        return None
