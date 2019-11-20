#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os


class McParam:

    def __init__(self):
        self.etcDir = "/etc/mycdn"
        self.libDir = "/usr/lib/mycdn"
        self.pluginsDir = os.path.join(self.libDir, "plugins")
        self.cacheDir = "/var/cache/mycdn"
        self.runDir = "/run/mycdn"
        self.logDir = "/var/log/mycdn"
        self.tmpDir = "/tmp/mycdn"

        self.cfg = None

        self.pluginList = []
        self.publicMirrorDatabaseList = []
        self.mirrorSiteList = []

        self.listenIp = "0.0.0.0"

        self.apiPort = 2300
        self.httpPort = 80      # can be "random"
        self.ftpPort = 21       # can be "random"
        self.rsyncPort = 1001   # can be "random"       # FIXME

        self.avahiSupport = True

        # objects
        self.mainloop = None
        self.pluginManager = None
        self.apiServer = None
        self.avahiObj = None
        self.updater = None
        self.advertiser = None


class McConfig:

    def __init__(self):
        self.bLocalOnly = False

        self.apiPort = None
        self.httpPort = None        # can be random
        self.ftpPort = None         # can be random
        self.rsyncPort = None       # can be random
