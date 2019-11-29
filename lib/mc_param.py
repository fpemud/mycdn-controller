#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os


class McParam:

    def __init__(self):
        self.etcDir = "/etc/mirrors"
        self.libDir = "/usr/lib64/mirrors"
        self.pluginsDir = os.path.join(self.libDir, "plugins")
        self.libexecDir = "/usr/libexec/mirrors"
        self.updaterExe = os.path.join(self.libexecDir, "updater_proc.py")
        self.cacheDir = "/var/cache/mirrors"
        self.runDir = "/run/mirrors"
        self.logDir = "/var/log/mirrors"
        self.tmpDir = "/tmp/mirrors"

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
