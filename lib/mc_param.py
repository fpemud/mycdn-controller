#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os


class McConst:

    etcDir = "/etc/mirrors"
    libDir = "/usr/lib64/mirrors"
    pluginsDir = os.path.join(libDir, "plugins")
    varDir = "/var/lib/mirrors"
    cacheDir = "/var/cache/mirrors"
    runDir = "/run/mirrors"
    logDir = "/var/log/mirrors"
    tmpDir = "/tmp/mirrors"             # FIXME

    updaterLogFileSize = 10 * 1024 * 1024
    updaterLogFileCount = 2

    apiServerFile = os.path.join(runDir, "api.socket")

    avahiSupport = True


class McParam:

    def __init__(self):
        self.cfg = None

        self.pluginList = []
        self.mirrorSiteDict = dict()

        self.listenIp = "0.0.0.0"

        self.httpPort = 80      # can be "random"
        self.ftpPort = 21       # can be "random"
        self.rsyncPort = 873    # can be "random"

        # objects
        self.mainloop = None
        self.pluginManager = None
        self.avahiObj = None
        self.updater = None
        self.httpServer = None
        self.ftpServer = None
        self.rsyncServer = None
        self.advertiser = None
