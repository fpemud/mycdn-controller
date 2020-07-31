#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import pwd
import grp


class McConst:

    etcDir = "/etc/mirrors"
    libDir = "/usr/lib64/mirrors"
    libexecDir = "/usr/libexec/mirrors"
    pluginsDir = os.path.join(libDir, "plugins")

    varDir = "/var/lib/mirrors"
    cacheDir = "/var/cache/mirrors"
    logDir = "/var/log/mirrors"
    runDir = "/run/mirrors"
    tmpDir = "/tmp/mirrors"             # FIXME

    user = "mirrors"
    group = "mirrors"
    uid = pwd.getpwnam(user).pw_uid
    gid = grp.getgrnam(group).gr_gid

    updaterLogFileSize = 10 * 1024 * 1024
    updaterLogFileCount = 2

    pidFile = os.path.join(runDir, "mirrors.pid")
    apiServerFile = os.path.join(runDir, "api.socket")

    avahiSupport = True
    avahiServiceName = "_mirrors._tcp"


class McParam:

    def __init__(self):
        self.cfg = None

        self.pluginList = []
        self.mirrorSiteDict = dict()

        self.listenIp = "0.0.0.0"

        self.httpPort = "random"
        self.ftpPort = "random"
        self.rsyncPort = "random"

        # objects
        self.mainloop = None
        self.pluginManager = None
        self.avahiObj = None
        self.updater = None
        self.httpServer = None
        self.ftpServer = None
        self.rsyncServer = None
        self.advertiser = None
