#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import pwd
import grp


class McConst:

    etcDir = "/etc/mirrors"
    libDir = "/usr/lib64/mirrors"
    pluginsDir = os.path.join(libDir, "plugins")
    varDir = "/var/lib/mirrors"
    cacheDir = "/var/cache/mirrors"
    runDir = "/run/mirrors"
    logDir = "/var/log/mirrors"
    tmpDir = "/tmp/mirrors"             # FIXME

    user = "mirrors"
    group = "mirrors"
    uid = pwd.getpwnam(user).pw_uid
    gid = grp.getgrnam(group).gr_gid

    dataDirMode = 0o700
    logDirMode = 0o750
    runDirMode = 0o755
    tmpDirMode = 0o755

    updaterLogFileSize = 10 * 1024 * 1024
    updaterLogFileCount = 2

    pidFile = os.path.join(runDir, "mirrors.pid")
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
