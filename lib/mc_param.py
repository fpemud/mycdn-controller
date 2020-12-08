#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import pwd
import grp


class McConst:

    etcDir = "/etc/mirrors"
    libDir = "/usr/lib64/mirrors"
    libexecDir = "/usr/libexec/mirrors"
    storageDir = os.path.join(libDir, "storage")
    advertiserDir = os.path.join(libDir, "advertiser")
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

    mainCfgFile = os.path.join(etcDir, "main.conf")
    pluginCfgFileGlobPattern = os.path.join(etcDir, "plugin-*.conf")
    pidFile = os.path.join(runDir, "mirrors.pid")
    apiServerFile = os.path.join(runDir, "api.socket")

    avahiSupport = True
    avahiServiceName = "_mirrors._tcp"


class McParam:

    def __init__(self):
        self.listenIp = "0.0.0.0"
        self.mainPort = 2300
        self.webAcceptForeign = True

        self.mainCfg = {
            "preferedUpdatePeriodList": [],     # { "start": CRON-EXPRESSION, "time": HOURS }
            "country": "CN",
            "location": "",
        }

        # objects
        self.mainloop = None
        self.pluginManager = None
        self.mirrorSiteDict = dict()
        self.storageDict = dict()
        self.advertiserDict = dict()
        self.avahiObj = None
        self.updater = None
