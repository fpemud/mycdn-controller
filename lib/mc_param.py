#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os


class McParam:

    def __init__(self):
        self.etcDir = "/etc/mycdn-controller"
        self.libDir = "/usr/lib/mycdn-controller"
        self.dataDir = "/usr/share/mycdn-controller"
        self.cacheDir = "/var/cache/mycdn-controller"
        self.tmpDir = "/tmp/mycdn-controller"

        self.sourcesDir = os.path.join(self.libDir, "sources")
        self.mirrorsDir = os.path.join(self.libDir, "mirrors")
        self.helpersDir = os.path.join(self.libDir, "helpers")

        self.caCertFile = os.path.join(self.etcDir, "ca-cert.pem")
        self.caKeyFile = os.path.join(self.etcDir, "ca-privkey.pem")

        self.syncTaskInterval = 60
        self.apiPort = 3220

        self.mainloop = None
