#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os


class McParam:

    def __init__(self):
        self.etcDir = "/etc/mycdn"
        self.libDir = "/usr/lib/mycdn"
        self.dataDir = "/usr/share/mycdn"
        self.cacheDir = "/var/cache/mycdn"
        self.tmpDir = "/tmp/mycdn"

        self.pluginsDir = os.path.join(self.libDir, "plugins")

        self.mainloop = None
