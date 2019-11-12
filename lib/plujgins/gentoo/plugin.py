#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import shutil
import multiprocessing


class InfoProvider:

    def getInformation(self):
        pass


class Updater:

    def __init__(self, param, api):
        self.param = param
        self.api = api

        self.source = "rsync://ftp.ussg.iu.edu::gentoo-distfiles"

    def start(self):
        cmd = "/usr/bin/rsync -q -a --delete %s \"%s\"" % (self.api.get_data_directory())
        ret = subprocess.Popen(cmd, shell=True, universal_newlines=True)

        _Util.shellExec()

    def stop(self):
        pass


class _Util:

    @staticmethod
    def shellExec(cmd):
        ret = subprocess.run(cmd, shell=True, universal_newlines=True)
        if ret.returncode > 128:
            time.sleep(1.0)
        ret.check_returncode()