#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import sys
import time
import random
import subprocess


def main():
    dataDir = sys.argv[1]
    logDir = sys.argv[3]
    rsyncSource = "rsync://mirrors.tuna.tsinghua.edu.cn/gentoo"
    logFile = os.path.join(logDir, "rsync.log")
    _Util.shellCall("/usr/bin/rsync -a -z --no-motd --delete %s %s >%s 2>&1" % (rsyncSource, dataDir, logFile))


class _Util:

    @staticmethod
    def shellCall(cmd):
        # call command with shell to execute backstage job
        # scenarios are the same as FmUtil.cmdCall

        ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             shell=True, universal_newlines=True)
        if ret.returncode > 128:
            # for scenario 1, caller's signal handler has the oppotunity to get executed during sleep
            time.sleep(1.0)
        if ret.returncode != 0:
            ret.check_returncode()
        return ret.stdout.rstrip()


###############################################################################

if __name__ == "__main__":
    main()
