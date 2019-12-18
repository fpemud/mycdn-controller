#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import sys
import time
import subprocess


def shellCallWithRetCode(cmd):
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         shell=True, universal_newlines=True)
    if ret.returncode > 128:
        time.sleep(1.0)
    return (ret.returncode, ret.stdout.rstrip())


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("syntax: check-cache.py <cache-directory>")
        sys.exit(1)

    cacheDir = sys.argv[1]
    for fn in sorted(os.listdir(cacheDir)):
        if re.match("DP_.*\\.7z", fn) is None:
            continue
        fullfn = os.path.join(cacheDir, fn)
        rc, out = shellCallWithRetCode("/usr/bin/7z t %s" % (fullfn))
        if rc == 0:
            print("Ok:   %s" % (fn))
        else:
            print("Fail: %s" % (fn))
