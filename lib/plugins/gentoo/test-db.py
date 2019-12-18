#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import json
import time
import certifi
import urllib.request
import subprocess


def testGentoo(filename):
    print("Testing %s" % (filename))
    jobj = json.load(filename)
    for url in jobj.keys():
        sys.stdout.write("    Testing \"%s\" ... " % (url))
        try:
            urllib.request.urlopen(url, timeout=60, cafile=certifi.where())
            print("Ok.")
        except:
            print("Failed.")
    print("")


def testGentooPortage(filename):
    print("Testing %s" % (filename))
    jobj = json.load(filename)
    for url in jobj.keys():
        sys.stdout.write("    Testing \"%s\" ... " % (url))
        try:
            shellCall("/usr/bin/rsync -a --no-motd --list-only %s" % (url))
            print("Ok.")
        except:
            print("Failed.")
    print("")


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


if __name__ == "__main__":
    selfDir = os.path.dirname(os.path.realpath(__file__))
    testGentoo(os.path.join(selfDir, "db-official.json"))
    testGentoo(os.path.join(selfDir, "db-extended.json"))
    testGentooPortage(os.path.join(selfDir, "db-portage-official.json"))
    testGentooPortage(os.path.join(selfDir, "db-portage-extended.json"))
