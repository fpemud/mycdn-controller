#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import io
import gzip
import time
import certifi
import subprocess
import lxml.html
import urllib.request


class Updater:

    def init(self, api):
        # download
        url = "https://mirrors.tuna.tsinghua.edu.cn/aosp-monthly/aosp-latest.tar"
        dstFile = os.path.join(api.get_data_dir(), "aosp-latest.tar")
        logFile = os.path.join(api.get_log_dir(), "wget.log")
        _Util.shellCall("/usr/bin/wget -c -O \"%s\" \"%s\" >\"%s\" 2>&1" % (dstFile, url, logFile))

        # clear directory
        for fn in os.listdir(api.get_data_dir()):
            fullfn = os.path.join(api.get_data_dir(), fn)
            if fullfn != dstFile:
                _Util.forceDelete(fullfn)

        # extract
        _Util.shellCall("/bin/tar -x -C \"%s\" -f \"%s\"" % (api.get_data_dir(), dstFile))
        _Util.forceDelete(dstFile)

    def update(self, api):
        pass


class _Util:

    @staticmethod
    def getWebPageElementTree(url):
        for i in range(0, 3):
            try:
                resp = urllib.request.urlopen(url, timeout=60, cafile=certifi.where())
                if resp.info().get('Content-Encoding') is None:
                    fakef = resp
                elif resp.info().get('Content-Encoding') == 'gzip':
                    fakef = io.BytesIO(resp.read())
                    fakef = gzip.GzipFile(fileobj=fakef)
                else:
                    assert False
                return lxml.html.parse(fakef)
            except urllib.error.URLError as e:
                if isinstance(e.reason, TimeoutError):
                    pass                                # retry 3 times
                else:
                    raise

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

    @staticmethod
    def shellCallWithRetCode(cmd):
        ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             shell=True, universal_newlines=True)
        if ret.returncode > 128:
            time.sleep(1.0)
        return (ret.returncode, ret.stdout.rstrip())
