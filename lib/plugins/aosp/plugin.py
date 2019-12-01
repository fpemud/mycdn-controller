#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import time
import shutil
import subprocess


class Database:

    def __init__(self):
        # self.db = [
        #     "": {
        #     },
        # ]
        pass

    def get(self, extended=False):
        if not extended:
            return {}
        else:
            return self.dictExtended

    def query(self, country=None, location=None, protocolList=None, extended=False, maximum=1):
        assert location is None or (country is not None and location is not None)
        assert protocolList is None or all(x in ["http", "ftp", "rsync"] for x in protocolList)

        # select database
        srcDict = self.dictOfficial if not extended else self.dictExtended

        # country out of scope, we don't consider this condition
        if country is not None:
            if not any(x.get("country", None) == country for x in srcDict.values()):
                country = None
                location = None

        # location out of scope, same as above
        if location is not None:
            if not any(x["country"] == country and x.get("location", None) == location for x in srcDict.values()):
                location = None

        # do query
        ret = []
        for url, prop in srcDict.items():
            if len(ret) >= maximum:
                break
            if country is not None and prop.get("country", None) != country:
                continue
            if location is not None and prop.get("location", None) != location:
                continue
            if protocolList is not None and prop.get("protocol", None) not in protocolList:
                continue
            ret.append(url)
        return ret


class Updater:

    def init(self, api):
        # download
        if True:
            url = "https://mirrors.tuna.tsinghua.edu.cn/aosp-monthly/aosp-latest.tar"
            dstFile = os.path.join(api.get_data_dir(), "aosp-latest.tar")
            logFile = os.path.join(api.get_log_dir(), "wget.log")
            _Util.shellCall("/usr/bin/wget -c -O \"%s\" \"%s\" >\"%s\" 2>&1" % (dstFile, url, logFile))
        api.progress_changed(80)

        # clear directory
        for fn in os.listdir(api.get_data_dir()):
            fullfn = os.path.join(api.get_data_dir(), fn)
            if fullfn != dstFile:
                _Util.forceDelete(fullfn)
        api.progress_changed(82)

        # extract
        _Util.shellCall("/bin/tar -x --strip-components=1 -C \"%s\" -f \"%s\"" % (api.get_data_dir(), dstFile))
        api.progress_changed(90)

        # sync
        with _TempChdir(api.get_data_dir()):
            _Util.shellCall("/usr/bin/repo sync")
        api.progress_changed(99)

        # all done, delete the tar file
        _Util.forceDelete(dstFile)
        api.progress_changed(100)

    def update(self, api):
        with _TempChdir(api.get_data_dir()):
            _Util.shellCall("/usr/bin/repo sync")


class _Util:

    @staticmethod
    def forceDelete(filename):
        if os.path.islink(filename):
            os.remove(filename)
        elif os.path.isfile(filename):
            os.remove(filename)
        elif os.path.isdir(filename):
            shutil.rmtree(filename)

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


class _TempChdir:

    def __init__(self, dirname):
        self.olddir = os.getcwd()
        os.chdir(dirname)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        os.chdir(self.olddir)
