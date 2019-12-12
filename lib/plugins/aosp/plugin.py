#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import time
import shutil
import subprocess


class Initializer:

    def run(self, api):
        dstFile = os.path.join(api.get_data_dir(), "aosp-latest.tar")
        usedFile = dstFile + ".used"

        if not os.path.exists(usedFile):
            # download
            if True:
                url = "https://mirrors.tuna.tsinghua.edu.cn/aosp-monthly/aosp-latest.tar"
                logFile = os.path.join(api.get_log_dir(), "wget.log")
                _Util.shellCall("/usr/bin/wget -c -O \"%s\" \"%s\" >\"%s\" 2>&1" % (dstFile, url, logFile))
            api.progress_changed(50)

            # clear directory
            for fn in os.listdir(api.get_data_dir()):
                fullfn = os.path.join(api.get_data_dir(), fn)
                if fullfn != dstFile:
                    _Util.forceDelete(fullfn)
            api.progress_changed(55)

            # extract
            # sometimes tar file contains minor errors
            _Util.shellCallIgnoreResult("/bin/tar -x --strip-components=1 -C \"%s\" -f \"%s\"" % (api.get_data_dir(), dstFile))
            os.rename(dstFile, usedFile)
            api.progress_changed(60)
        else:
            api.progress_changed(60)

        # sync
        with _TempChdir(api.get_data_dir()):
            logFile = os.path.join(api.get_log_dir(), "repo.log")
            _Util.shellCall("/usr/bin/repo sync >\"%s\" 2>&1" % (logFile))
        api.progress_changed(99)

        # all done, delete the tar file
        _Util.forceDelete(usedFile)
        api.progress_changed(100)


class Updater:

    def run(self, api):
        with _TempChdir(api.get_data_dir()):
            logFile = os.path.join(api.get_log_dir(), "repo.log")
            _Util.shellCall("/usr/bin/repo sync >\"%s\" 2>&1" % (logFile))


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

    @staticmethod
    def shellCallIgnoreResult(cmd):
        ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             shell=True, universal_newlines=True)
        if ret.returncode > 128:
            time.sleep(1.0)


class _TempChdir:

    def __init__(self, dirname):
        self.olddir = os.getcwd()
        os.chdir(dirname)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        os.chdir(self.olddir)
