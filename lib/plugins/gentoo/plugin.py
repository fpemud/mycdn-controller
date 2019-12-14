#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import io
import gzip
import time
import certifi
import lxml.html
import urllib.request
import subprocess


PROGRESS_STAGE_1 = 10
PROGRESS_STAGE_2 = 70
PROGRESS_STAGE_3 = 20


class Initializer:

    def run(self, api):
        db = api.get_public_mirror_database()
        rsyncSource = db.query(api.get_country(), api.get_location(), ["rsync"], extended=True)[0]
        fileSource = db.query(api.get_country(), api.get_location(), ["http", "ftp"], True)[0]

        # stage1: create directories, get file list, ignore symlinks
        fileList = self._makeDirAndGetFileList(rsyncSource)
        api.progress_changed(PROGRESS_STAGE_1)

        # stage2: download file list
        logFile = os.path.join(api.get_log_dir(), "wget.log")
        for i in range(0, len(fileList)):
            fn = fileList[i]
            fullfn = os.path.join(api.get_data_dir(), fn)
            if not os.path.exists(fullfn):
                url = os.path.join(fileSource, fn)
                rc, out = _Util.shellCallWithRetCode("/usr/bin/wget -O \"%s\" %s >%s 2>&1" % (fullfn, url, logFile))
                if rc != 0 and rc != 8:
                    # ignore "file not found" error (8) since rsyncSource and fileSource may be different servers
                    raise Exception("download %s failed" % (url))
            api.progress_changed(PROGRESS_STAGE_1 + PROGRESS_STAGE_2 * i // len(fileList))
        api.progress_changed(PROGRESS_STAGE_1 + PROGRESS_STAGE_2)

        # stage3: rsync
        logFile = os.path.join(api.get_log_dir(), "rsync.log")
        _Util.shellCall("/usr/bin/rsync -a -z --delete %s %s >%s 2>&1" % (rsyncSource, api.get_data_dir(), logFile))

        # report full progress
        api.progress_changed(100)

    def _makeDirAndGetFileList(self, rsyncSource):
        out = _Util.shellCall("/usr/bin/rsync --list-only %s" % (rsyncSource))

        ret = []
        for line in out.split("\n"):
            m = re.match("(\\S+) +(\\S+) +(\\S+ \\S+) (.+)", line)
            if m is None:
                continue
            modstr = m.group(1)
            filename = m.group(4)
            if filename.startswith("."):
                continue
            if " -> " in filename:
                continue

            if modstr.startswith("d"):
                _Util.ensureDir(os.path.join(self._api.get_data_dir(), filename))
            else:
                ret.append(filename)

        return ret


class Updater:

    def run(self, api):
        db = self._api.get_public_mirror_database()
        source = db.query(self._api.get_country(), self._api.get_location(), ["rsync"], True)[0]
        dataDir = self._api.get_data_dir()
        logFile = os.path.join(self._api.get_log_dir(), "rsync.log")
        _Util.shellCall("/usr/bin/rsync -a -z --delete %s %s >%s 2>&1" % (source, dataDir, logFile))


class PortageInitAndUpdater(Updater):
    pass


class _Util:

    @staticmethod
    def ensureDir(dirname):
        if not os.path.exists(dirname):
            os.makedirs(dirname)

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


# class _ShellProc:

#     def __init__(self, cmd, finishCallback, errorCallback):
#         targc, targv = GLib.shell_parse_argv("/bin/sh -c \"%s\"" % (cmd))
#         self.pid = GLib.spawn_async(targv, flags=GLib.SpawnFlags.DO_NOT_REAP_CHILD)[0]
#         self.finishCallback = finishCallback
#         self.errorCallback = errorCallback
#         self.pidWatch = GLib.child_watch_add(self.pid, self._exitCallback)

#     def terminate(self):
#         # FIXME
#         pass

#     def _exitCallback(self, status, data):
#         try:
#             GLib.spawn_check_exit_status(status)
#             self.finishCallback()
#         except:
#             self.errorCallback(sys.exc_info())
#         finally:
#             GLib.source_remove(self.pidWatch)
#             self.pidWatch = None
#             GLib.spawn_close_pid(self.pid)
#             self.pid = None
