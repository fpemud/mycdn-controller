#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import io
import gzip
import lxml
import time
import urllib
import certifi
import subprocess


class InitAndUpdater:

    def run(self, api):
        self._api = api
        db = self._api.get_public_mirror_database()

        # download distfiles by wget
        distfilesUrl = os.path.join(db.query(self._api.get_country(), self._api.get_location(), ["http"], True)[0], "distfiles")
        distfilesDir = os.path.join(self._api.get_data_dir(), "distfiles")
        logFile = os.path.join(api.get_log_dir(), "wget.log")
        wgetArgs = []
        if True:
            wgetArgs.append("-e robots=off")
            wgetArgs.append("-m")
            wgetArgs.append("-np")                          # --no-parent
            wgetArgs.append("-nH")                          # --no-host-directories
            wgetArgs.append("-nc")                          # --no-clobber
            wgetArgs.append("--reject \"index.html\"")
        for elem in _Util.getWebPageElementTree(distfilesUrl).xpath(".//a"):
            if elem.href.startswith("/"):
                continue    # absolute path
            if elem.href.startswith("."):
                continue    # parent path or myself
            if elem.href.endswith("/"):
                myUrl = os.path.join(distfilesUrl, elem.href)
                myDir = os.path.join(distfilesDir, elem.href)
                _Util.ensureDir(myDir)
                _Util.shellCall("/usr/bin/wget %s --cut-dirs=2 -P \"%s\" %s >%s 2>&1" % (" ".join(wgetArgs), myDir, myUrl, logFile))

        # rsync
        source = db.query(self._api.get_country(), self._api.get_location(), ["rsync"], True)[0]
        dataDir = self._api.get_data_dir()
        logFile = os.path.join(self._api.get_log_dir(), "rsync.log")
        _Util.shellCall("/usr/bin/rsync -a -z --delete %s %s >%s 2>&1" % (source, dataDir, logFile))


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
