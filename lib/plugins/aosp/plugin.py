#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import io
import re
import gzip
import time
import subprocess
import lxml.html
import urllib.request


class Database:

    def __init__(self):
        self.db = [
            "": {
            },
        ]

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
        _Util.cmdCall("/bin/tar -x -C \"%s\" -f \"%s\"" % (api.get_data_dir(), dstFile))
        _Util.forceDelete(dstFile)

    def update(self, api):
        pass


class _Util:

    @staticmethod
    def getWebPageElementTree(url):
        for i in range(0, 3):
            try:
                resp = urllib.request.urlopen(url)
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


# class _GLibWgetProc:

#     def __init__(self, url, dir, filename, logFile, endCallback):
#         self.dir = dir
#         self.filename = filename
#         self.fullfn = os.path.join(dir, filename)
#         self.tmpfullfn = self.fullfn + ".tmp"
#         targc, targv = GLib.shell_parse_argv("/bin/sh -c \"/usr/bin/wget -O %s %s >%s 2>&1\"" % (self.tmpfullfn, url, logFile))
#         ret = GLib.spawn_async(targv, flags=GLib.SpawnFlags.DO_NOT_REAP_CHILD)
#         if not ret[0]:
#             raise Exception("failed to create process")
#         self.pid = ret[1]
#         self.endCallback = endCallback
#         self.pidWatch = GLib.child_watch_add(self.pid, self._exitCallback)

#     def terminate(self):
#         # FIXME
#         pass

#     def _exitCallback(self, status, data):
#         try:
#             GLib.spawn_check_exit_status(status)
#             os.rename(self.tmpfullfn, self.fullfn)
#             self.endCallback(self.dir, self.filename, True)
#         except GLib.GError:
#             self.endCallback(self.dir, self.filename, False)
#         finally:
#             GLib.source_remove(self.pidWatch)
#             self.pidWatch = None
#             GLib.spawn_close_pid(self.pid)
#             self.pid = None


# class _GLibUrlOpener:

#     def __init__(self, urlList, urlOpenCallback, endCallback):
#         self.urlOpenCallback = urlOpenCallback
#         self.endCallback = endCallback

#         self.session = Soup.Session()
#         self.retSet = set(range(0, len(urlList)))
#         for i in range(0, len(urlList)):
#             url = urlList[i]
#             msg = Soup.Message("GET", url)
#             self.session.queue_message(msg, lambda a1, a2: self._urlOpenCallback(a1, a2, i, url))

#     def stop(self):
#         # FIXME
#         pass

#     def _urlOpenCallback(self, session, msg, i, url):
#         self.urlOpenCallback(i, url, msg.status_code, msg.response_body)
#         self.retSet.remove(i)
#         if len(self.retSet) == 0:
#             self.endCallback()
#             del self.session
