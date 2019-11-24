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


class Updater:

    MAX_PAGE = 10
    PROGRESS_STAGE_1 = 20
    PREOGRES_STAGE_2 = 79

    def init(self, api):
        self._api = api
        self._doWork()

    def update(self, api):
        self._api = api
        self._doWork()

    def _doWork(self):
        linkDict = dict()
        fnSet = set()

        # fetch web pages
        # retrived from "http://driveroff.net/category/dp", it's in russian, do use google webpage translator
        for i in range(1, self.MAX_PAGE):
            found = False
            url = "http://www.gigabase.com/folder/cbcv8AZeKsHjAkenvVrjPQBB?page=%d" % (i)
            root = _Util.getWebPageElementTree(url)
            for elem in root.xpath(".//a"):
                if elem.text is None:
                    continue
                if not elem.text.startswith("DP_"):
                    continue
                m = re.match("(DP_.*)_([0-9]+)\\.7z", elem.text)
                if m.group(1) in linkDict:
                    if m.group(2) < linkDict[m.group(1)][1]:
                        continue
                linkDict[m.group(1)] = (m.group(0), m.group(2), elem.attrib["href"])
                found = True
            if not found:
                break
            self._api.progress_changed(self.PROGRESS_STAGE_1 * i // self.MAX_PAGE)
        self._api.progress_changed(self.PROGRESS_STAGE_1)

        # download driver pack file one by one
        i = 1
        total = len(linkDict)
        for prefix, v in linkDict.items():
            filename, timeStr, url = v
            fullfn = os.path.join(self._api.get_data_dir(), filename)
            if not os.path.exists(fullfn):
                # get real download url, gigabase sucks
                downloadUrl = None
                if True:
                    for elem in _Util.getWebPageElementTree(url).xpath(".//a"):
                        if elem.text == "Download file":
                            downloadUrl = elem.attrib["href"]
                            break
                    assert downloadUrl is not None

                # download
                logFile = os.path.join(self._api.get_log_dir(), "wget.log")
                _Util.shellCall("/usr/bin/wget -O \"%s\" \"%s\" >\"%s\" 2>&1" % (fullfn + ".tmp", downloadUrl, logFile))
                os.rename(fullfn + ".tmp", fullfn)
            fnSet.add(filename)
            self._api.progress_changed(self.PROGRESS_STAGE_1 + self.PROGRESS_STAGE_2 * i // total)
            i += 1

        # clear old files in cache
        for fn in (set(os.listdir(self._api.get_data_dir())) - fnSet):
            fullfn = os.path.join(self._api.get_data_dir(), fn)
            os.unlink(fullfn)

        # report full progress
        self._api.progress_changed(100)


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
