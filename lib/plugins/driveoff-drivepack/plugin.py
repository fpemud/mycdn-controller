#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import io
import gzip
import lxml
import urllib.request
from collections import OrderedDict
from gi.repository import GLib


class Updater:

    def __init__(self, api):
        self.api = api
        self.linkDict = None
        self.wgetProc = None

    def init_start(self):
        assert self.linkDict is None
        self._getLinkDict()
        self._downloadOneFile()

    def init_stop(self):
        assert self.linkDict is not None
        if self.wgetProc is not None:
            self.wgetProc.terminate()
            self.wgetProc = None
        self.linkDict = None

    def update_start(self, schedDatetime):
        assert self.linkDict is not None
        self._getLinkDict()
        self._downloadOneFile()

    def update_stop(self):
        assert self.linkDict is not None
        if self.wgetProc is not None:
            self.wgetProc.terminate()
            self.wgetProc = None
        self.linkDict = None

    def _getLinkDict(self):
        # fetch main web page
        urlCurrent = "http://www.gigabase.com/folder/cbcv8AZeKsHjAkenvVrjPQBB"       # retrived from "http://driveroff.net/category/dp", it's in russian, do use google webpage translator
        root = _Util.getWebPageElementTree(urlCurrent)

        # parse and get all the driver pack url
        self.linkDict = OrderedDict()                    # prefix:(file-name,file-date,href)
        for elem in root.xpath(".//a"):
            if elem.text is None:
                continue
            if not elem.text.startswith("DP_"):
                continue
            m = re.match("(DP_.*)_([0-9]+)\\.7z", elem.text)
            if m.group(1) in self.linkDict:
                if m.group(2) < self.linkDict[m.group(1)][1]:
                    continue
            self.linkDict[m.group(1)] = (m.group(0), m.group(2), elem.attrib["href"])

    def _downloadOneFile(self):
        logFile = os.path.join(self.api.get_log_dir(), "wget.log")

        # download driver pack file one by one
        for prefix, v in self.linkDict.items():
            filename, timeStr, url = v
            fullfn = os.path.join(self.api.get_data_dir(), filename)

            # check if file is cached
            if os.path.exists(fullfn):
                continue

            # get real download url, gigabase sucks
            downloadUrl = None
            if True:
                for elem in _Util.getWebPageElementTree(url).xpath(".//a"):
                    if elem.text == "Download file":
                        downloadUrl = elem.attrib["href"]
                        break
                assert downloadUrl is not None

            # download
            self.wgetProc = _WgetProc(downloadUrl, self.api.get_data_dir(), filename, logFile, self._downloadOneFileCallback)

    def _downloadOnFileCallback(self, dir, filename, success):
        # calculate progress
        count = 0
        fnSet = set()
        for filename, timeStr, url in self.linkDict.values():
            fnSet.add(filename)
            fullfn = os.path.join(self.api.get_data_dir(), filename)
            if os.path.exists(fullfn):
                count += 1

        if count == len(self.linkDict):
            self.api.notify_progress(100, True)

            # clear redundant files
            for fn in (set(os.listdir(self.api.get_data_dir())) - fnSet):
                fullfn = os.path.join(self.api.get_data_dir(), fn)
                os.unlink(fullfn)

            self.wgetProc = None
            self.linkDict = None
        else:
            self.api.notify_progress(100 * count // len(self.linkDict), False)
            self._downloadOneFile()


class _WgetProc:

    def __init__(self, url, dir, filename, logFile, endCallback):
        self.dir = dir
        self.filename = filename
        self.fullfn = os.path.join(dir, filename)
        self.tmpfullfn = self.fullfn + ".tmp"
        targc, targv = GLib.shell_parse_argv("/bin/sh -c \"/usr/bin/wget -O %s %s >%s 2>&1\"" % (self.tmpfullfn, url, logFile))
        ret = GLib.spawn_async(targv, flags=GLib.SpawnFlags.DO_NOT_REAP_CHILD)
        if not ret[0]:
            raise Exception("failed to create process")
        self.pid = ret[1]
        self.endCallback = endCallback
        self.pidWatch = GLib.child_watch_add(self.pid, self._exitCallback)

    def terminate(self):
        # FIXME
        pass

    def _exitCallback(self, status, data):
        try:
            GLib.spawn_check_exit_status(status)
            os.rename(self.tmpfullfn, self.fullfn)
            self.endCallback(self.dir, self.filename, True)
        except GLib.GError:
            self.endCallback(self.dir, self.filename, False)
        finally:
            GLib.source_remove(self.pidWatch)
            self.pidWatch = None
            GLib.spawn_close_pid(self.pid)
            self.pid = None


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
