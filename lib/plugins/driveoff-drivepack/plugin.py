#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import io
import re
import gi
import gzip
import lxml
import threading
import urllib.request
from collections import OrderedDict
gi.require_version("Soup", "2.4")
from gi.repository import GLib
from gi.repository import Soup


class Updater:

    def __init__(self, api):
        self.api = api
        self.linkDict = None
        self.urlOpener = None
        self.wgetProc = None

    def init_start(self):
        assert self.linkDict is None
        self._getLinkDict()

    def init_stop(self):
        assert self.linkDict is not None
        self._stop()

    def update_start(self, schedDatetime):
        assert self.linkDict is None
        self._getLinkDict()

    def update_stop(self):
        assert self.linkDict is not None
        self._stop()

    def _getLinkDict(self):
        self.linkDict = OrderedDict()

        # fetch web pages
        # retrived from "http://driveroff.net/category/dp", it's in russian, do use google webpage translator
        urlList = []
        for i in range(1, 3):
            urlList.append("http://www.gigabase.com/folder/cbcv8AZeKsHjAkenvVrjPQBB?page=%d" % (i))
        self.urlOpener = _GLibUrlOpener(urlList, self._urlOpenCallback, self._downloadOneFile)

    def _urlOpenCallback(self, i, url, statusCode, responseBody):
        if statusCode == 200:
            return

        # parse and get all the driver pack url
        root = lxml.html.parse(responseBody)
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
            self.wgetProc = _GLibWgetProc(downloadUrl, self.api.get_data_dir(), filename, logFile, self._downloadOneFileCallback)

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
            # clear redundant files
            for fn in (set(os.listdir(self.api.get_data_dir())) - fnSet):
                fullfn = os.path.join(self.api.get_data_dir(), fn)
                os.unlink(fullfn)
            self.api.notify_progress(100)
            self.wgetProc = None
            self.urlOpener = None
            self.linkDict = None
        else:
            self.api.notify_progress(100 * count // len(self.linkDict))
            self._downloadOneFile()

    def _stop(self):
        if self.wgetProc is not None:
            self.wgetProc.terminate()
            self.wgetProc = None
        if self.urlOpener is not None:
            self.urlOpener.stop()
            self.urlOpener = None
        self.linkDict = None


class _UpdateThread(threading.Thread):

    def __init__(self, api):
        self.api = api
        self.progress = 0
        self.stop = False

    def run(self):
        logFile = os.path.join(self.api.get_log_dir(), "wget.log")

        # fetch web pages
        # retrived from "http://driveroff.net/category/dp", it's in russian, do use google webpage translator
        linkDict = OrderedDict()
        i = 1
        while True:
            url = "http://www.gigabase.com/folder/cbcv8AZeKsHjAkenvVrjPQBB?page=%d" % (i)
            root = _Util.getWebPageElementTree(url)
            found = False
            for elem in root.xpath(".//a"):
                if elem.text is None:
                    continue
                if not elem.text.startswith("DP_"):
                    continue
                m = re.match("(DP_.*)_([0-9]+)\\.7z", elem.text)
                if m.group(1) in self.linkDict:
                    if m.group(2) < self.linkDict[m.group(1)][1]:
                        continue
                linkDict[m.group(1)] = (m.group(0), m.group(2), elem.attrib["href"])
                found = True
            if not found:
                break
            i += 1

        # download driver pack file one by one
        i = 0
        for prefix, v in linkDict.items():
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
            self.wgetProc = _GLibWgetProc(downloadUrl, self.api.get_data_dir(), filename, logFile, self._downloadOneFileCallback)





class _GLibWgetProc:

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


class _GLibUrlOpener:

    def __init__(self, urlList, urlOpenCallback, endCallback):
        self.urlOpenCallback = urlOpenCallback
        self.endCallback = endCallback

        self.session = Soup.Session()
        self.retSet = set(range(0, len(urlList)))
        for i in range(0, len(urlList)):
            url = urlList[i]
            msg = Soup.Message("GET", url)
            self.session.queue_message(msg, lambda a1, a2: self._urlOpenCallback(a1, a2, i, url))

    def stop(self):
        # FIXME
        pass

    def _urlOpenCallback(self, session, msg, i, url):
        self.urlOpenCallback(i, url, msg.status_code, msg.response_body)
        self.retSet.remove(i)
        if len(self.retSet) == 0:
            self.endCallback()
            del self.session


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
