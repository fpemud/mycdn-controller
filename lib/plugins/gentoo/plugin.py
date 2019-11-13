#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import shutil
import threading


class Db:

    dictOfficial = None
    dictExtended = None

    def __init__(self):
        selfDir = os.path.dirname(os.path.realpath(__file__))

    if Db.dictOfficial is None:
        with open(os.path.join(selfDir, "db-official.json")) as f:
            Db.dictOfficial = json.load(f)

    if Db.dictExtended is None:
        Db.dictExtended = obj.deepcopy(Db.dictOfficial)
        with open(os.path.join(selfDir, "db-extended.json")) as f:
            Db.dictExtended.update(json.load(f))

    def get(self, extended=False):
        if not extended:
            return Db.dictOfficial
        else:
            return Db.dictExtended

    def query(self, country=None, location=None, protocolList=None, extended=False, maximum=1):
        assert location is None or (country is not None and location is not None)
        assert protoList is None or all(x in ["http", "ftp", "rsync"] for x in protoList)

        # country out of scope, we don't consider this condition
        if country is not None:
            if not any(x.get("country", None) == country for x in srcDict.values()):
                country = None
                location = None

        # location out of scope, same as above
        if location is not None:
            if not any(x["country"] == country and x.get("location", None) == location for x in srcDict.values()):
                location = None

        # select database
        srcDict = Db.dictOfficial if not extended else Db.dictExteneded

        # do query
        ret = []
        for url, prop in srcDict.items():
            if len(ret) >= maximum:
                break
            if country is not None and prop.get("country", None) != country:
                continue
            if location is not None and prop.get("location", None) != location:
                continue
            if protocol is not None and prop.get("protocol", None) not in protoList:
                continue
            ret.append(url)
        return ret


class PortageDb:

    def __init__(self):
        self.db = Db()

    def get(self, extended=False):
        srcDict = self.db.get(extended)
        ret = dict()
        for url, prop in srcDict.items():
            if prop["protocol"] != "rsync":
                continue
            url = self._convertUrl(url)
            if url is None:
                continue
            ret[url] = prop
        return ret

    def query(self, country=None, location=None, protocolList=None, extended=False, maximum=1):
        if protocolList is None or protocolList == []:
            protocolList = ["rsync"]
        if "rsync" not in protocolList:
            return []

        srcList = self.db.query(country, location, protocolList, extended, maximum)
        ret = []
        for url in srcList:
            url = self._convertUrl(url)
            if url is not None:
                ret.append(url)
        return ret

    def _convertUrl(self. url):
        url = url.rstrip("/")
        if not url.endswith("/gentoo"):
            return None
        url += "-portage"
        return url


class Updater:

    def __init__(self, param, api):
        self.param = param
        self.api = api
        self.proc = None
        self.thread = None

    def start(self):
        source = Db().query(self.api.get_country(), self.api.get_location(), ["rsync"], True)[0]
        dataDir = self.api.get_data_dir()
        logFile = os.path.join(self.api.get_log_dir(), "rsync-%s.log" % (self.api.get_sched_datetime()))
        cmd = "/usr/bin/rsync -q -a --delete \"%s\" \"%s\" >\"%s\" 2>&1" % (source, dataDir, logFile)
        self.proc = _Util.shellProc(cmd, self._finishCallback)
        self.api.notify_progress(1)

    def stop(self):
        self.proc.terminate()

    def _finishCallback(self, proc):
        self.api.notify_progress(100)


class PortageUpdater:

    def __init__(self, param, api):
        self.param = param
        self.api = api
        self.proc = None
        self.thread = None

    def start(self):
        source = Db().query(self.api.get_country(), self.api.get_location(), ["rsync"], True)[0]
        dataDir = self.api.get_data_dir()
        logFile = os.path.join(self.api.get_log_dir(), "rsync-%s.log" % (self.api.get_sched_datetime()))
        cmd = "/usr/bin/rsync -q -a --delete \"%s\" \"%s\" >\"%s\" 2>&1" % (source, dataDir, logFile)
        self.proc = _Util.shellProc(cmd, self._finishCallback)
        self.api.notify_progress(1)

    def stop(self):
        self.proc.terminate()

    def _finishCallback(self, proc):
        self.api.notify_progress(100)


class _Util:

    @staticmethod
    def shellProc(cmd, finishCallback):
        proc = subprocess.Popen(cmd, shell=True, universal_newlines=True)
        return proc
