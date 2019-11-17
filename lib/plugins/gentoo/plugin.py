#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import copy
import json
from gi.repository import GLib


class Db:

    dictOfficial = None
    dictExtended = None

    def __init__(self):
        selfDir = os.path.dirname(os.path.realpath(__file__))

        if Db.dictOfficial is None:
            with open(os.path.join(selfDir, "db-official.json")) as f:
                Db.dictOfficial = json.load(f)

        if Db.dictExtended is None:
            Db.dictExtended = copy.deepcopy(Db.dictOfficial)
            with open(os.path.join(selfDir, "db-extended.json")) as f:
                Db.dictExtended.update(json.load(f))

    def get(self, extended=False):
        if not extended:
            return Db.dictOfficial
        else:
            return Db.dictExtended

    def query(self, country=None, location=None, protocolList=None, extended=False, maximum=1):
        assert location is None or (country is not None and location is not None)
        assert protocolList is None or all(x in ["http", "ftp", "rsync"] for x in protocolList)

        # select database
        srcDict = Db.dictOfficial if not extended else Db.dictExtended

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


class PortageDb:

    dictOfficial = None
    dictExtended = None

    def __init__(self):
        selfDir = os.path.dirname(os.path.realpath(__file__))

        if PortageDb.dictOfficial is None:
            with open(os.path.join(selfDir, "db-official.json")) as f:
                PortageDb.dictOfficial = self._convertDict(json.load(f))

        if PortageDb.dictExtended is None:
            PortageDb.dictExtended = copy.deepcopy(PortageDb.dictOfficial)
            with open(os.path.join(selfDir, "db-extended.json")) as f:
                PortageDb.dictExtended.update(self._convertDict(json.load(f)))

    def get(self, extended=False):
        if not extended:
            return PortageDb.dictOfficial
        else:
            return PortageDb.dictExtended

    def query(self, country=None, location=None, protocolList=None, extended=False, maximum=1):
        assert location is None or (country is not None and location is not None)
        assert protocolList is None or protocolList == ["rsync"]

        # select database
        srcDict = PortageDb.dictOfficial if not extended else PortageDb.dictExtended

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
            ret.append(url)
        return ret

    def _convertDict(self, srcDict):
        ret = dict()
        for url, prop in srcDict.items():
            if prop["protocol"] != "rsync":
                continue
            url = self._convertUrl(url)
            if url is None:
                continue
            ret[url] = prop
        return ret

    def _convertUrl(self, srcUrl):
        url = srcUrl.rstrip("/")
        if not url.endswith("/gentoo"):
            return None
        url += "-portage"
        return url


class PeriodicalUpdater:

    def __init__(self, api):
        self.api = api
        self.proc = None

    def start(self, schedDatetime):
        source = Db().query(self.api.get_country(), self.api.get_location(), ["rsync"], True)[0]
        dataDir = self.api.get_data_dir()
        logFile = os.path.join(self.api.get_log_dir(), "rsync-%s.log" % (schedDatetime))
        cmd = "/usr/bin/rsync -q -a --delete \"%s\" \"%s\" >\"%s\" 2>&1" % (source, dataDir, logFile)
        self.proc = _ShellProc(cmd, self._finishCallback)

    def stop(self):
        self.proc.terminate()

    def _finishCallback(self, proc):
        self.api.notify_progress(100)


class PortagePeriodicalUpdater:

    def __init__(self, api):
        self.api = api
        self.proc = None

    def start(self, schedDatetime):
        source = PortageDb().query(self.api.get_country(), self.api.get_location(), ["rsync"], True)[0]
        dataDir = self.api.get_data_dir()
        logFile = os.path.join(self.api.get_log_dir(), "rsync-%s.log" % (schedDatetime))
        cmd = "/usr/bin/rsync -q -a --delete \"%s\" \"%s\" >\"%s\" 2>&1" % (source, dataDir, logFile)
        self.proc = _ShellProc(cmd, self._finishCallback)

    def stop(self):
        self.proc.terminate()

    def _finishCallback(self):
        self.api.notify_progress(100, True)


class _ShellProc:

    def __init__(self, cmd, exitCallback):
        targc, targv = GLib.shell_parse_argv(cmd)
        self.exitCallback = exitCallback
        self.pid = GLib.spawn_async(targv, flags=GLib.SpawnFlags.DO_NOT_REAP_CHILD)[0]
        self.pidWatch = GLib.child_watch_add(self.pid, self._exitCallback)

    def terminate(self):
        # FIXME
        pass

    def _exitCallback(self, dummy1, dummy2):        # FIXME
        self.exitCallback()
        GLib.source_remove(self.pidWatch)
        self.pidWatch = None
        os.waitpid(self.pid, 0)
        GLib.spawn_close_pid(self.pid)
        self.pid = None
