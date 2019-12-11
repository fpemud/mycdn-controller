#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import copy
import json
from gi.repository import GLib


class Database:

    def get_data(self):
        selfDir = os.path.dirname(os.path.realpath(__file__))

        dictOfficial = dict()

        dictExtended = copy.deepcopy(dictOfficial)
        with open(os.path.join(selfDir, "db-extended.json")) as f:
            dictExtended.update(json.load(f))

        return (dictOfficial, dictExtended)


class Updater:

    def init_start(self, api):
        self._api = api
        self._start()

    def init_stop(self):
        self._proc.terminate()

    def update_start(self, api):
        self._api = api
        self._start()

    def update_stop(self):
        self._proc.terminate()

    def _start(self):
        source = self._db.query(self._api.get_country(), self._api.get_location(), ["http"], True)[0]
        dataDir = self._api.get_data_dir()
        logFile = os.path.join(self._api.get_log_dir(), "rsync.log")
        cmd = "/usr/bin/rsync -a -z --delete %s %s >%s 2>&1" % (source, dataDir, logFile)
        self._proc = _ShellProc(cmd, self._finishCallback, self._errorCallback)

    def _finishCallback(self):
        self._api.progress_changed(100)
        del self._proc
        del self._api

    def _errorCallback(self, exc_info):
        self._api.error_occured(exc_info)
        del self._proc
        del self._api


class _ShellProc:

    def __init__(self, cmd, finishCallback, errorCallback):
        targc, targv = GLib.shell_parse_argv("/bin/sh -c \"%s\"" % (cmd))
        self.pid = GLib.spawn_async(targv, flags=GLib.SpawnFlags.DO_NOT_REAP_CHILD)[0]
        self.finishCallback = finishCallback
        self.errorCallback = errorCallback
        self.pidWatch = GLib.child_watch_add(self.pid, self._exitCallback)

    def terminate(self):
        # FIXME
        pass

    def _exitCallback(self, status, data):
        try:
            GLib.spawn_check_exit_status(status)
            self.finishCallback()
        except:
            self.errorCallback(sys.exc_info())
        finally:
            GLib.source_remove(self.pidWatch)
            self.pidWatch = None
            GLib.spawn_close_pid(self.pid)
            self.pid = None
