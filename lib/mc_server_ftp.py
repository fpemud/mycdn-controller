#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import json
import signal
import logging
import subprocess
from mc_util import McUtil


class McFtpServer:

    def __init__(self, serverName, ip, port, logDir):
        assert port == "random" or 0 < port < 65536

        self._serverName = serverName
        self._ip = ip
        self._port = port
        self._logDir = logDir

        self._userSet = set()
        self._dirDict = dict()

        self._ftpdExecFile = os.path.join(self.libexecDir, "ftpd.py")
        self._ftpdCfgFile = os.path.join(self.tmpDir, "mirrors-ftpd.cfg")
        self._bStart = False
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def useBy(self, user):
        assert not self._bStart
        self._userSet.add(user)

    def start(self):
        assert not self._bStart
        self._bStart = True
        try:
            if len(self._userSet) > 0:
                if self._port == "random":
                    self._port = McUtil.getFreeSocketPort("tcp")
                self._generateCfgFile()
                self._proc = subprocess.Popen([self._ftpdExecFile, self._ftpdCfgFile])
                logging.info("%s started, listening on port %d." % (self._serverName, self._port))
        except Exception:
            self._bStart = False
            raise

    def stop(self):
        assert self._bStart
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        self._bStart = False

    def isStarted(self):
        return self._bStart

    def isRunning(self):
        assert self._bStart
        return self._proc is not None

    def addFileDir(self, name, realPath):
        assert self._proc is not None
        self._dirDict[name] = realPath
        self._generateCfgFile()
        os.kill(self._proc.pid, signal.SIGHUP)

    def _generateCfgFile(self):
        dataObj = dict()
        dataObj["ip"] = self._ip
        dataObj["port"] = self._port
        dataObj["dirmap"] = self._dirDict
        with open(self._ftpdCfgFile, "w") as f:
            json.dump(dataObj, f)
