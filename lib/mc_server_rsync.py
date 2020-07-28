#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import logging
import subprocess
from mc_util import McUtil


class McRsyncServer:

    def __init__(self, serverName, mainloop, ip, port, tmpDir, logDir):
        assert port == "random" or 0 < port < 65536

        self._serverName = serverName
        self._mainloop = mainloop
        self._ip = ip
        self._port = port

        self._userSet = set()
        self._dirDict = dict()

        self._bStart = False
        self.rsyncdCfgFile = os.path.join(tmpDir, "rsyncd.conf")
        self.rsyncdLockFile = os.path.join(tmpDir, "rsyncd.lock")
        self.rsyncdLogFile = os.path.join(logDir, "rsyncd.log")
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
                cmd = "/usr/bin/rsync --daemon --no-detach --config=\"%s\"" % (self.rsyncdCfgFile)
                self._proc = subprocess.Popen(cmd, shell=True, universal_newlines=True)
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
        self._generateCfgFile()             # rsync picks the new cfg-file when new connection comes in

    def _generateCfgFile(self):
        buf = ""
        buf += "lock file = %s\n" % (self.rsyncdLockFile)
        buf += "log file = %s\n" % (self.rsyncdLogFile)
        buf += "\n"
        buf += "port = %s\n" % (self._port)
        buf += "timeout = 600\n"
        buf += "\n"
        buf += "use chroot = no\n"      # we are not running rsyncd using the root user
        buf += "\n"
        for name, d in self._dirDict.items():
            buf += "[%s]\n" % (name)
            buf += "path = %s\n" % (d)
            buf += "read only = yes\n"
            buf += "\n"
        with open(self.rsyncdCfgFile, "w") as f:
            f.write(buf)
