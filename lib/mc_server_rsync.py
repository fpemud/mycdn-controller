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
        self.rsyncdCfgFile = os.path.join(tmpDir, "rsyncd.conf")
        self.rsyncdLockFile = os.path.join(tmpDir, "rsyncd.lock")
        self.rsyncdLogFile = os.path.join(logDir, "rsyncd.log")
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def useBy(self, user):
        self._userSet.add(user)

    def addFileDir(self, name, realPath):
        self._dirDict[name] = realPath
        self._generateCfgFile()             # rsync picks the new cfg-file when new connection comes in

    def start(self):
        assert self._proc is None
        self._mainloop.call_soon(self._start)

    def stop(self):
        pass

    def isStarted(self):
        return self._proc is not None

    def isRunning(self):
        return self._proc is not None

    def _start(self):
        if self._port == "random":
            self._port = McUtil.getFreeSocketPort("tcp")
        self._generateCfgFile()
        cmd = "/usr/bin/rsync --daemon --no-detach --config=\"%s\"" % (self.rsyncdCfgFile)
        self._proc = subprocess.Popen(cmd, shell=True, universal_newlines=True)
        logging.info("%s started, listening on port %d." % (self._serverName, self._port))

    def _generateCfgFile(self):
        buf = ""
        buf += "lock file = %s\n" % (self.rsyncdLockFile)
        buf += "log file = %s\n" % (self.rsyncdLogFile)
        buf += "\n"
        buf += "port = %s\n" % (self._port)
        buf += "timeout = 600\n"
        buf += "\n"
        buf += "use chroot = yes\n"
        buf += "uid = root\n"           # FIXME
        buf += "gid = root\n"           # FIXME
        buf += "\n"
        for name, d in self._dirDict.items():
            buf += "[%s]\n" % (name)
            buf += "path = %s\n" % (d)
            buf += "read only = yes\n"
            buf += "\n"
        with open(self.rsyncdCfgFile, "w") as f:
            f.write(buf)
