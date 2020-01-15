#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import logging
import subprocess


class McRsyncServer:

    def __init__(self, mainloop, ip, port, tmpDir, logDir):
        assert 0 < port < 65536
        self._mainloop = mainloop
        self._ip = ip
        self._port = port
        self._dirDict = dict()
        self.rsyncdCfgFile = os.path.join(tmpDir, "rsyncd.conf")
        self.rsyncdLockFile = os.path.join(tmpDir, "rsyncd.lock")
        self.rsyncdLogFile = os.path.join(logDir, "rsyncd.log")
        self._proc = None

    @property
    def portStandard(self):
        return 873

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    @property
    def running(self):
        return self._proc is not None

    def addFileDir(self, name, realPath):
        self._dirDict[name] = realPath
        self._generateCfgFile()             # rsync picks the new cfg-file when new connection comes in

    def start(self):
        assert self._proc is None
        self._mainloop.call_soon(self.start)

    def stop(self):
        pass

    def _start(self):
        self._generateCfgFile()
        cmd = "/usr/bin/rsync --daemon --no-detach --config=\"%s\"" % (self.rsyncdCfgFile)
        self._proc = subprocess.Popen(cmd, shell=True, universal_newlines=True)
        logging.info("Advertising server (rsync) started, listening on port %d." % (self._port))

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
