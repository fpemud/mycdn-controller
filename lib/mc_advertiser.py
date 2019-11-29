#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

from mc_util import McUtil
from mc_util import HttpFileServer
from mc_util import FtpServer
from mc_util import RsyncServer


class McAdvertiser:

    def __init__(self, param):
        self.param = param

        self.httpDirDict = dict()       # dict<mirror-id,data-dir>
        self.ftpDirDict = dict()        # dict<mirror-id,data-dir>
        self.rsyncDirDict = dict()      # dict<mirror-id,data-dir>
        for ms in self.param.mirrorSiteList:
            for proto in ms.advertiseProtocolList:
                if proto == "http":
                    self.httpDirDict[ms.id] = ms.dataDir
                elif proto == "ftp":
                    self.ftpDirDict[ms.id] = ms.dataDir
                elif proto == "rsync":
                    self.rsyncDirDict[ms.id] = ms.dataDir
                else:
                    assert False

        self.httpServer = None
        if len(self.httpDirDict) > 0:
            if self.param.httpPort == "random":
                self.param.httpPort = McUtil.getFreeSocketPort("tcp")
            self.httpServer = HttpFileServer(self.param.listenIp, self.param.httpPort, list(self.httpDirDict.values()), self.param.logDir)
            self.httpServer.start()

        self.ftpServer = None
        if len(self.ftpDirDict) > 0:
            if self.param.ftpPort == "random":
                self.param.ftpPort = McUtil.getFreeSocketPort("tcp")
            self.ftpServer = FtpServer(self.param.listenIp, self.param.ftpPort, list(self.ftpDirDict.values()), self.param.logDir)
            self.ftpServer.start()

        self.rsyncServer = None
        if len(self.rsyncDirDict) > 0:
            if self.param.rsyncPort == "random":
                self.param.rsyncPort = McUtil.getFreeSocketPort("tcp")
            self.rsyncServer = RsyncServer(self.param.listenIp, self.param.rsyncPort, list(self.rsyncDirDict.values()), self.param.tmpDir, self.param.logDir)
            self.rsyncServer.start()

    def dispose(self):
        if self.httpServer is not None:
            self.httpServer.stop()
            self.httpServer = None
        if self.ftpServer is not None:
            self.ftpServer.stop()
            self.ftpServer = None
        if self.rsyncServer is not None:
            self.rsyncServer.stop()
            self.rsyncServer = None


class HttpServer:

    def __init__(self, ip, port, logDir):
        assert 0 < port < 65536
        self._ip = ip
        self._port = port
        self._dirDict = dict()
        self._logDir = logDir

    @property
    def port(self):
        return self._port

    @property
    def running(self):
        assert False

    def start(self):
        pass

    def stop(self):
        pass

    def addFileDir(self, dirname, realPath):
        port = McUtil.getFreeSocketPort("tcp")
        logfile = os.path.join(self._logDir, "httpd-%d.log" % (port))
        cmd = "/usr/bin/bozohttpd -b -f -H -I %d -s -X %s 2>%s" % (port, realPath, logfile)
        proc = subprocess.Popen(cmd, shell=True, universal_newlines=True)
        self._dirDict[dirname] = (realPath, port, proc)

    def removeFileDir(self, dirname):
        realPath, port, proc = self._dirDict[dirname]
        proc.terminate()
        proc.wait()
        del self._dirDict[dirname]
