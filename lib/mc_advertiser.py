#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import subprocess
from mc_util import McUtil
from mc_util import HttpFileServer
from mc_util import FtpServer
from mc_util import RsyncServer
from mc_param import McConst


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
            self.httpServer = AioHttpFileServer(self.param.listenIp, self.param.httpPort, list(self.httpDirDict.values()), McConst.logDir)
            self.param.mainloop.call_soon(self.httpServer.start())

        self.ftpServer = None
        if len(self.ftpDirDict) > 0:
            if self.param.ftpPort == "random":
                self.param.ftpPort = McUtil.getFreeSocketPort("tcp")
            self.ftpServer = AioFtpServer(self.param.listenIp, self.param.ftpPort, list(self.ftpDirDict.values()), McConst.logDir)
            self.param.mainloop.call_soon(self.ftpServer.start())

        self.rsyncServer = None
        if len(self.rsyncDirDict) > 0:
            if self.param.rsyncPort == "random":
                self.param.rsyncPort = McUtil.getFreeSocketPort("tcp")
            self.rsyncServer = RsyncServer(self.param.listenIp, self.param.rsyncPort, list(self.rsyncDirDict.values()), McConst.tmpDir, McConst.logDir)
            self.param.mainloop.call_soon(self.rsyncServer.start())

    def dispose(self):
        if self.httpServer is not None:
            self.param.mainloop.run_until_complete(self.httpServer.stop())
            self.httpServer = None
        if self.ftpServer is not None:
            self.param.mainloop.run_until_complete(self.ftpServer.stop())
            self.ftpServer = None
        if self.rsyncServer is not None:
            self.param.mainloop.run_until_complete(self.rsyncServer.stop())
            self.rsyncServer = None


class AioHttpServer:

    def __init__(self, mainloop, ip, port, logDir):
        assert 0 < port < 65536

        self._mainloop = mainloop
        self._ip = ip
        self._port = port
        self._dirDict = dict()
        self._logDir = logDir

        self._runner = None

    @property
    def port(self):
        return self._port

    @property
    def running(self):
        return self._runner is None

    def addFileDir(self, dirname, realPath):
        self._dirDict[dirname] = realPath

    def removeFileDir(self, dirname):
        del self._dirDict[dirname]

    async def start(self):
        app = web.Application(loop=self._mainloop)
        for dirname, realPath in self._dirDict.items():
            app.router.add_static("/" + dirname + "/", realPath, name=dirname, show_index=True, follow_symlinks=True)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._ip, self._port)
        await site.start()

    async def stop(self):
        await self._runner.cleanup()
