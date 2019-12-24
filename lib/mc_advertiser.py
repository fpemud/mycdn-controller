#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import aioftp
import aiohttp.web
from mc_util import McUtil
from mc_util import RsyncServer
from mc_param import McConst


class McAdvertiser:

    def __init__(self, param):
        self.param = param

        self.httpMirrorSiteList = []
        self.ftpMirrorSiteList = []
        self.rsyncMirrorSiteList = []
        for ms in self.param.mirrorSiteDict.values():
            for proto in ms.advertiseProtocolList:
                if proto == "http":
                    self.httpMirrorSiteList.append(ms.id)
                elif proto == "ftp":
                    self.ftpMirrorSiteList.append(ms.id)
                elif proto == "rsync":
                    self.rsyncMirrorSiteList.append(ms.id)
                else:
                    assert False

        self.httpServer = None
        if len(self.httpMirrorSiteList) > 0:
            if self.param.httpPort == "random":
                self.param.httpPort = McUtil.getFreeSocketPort("tcp")
            self.httpServer = _HttpServer(self.param.mainloop, self.param.listenIp, self.param.httpPort, McConst.logDir)
            for msId in self.httpMirrorSiteList:
                # if self.param.updater.isMirrorSiteInitialized(msId):
                if True:
                    self.httpServer.addFileDir(msId, self.param.mirrorSiteDict[msId].dataDir)
            self.param.mainloop.call_soon(self.httpServer.start())

        self.ftpServer = None
        if len(self.ftpMirrorSiteList) > 0:
            if self.param.ftpPort == "random":
                self.param.ftpPort = McUtil.getFreeSocketPort("tcp")
            self.ftpServer = _FtpServer(self.param.mainloop, self.param.listenIp, self.param.ftpPort, McConst.logDir)
            for msId in self.ftpMirrorSiteList:
                # if self.param.updater.isMirrorSiteInitialized(msId):
                if True:
                    self.ftpServer.addFileDir(msId, self.param.mirrorSiteDict[msId].dataDir)
            self.param.mainloop.call_soon(self.ftpServer.start())

        self.rsyncServer = None
        if len(self.rsyncMirrorSiteList) > 0:
            if self.param.rsyncPort == "random":
                self.param.rsyncPort = McUtil.getFreeSocketPort("tcp")
            self.rsyncServer = RsyncServer(self.param.listenIp, self.param.rsyncPort, [], McConst.tmpDir, McConst.logDir)   # FIXME
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


class _HttpServer:

    def __init__(self, mainloop, ip, port, logDir):
        assert 0 < port < 65536

        self._ip = ip
        self._port = port
        self._dirDict = dict()
        self._logDir = logDir

        self._app = aiohttp.web.Application(loop=mainloop)
        self._runner = None

    @property
    def port(self):
        return self._port

    @property
    def running(self):
        return self._runner is None

    def addFileDir(self, name, realPath):
        self._dirDict[name] = realPath
        self._app.router.add_static("/" + name + "/", realPath, name=name, show_index=True, follow_symlinks=True)

    async def start(self):
        self._runner = aiohttp.web.AppRunner(self._app)
        await self._runner.setup()
        site = aiohttp.web.TCPSite(self._runner, self._ip, self._port)
        await site.start()

    async def stop(self):
        await self._runner.cleanup()


class _FtpServer:

    def __init__(self, mainloop, ip, port, logDir):
        assert 0 < port < 65536

        self._mainloop = mainloop
        self._ip = ip
        self._port = port
        self._dirDict = dict()
        self._logDir = logDir

        self._server = aioftp.Server(path_io_factory=self)
        self._bStart = False

    @property
    def port(self):
        return self._port

    @property
    def running(self):
        return self._bStart

    def addFileDir(self, name, realPath):
        self._dirDict[name] = realPath

    async def start(self):
        await self._server.start(self._ip, self._port)
        self._bStart = True

    async def stop(self):
        await self._server.cleanup()
        self._bStart = False

