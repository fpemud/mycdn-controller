#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import logging
import aiohttp.web
from mc_util import McUtil
from mc_param import McConst
from mc_advertiser_ftp import McFtpServer
from mc_advertiser_rsync import McRsyncServer


class McAdvertiser:

    def __init__(self, param):
        self.param = param

        bHasHttpMirrorSite = False
        bHasFtpMirrorSite = False
        bHasRsyncMirrorSite = False
        for ms in self.param.mirrorSiteDict.values():
            for proto in ms.advertiseProtocolList:
                if proto == "http":
                    bHasHttpMirrorSite = True
                elif proto == "ftp":
                    bHasFtpMirrorSite = True
                elif proto == "rsync":
                    bHasRsyncMirrorSite = True
                elif proto == "git-http":
                    pass    # FIXME
                else:
                    assert False

        self.httpServer = None
        self.ftpServer = None
        self.rsyncServer = None
        # if len(self.httpMirrorSiteList) > 0:
        #     if self.param.httpPort == "random":
        #         self.param.httpPort = McUtil.getFreeSocketPort("tcp")
        #     self.httpServer = _HttpServer(self.param.mainloop, self.param.listenIp, self.param.httpPort, McConst.logDir)
        #     for msId in self.httpMirrorSiteList:
        #         # if self.param.updater.isMirrorSiteInitialized(msId):
        #         if True:
        #             self.httpServer.addFileDir(msId, self.param.mirrorSiteDict[msId].dataDir)
        #     self.param.mainloop.create_task(self.httpServer.start())
        if bHasFtpMirrorSite:
            if self.param.ftpPort == "random":
                self.param.ftpPort = McUtil.getFreeSocketPort("tcp")
            self.ftpServer = McFtpServer(self.param.mainloop, self.param.listenIp, self.param.ftpPort, McConst.logDir)
            self.ftpServer.start()
        if bHasRsyncMirrorSite:
            if self.param.rsyncPort == "random":
                self.param.rsyncPort = McUtil.getFreeSocketPort("tcp")
            self.rsyncServer = McRsyncServer(self.param.mainloop, self.param.listenIp, self.param.rsyncPort, McConst.tmpDir, McConst.logDir)   # FIXME
            self.rsyncServer.start()

    def advertiseMirrorSite(self, mirrorSiteId):
        msObj = self.param.mirrorSiteDict[mirrorSiteId]
        if "http" in msObj.advertiseProtocolList:
            # FIXME
            pass
            # self.httpServer.addFileDir(msObj.id, msObj.dataDir)
        if "ftp" in msObj.advertiseProtocolList:
            self.ftpServer.addFileDir(msObj.id, msObj.dataDir)
        if "rsync" in msObj.advertiseProtocolList:
            self.rsyncServer.addFileDir(msObj.id, msObj.dataDir)
        if "git-http" in msObj.advertiseProtocolList:
            # FIXME
            pass

    def dispose(self):
        if self.httpServer is not None:
            self.param.mainloop.run_until_complete(self.httpServer.stop())
            self.httpServer = None
        if self.ftpServer is not None:
            self.ftpServer.stop()
            self.ftpServer = None
        if self.rsyncServer is not None:
            self.rsyncServer.stop()
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
        assert self._runner is not None
        return self._port

    @property
    def running(self):
        return self._runner is None

    def addFileDir(self, name, realPath):
        self._dirDict[name] = realPath
        self._app.router.add_static("/" + name + "/", realPath, name=name, show_index=True, follow_symlinks=True)

    def start(self, mainloop):
        mainloop.create_task(self._start())

    def stop(self, mainloop):
        mainloop.run_until_complete(self._stop())

    async def _start(self):
        self._runner = aiohttp.web.AppRunner(self._app)
        await self._runner.setup()
        site = aiohttp.web.TCPSite(self._runner, self._ip, self._port)
        await site.start()
        logging.info("Advertising server (HTTP) started, listening on port %d." % (self._port))

    async def _stop(self):
        await self._runner.cleanup()


class HttpServer2:

    def __init__(self, param):
        self.param = param

    @property
    def port(self):
        return self._port

    @property
    def running(self):
        return False

    def start(self):
        assert self.soupServer is None
        self.soupServer = SoupServer()
        self.soupServer.listen_all()
        self.soupServer.add_handler(None, server_callback, None, None)

        self.jinaEnv = jinja2.Environment(loader=jinja2.FileSystemLoader(self.param.shareDir),
                                          autoescape=select_autoescape(['html', 'xml']))

    def stop(self):
        assert self._proc is not None

    def _callback(self):
        pass

    def _generateHomePage(self):
        template = self.jinaEnv.get_template('index.html')

        env = None
        template = jinja2.Template('Hello {{ name }}!')
        template.render(name='John Doe')
