#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import aioftp
import logging
import subprocess
import aiohttp.web
from mc_util import McUtil
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
            self.param.mainloop.create_task(self.httpServer.start())

        self.ftpServer = None
        if len(self.ftpMirrorSiteList) > 0:
            if self.param.ftpPort == "random":
                self.param.ftpPort = McUtil.getFreeSocketPort("tcp")
            self.ftpServer = _FtpServer(self.param.mainloop, self.param.listenIp, self.param.ftpPort, McConst.logDir)
            for msId in self.ftpMirrorSiteList:
                # if self.param.updater.isMirrorSiteInitialized(msId):
                if True:
                    self.ftpServer.addFileDir(msId, self.param.mirrorSiteDict[msId].dataDir)
            self.param.mainloop.create_task(self.ftpServer.start())

        self.rsyncServer = None
        if len(self.rsyncMirrorSiteList) > 0:
            if self.param.rsyncPort == "random":
                self.param.rsyncPort = McUtil.getFreeSocketPort("tcp")
            self.rsyncServer = _RsyncServer(self.param.listenIp, self.param.rsyncPort, [], McConst.tmpDir, McConst.logDir)   # FIXME
            self.param.mainloop.call_soon(self.rsyncServer.start)

    def dispose(self):
        if self.httpServer is not None:
            self.param.mainloop.run_until_complete(self.httpServer.stop())
            self.httpServer = None
        if self.ftpServer is not None:
            self.param.mainloop.run_until_complete(self.ftpServer.stop())
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

    async def start(self):
        self._runner = aiohttp.web.AppRunner(self._app)
        await self._runner.setup()
        site = aiohttp.web.TCPSite(self._runner, self._ip, self._port)
        await site.start()
        logging.info("Advertising server (HTTP) started, listening on port %d." % (self._port))

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
        assert self._bStart
        return self._port

    @property
    def running(self):
        return self._bStart

    def addFileDir(self, name, realPath):
        self._dirDict[name] = realPath

    async def start(self):
        await self._server.start(self._ip, self._port)
        self._bStart = True
        logging.info("Advertising server (FTP) started, listening on port %d." % (self._port))

    async def stop(self):
        # it seems aioftp.Server.close() has syntax error
        # await self._server.close()
        self._bStart = False


class _RsyncServer:

    def __init__(self, ip, port, dirList, tmpDir, logDir):
        assert 0 < port < 65536
        self._ip = ip
        self._port = port
        self._dirlist = dirList
        self.rsyncdCfgFile = os.path.join(tmpDir, "rsyncd.conf")
        self.rsyncdLockFile = os.path.join(tmpDir, "rsyncd.lock")
        self.rsyncdLogFile = os.path.join(logDir, "rsyncd.log")
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    @property
    def running(self):
        return self._proc is not None

    def start(self):
        assert self._proc is None

        buf = ""
        buf += "lock file = %s\n" % (self.rsyncdLockFile)
        buf += "log file = %s\n" % (self.rsyncdLogFile)
        buf += "\n"
        buf += "port = %s\n" % (self._port)
        buf += "max connections = 1\n"
        buf += "timeout = 600\n"
        buf += "hosts allow = 127.0.0.1\n"
        buf += "\n"
        buf += "use chroot = yes\n"
        buf += "uid = root\n"
        buf += "gid = root\n"
        buf += "\n"
        for d in self._dirlist:
            buf += "[%s]\n" % (os.path.basename(d))
            buf += "path = %s\n" % (d)
            buf += "read only = yes\n"
            buf += "\n"
        with open(self.rsyncdCfgFile, "w") as f:
            f.write(buf)

        cmd = ""
        cmd += "/usr/bin/rsync --daemon --no-detach --config=\"%s\"" % (self.rsyncdCfgFile)
        self._proc = subprocess.Popen(cmd, shell=True, universal_newlines=True)

        logging.info("Advertising server (rsync) started, listening on port %d." % (self._port))

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
        self.soupServer.add_handler (None, server_callback, None, None)

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

