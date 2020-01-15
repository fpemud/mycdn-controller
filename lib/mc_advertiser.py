#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

from mc_util import McUtil
from mc_param import McConst
from mc_advertiser_http import McHttpServer
from mc_advertiser_ftp import McFtpServer
from mc_advertiser_rsync import McRsyncServer


class McAdvertiser:

    def __init__(self, param):
        self.param = param

        bHasHttpMirrorSite = False
        bHasFtpMirrorSite = False
        bHasRsyncMirrorSite = False
        bHasGitHttpMirrorSite = False
        for ms in self.param.mirrorSiteDict.values():
            for proto in ms.advertiseProtocolList:
                if proto == "http":
                    bHasHttpMirrorSite = True
                elif proto == "ftp":
                    bHasFtpMirrorSite = True
                elif proto == "rsync":
                    bHasRsyncMirrorSite = True
                elif proto == "git-http":
                    bHasGitHttpMirrorSite = True
                else:
                    assert False

        self.httpServer = None
        self.ftpServer = None
        self.rsyncServer = None
        if bHasHttpMirrorSite or bHasGitHttpMirrorSite:
            if self.param.httpPort == "random":
                self.param.httpPort = McUtil.getFreeSocketPort("tcp")
            self.httpServer = McHttpServer(self.param, self.param.mainloop, self.param.listenIp, self.param.httpPort, McConst.logDir)
            self.httpServer.start()
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
            self.httpServer.addFileDir(msObj.id, msObj.dataDir)
        if "ftp" in msObj.advertiseProtocolList:
            self.ftpServer.addFileDir(msObj.id, msObj.dataDir)
        if "rsync" in msObj.advertiseProtocolList:
            self.rsyncServer.addFileDir(msObj.id, msObj.dataDir)
        if "git-http" in msObj.advertiseProtocolList:
            # http server checks mirror status on the fly
            pass

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
