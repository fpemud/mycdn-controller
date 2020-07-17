#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

from mc_util import McUtil
from mc_param import McConst
from mc_server_http import McHttpServer
from mc_server_ftp import McFtpServer
from mc_server_rsync import McRsyncServer


class McAdvertiser:

    def __init__(self, param):
        self.param = param

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
        pass
