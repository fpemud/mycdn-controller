#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

from mc_util import HttpFileServer
from mc_util import FtpServer


class McAdvertiser:

    def __init__(self, param):
        self.param = param

        self.httpDirDict = dict()       # dict<mirror-site-id,data-dir>
        self.ftpDirDict = dict()        # dict<mirror-site-id,data-dir>
        for ms in self.param.getMirrorSiteList():
            for proto in ms.advertiseProtocolList:
                if proto == "http":
                    self.httpDirDict[ms.id] = ms.dataDir
                elif proto == "ftp":
                    self.ftpDirDict[ms.id] = ms.dataDir
                else:
                    assert False

        self.httpServer = None
        if len(self.httpDirDict) > 0:
            self.httpServer = HttpFileServer("0.0.0.0", 2299, self.httpDirDict.values(), "/dev/null")

        self.ftpServer = None
        if len(self.ftpDirDict) > 0:
            self.ftpServer = FtpServer("0.0.0.0", 2300, self.ftpDirdict.values(), "/dev/null")

    def dispose(self):
        if self.httpServer is not None:
            self.httpServer.stop()
            self.httpServer = None
        if self.ftpServer is not None:
            self.ftpServer.stop()
            self.ftpServer = None
