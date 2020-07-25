#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import signal
import shutil
import socket
import logging
import asyncio
import asyncio_glib
from mc_util import McUtil
from mc_util import StdoutRedirector
from mc_util import AvahiServiceRegister
from mc_server_http import McHttpServer
from mc_server_ftp import McFtpServer
from mc_server_rsync import McRsyncServer
from mc_param import McConst
from mc_plugin import McPluginManager
from mc_updater import McMirrorSiteUpdater
from mc_advertiser import McAdvertiser


class McDaemon:

    def __init__(self, param):
        self.param = param

    def run(self):
        try:
            # create directories
            McUtil.preparePersistDir(McConst.stateDir, McConst.uid, McConst.gid, McConst.stateDirMode)
            McUtil.preparePersistDir(McConst.cacheDir, McConst.uid, McConst.gid, McConst.cacheDirMode)
            McUtil.preparePersistDir(McConst.logDir, McConst.uid, McConst.gid, McConst.logDirMode)
            McUtil.prepareTransientDir(McConst.runDir, McConst.uid, McConst.gid, McConst.runDirMode)
            McUtil.prepareTransientDir(McConst.tmpDir, McConst.uid, McConst.gid, McConst.tmpDirMode)

            # drop priviledge
            McUtil.dropPriviledge(McConst.uid, McConst.gid)

            # initialize logging
            sys.stdout = StdoutRedirector(os.path.join(McConst.logDir, "mirrors.out"))
            sys.stderr = sys.stdout

            logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
            logging.getLogger().setLevel(logging.INFO)
            logging.info("Program begins.")

            # create mainloop
            asyncio.set_event_loop_policy(asyncio_glib.GLibEventLoopPolicy())
            self.param.mainloop = asyncio.get_event_loop()

            # write pid file
            McUtil.writePidFile(McConst.pidFile)

            # load plugins
            self.pluginManager = McPluginManager(self.param)
            self.pluginManager.loadPlugins()
            logging.info("Plugins loaded: %s" % (",".join(self.param.pluginList)))

            # start servers
            self.param.httpServer = McHttpServer("Advertising Server (http)", self.param.mainloop, self.param.listenIp, self.param.httpPort, McConst.logDir)
            self.param.ftpServer = McFtpServer("Advertising Server (ftp)", self.param.mainloop, self.param.listenIp, self.param.ftpPort, McConst.logDir)
            self.param.rsyncServer = McRsyncServer("Advertising Server (rsync)", self.param.mainloop, self.param.listenIp, self.param.rsyncPort, McConst.tmpDir, McConst.logDir)   # FIXME
            if True:
                self.param.httpServer.useBy("advertiser")
                for ms in self.param.mirrorSiteDict.values():
                    for storageName, protocolList in ms.advertiseDict.items():
                        for proto in protocolList:
                            if proto == "http":
                                self.param.httpServer.useBy(ms.id)
                            elif proto == "ftp":
                                self.param.ftpServer.useBy(ms.id)
                            elif proto == "rsync":
                                self.param.rsyncServer.useBy(ms.id)
                            elif proto == "git-http":
                                self.param.httpServer.useBy(ms.id)
                            else:
                                assert False
            self.param.httpServer.start()
            self.param.ftpServer.start()
            self.param.rsyncServer.start()

            # advertiser
            self.param.advertiser = McAdvertiser(self.param)
            logging.info("Advertiser initialized.")

            # updater
            self.param.updater = McMirrorSiteUpdater(self.param)
            logging.info("Mirror site updater initialized.")

            # register serivce
            if McConst.avahiSupport:
                self.param.avahiObj = AvahiServiceRegister()
                self.param.avahiObj.add_service(socket.gethostname(), "_mirrors._tcp", self.param.httpPort)
                self.param.avahiObj.start()

            # start main loop
            logging.info("Mainloop begins.")
            self.param.mainloop.add_signal_handler(signal.SIGINT, self._sigHandlerINT)
            self.param.mainloop.add_signal_handler(signal.SIGTERM, self._sigHandlerTERM)
            self.param.mainloop.run_forever()
            logging.info("Mainloop exits.")
        finally:
            if self.param.avahiObj is not None:
                self.param.avahiObj.stop()
            if self.param.updater is not None:
                self.param.updater.dispose()
            if self.param.advertiser is not None:
                self.param.advertiser.dispose()
            if self.param.httpServer is not None:
                self.param.httpServer.stop()
                self.param.httpServer = None
            if self.param.ftpServer is not None:
                self.param.ftpServer.stop()
                self.param.ftpServer = None
            if self.param.rsyncServer is not None:
                self.param.rsyncServer.stop()
                self.param.rsyncServer = None
            logging.shutdown()
            shutil.rmtree(McConst.tmpDir)
            shutil.rmtree(McConst.runDir)

    def _sigHandlerINT(self):
        logging.info("SIGINT received.")
        self.param.mainloop.stop()
        return True

    def _sigHandlerTERM(self):
        logging.info("SIGTERM received.")
        self.param.mainloop.stop()
        return True
