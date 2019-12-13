#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import signal
import shutil
import socket
import logging
from gi.repository import GLib
from mc_util import McUtil
from mc_util import StdoutRedirector
from mc_util import AvahiServiceRegister
from mc_param import McConst
from mc_plugin import McPluginManager
from mc_api_server import McApiServer
from mc_updater import McMirrorSiteUpdater
from mc_advertiser import McAdvertiser


class McDaemon:

    def __init__(self, param):
        self.param = param

    def run(self):
        McUtil.ensureDir(McConst.logDir)
        McUtil.mkDirAndClear(McConst.runDir)
        McUtil.mkDirAndClear(self.param.tmpDir)
        try:
            sys.stdout = StdoutRedirector(os.path.join(self.param.tmpDir, "mirrors.out"))
            sys.stderr = sys.stdout

            logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
            logging.getLogger().setLevel(logging.INFO)
            logging.info("Program begins.")

            # create mainloop
            self.param.mainloop = GLib.MainLoop()

            # write pid file
            with open(os.path.join(McConst.runDir, "mirrors.pid"), "w") as f:
                f.write(str(os.getpid()))

            # load plugins
            self.pluginManager = McPluginManager(self.param)
            self.pluginManager.loadPlugins()
            logging.info("Plugins loaded: %s" % (",".join(self.param.pluginList)))

            # updater
            self.param.updater = McMirrorSiteUpdater(self.param)
            logging.info("Mirror site updater initialized.")

            # advertiser
            self.param.advertiser = McAdvertiser(self.param)
            logging.info("Advertiser initialized.")
            if self.param.advertiser.httpServer is not None:
                logging.info("   HTTP server enableed, listening on port %d." % (self.param.advertiser.httpServer.port))
            if self.param.advertiser.ftpServer is not None:
                logging.info("   FTP server enableed, listening on port %d." % (self.param.advertiser.ftpServer.port))
            if self.param.advertiser.rsyncServer is not None:
                logging.info("   Rsync server enableed, listening on port %d." % (self.param.advertiser.rsyncServer.port))

            # api server
            self.param.apiServer = McApiServer(self.param)
            logging.info("API server initialized, listening on port %d." % (self.param.apiPort))

            # register serivce
            if self.param.avahiSupport:
                self.param.avahiObj = AvahiServiceRegister()
                self.param.avahiObj.add_service(socket.gethostname(), "_mirrors._tcp", self.param.apiPort)
                self.param.avahiObj.start()

            # start main loop
            logging.info("Mainloop begins.")
            GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGINT, self._sigHandlerINT, None)
            GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGTERM, self._sigHandlerTERM, None)
            self.param.mainloop.run()
            logging.info("Mainloop exits.")
        finally:
            if self.param.avahiObj is not None:
                self.param.avahiObj.stop()
            if self.param.apiServer is not None:
                self.param.apiServer.dispose()
            if self.param.updater is not None:
                self.param.updater.dispose()
            if self.param.advertiser is not None:
                self.param.advertiser.dispose()
            logging.shutdown()
            shutil.rmtree(self.param.tmpDir)
            shutil.rmtree(McConst.runDir)

    def _sigHandlerINT(self, signum):
        logging.info("SIGINT received.")
        self.param.mainloop.quit()
        return True

    def _sigHandlerTERM(self, signum):
        logging.info("SIGTERM received.")
        self.param.mainloop.quit()
        return True
