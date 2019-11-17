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
from mc_param import McConfig
from mc_plugin import McPlugin
from mc_api_server import McApiServer
from mc_updater import McMirrorSiteUpdater
from mc_advertiser import McAdvertiser


class McDaemon:

    def __init__(self, param):
        self.param = param

    def run(self):
        McUtil.ensureDir(self.param.logDir)
        McUtil.mkDirAndClear(self.param.runDir)
        McUtil.mkDirAndClear(self.param.tmpDir)
        try:
            sys.stdout = StdoutRedirector(os.path.join(self.param.tmpDir, "mycdn.out"))
            sys.stderr = sys.stdout

            logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
            logging.getLogger().setLevel(logging.INFO)
            logging.info("Program begins.")

            # create mainloop
            self.param.mainloop = GLib.MainLoop()

            # write pid file
            with open(os.path.join(self.param.runDir, "mycdn.pid"), "w") as f:
                f.write(str(os.getpid()))

            # load config
            self._loadConfig()
            logging.info("Configuration loaded.")

            # load plugins
            self._loadPlugins()
            logging.info("Plugins loaded: %s" % (",".join([x.id for x in self.param.pluginList])))

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
                self.param.avahiObj.add_service(socket.gethostname(), "_mycdn._tcp", self.param.apiPort)
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
                self.param.apiServer.stop()
            if self.param.updater is not None:
                self.param.updater.dispose()
            if self.param.advertiser is not None:
                self.param.advertiser.dispose()
            logging.shutdown()
            shutil.rmtree(self.param.tmpDir)
            shutil.rmtree(self.param.runDir)

    def _sigHandlerINT(self, signum):
        logging.info("SIGINT received.")
        self.param.mainloop.quit()
        return True

    def _sigHandlerTERM(self, signum):
        logging.info("SIGTERM received.")
        self.param.mainloop.quit()
        return True

    def _loadConfig(self):
        # FIXME
        self.param.cfg = McConfig()

    def _loadPlugins(self):
        for fn in os.listdir(self.param.etcDir):
            if not fn.endswith(".conf"):
                continue
            pluginName = fn.replace(".conf", "")
            pluginPath = os.path.join(self.param.pluginsDir, pluginName)
            if not os.path.isdir(pluginPath):
                raise Exception("Invalid configuration file %s" % (fn))
            pluginObj = McPlugin(self.param, pluginName, pluginPath)
            self.param.pluginList.append(pluginObj)
