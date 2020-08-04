#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import json
import signal
import shutil
import socket
import logging
import asyncio
import asyncio_glib
from mc_util import McUtil
from mc_util import DropPriviledge
from mc_util import StdoutRedirector
from mc_util import AvahiServiceRegister
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
            McUtil.preparePersistDir(McConst.varDir, McConst.uid, McConst.gid, 0o755)
            McUtil.preparePersistDir(McConst.cacheDir, McConst.uid, McConst.gid, 0o755)
            McUtil.preparePersistDir(McConst.logDir, McConst.uid, McConst.gid, 0o755)
            McUtil.prepareTransientDir(McConst.runDir, McConst.uid, McConst.gid, 0o755)
            McUtil.prepareTransientDir(McConst.tmpDir, McConst.uid, McConst.gid, 0o755)

            with DropPriviledge(McConst.uid, McConst.gid):
                try:
                    # initialize logging
                    sys.stdout = StdoutRedirector(os.path.join(McConst.logDir, "mirrors.out"))
                    sys.stderr = sys.stdout

                    logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
                    logging.getLogger().setLevel(logging.INFO)
                    logging.info("Program begins.")

                    # load main config
                    self._loadMainCfg()

                    # create mainloop
                    asyncio.set_event_loop_policy(asyncio_glib.GLibEventLoopPolicy())
                    self.param.mainloop = asyncio.get_event_loop()

                    # write pid file
                    McUtil.writePidFile(McConst.pidFile)

                    # load plugins
                    self.pluginManager = McPluginManager(self.param)
                    self.pluginManager.loadPlugins()
                    logging.info("Plugins loaded: %s" % (",".join(self.param.pluginList)))

                    # advertiser
                    self.param.advertiser = McAdvertiser(self.param)
                    self.param.advertiser.start()   # this function shows log messages

                    # updater
                    self.param.updater = McMirrorSiteUpdater(self.param)
                    logging.info("Mirror site updater initialized.")

                    # register serivce
                    if McConst.avahiSupport:
                        self.param.avahiObj = AvahiServiceRegister()
                        self.param.avahiObj.add_service(socket.gethostname(), McConst.avahiServiceName, self.param.mainPort)
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
                        self.param.advertiser.stop()
                    logging.shutdown()
        finally:
            shutil.rmtree(McConst.tmpDir)
            shutil.rmtree(McConst.runDir)

    def _loadMainCfg(self):
        if not os.path.exists(McConst.mainCfgFile):
            return

        buf = McUtil.readFile(McConst.mainCfgFile)
        if buf == "":
            return

        dataObj = json.loads(buf)
        if "listenIp" in dataObj:
            self.param.listenIp = dataObj["listenIp"]
        if "mainPort" in dataObj:
            self.param.mainPort = dataObj["mainPort"]
        if "webAcceptForeign" in dataObj:
            self.param.webAcceptForeign = dataObj["webAcceptForeign"]
        if "preferedUpdatePeriod" in dataObj:
            self.param.mainCfg["preferedUpdatePeriod"] = dataObj["preferedUpdatePeriod"]
        if "country" in dataObj:
            self.param.mainCfg["country"] = dataObj["country"]
        if "location" in dataObj:
            if "country" not in dataObj:
                raise Exception("only \"location\" specified in main config file")
            self.param.mainCfg["location"] = dataObj["location"]

    def _sigHandlerINT(self):
        logging.info("SIGINT received.")
        self.param.mainloop.stop()
        return True

    def _sigHandlerTERM(self):
        logging.info("SIGTERM received.")
        self.param.mainloop.stop()
        return True
