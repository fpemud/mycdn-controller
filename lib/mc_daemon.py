#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import json
import prctl
import signal
import shutil
import socket
import logging
import asyncio
import asyncio_glib
from gi.repository import GLib
from mc_util import McUtil
from mc_util import DropPriviledge
from mc_util import StdoutRedirector
from mc_util import AvahiServiceRegister
from mc_param import McConst
from mc_plugin import McPluginManager
from mc_advertiser import McMainAdvertiser
from mc_updater import McMirrorSiteUpdater


class McDaemon:

    def __init__(self, param):
        self.param = param

    def run(self):
        self._loadMainCfg()
        try:
            # create directories
            McUtil.preparePersistDir(McConst.varDir, McConst.uid, McConst.gid, 0o755)
            McUtil.preparePersistDir(McConst.cacheDir, McConst.uid, McConst.gid, 0o755)
            McUtil.preparePersistDir(McConst.logDir, McConst.uid, McConst.gid, 0o755)
            McUtil.prepareTransientDir(McConst.runDir, McConst.uid, McConst.gid, 0o755)
            McUtil.prepareTransientDir(McConst.tmpDir, McConst.uid, McConst.gid, 0o755)

            with DropPriviledge(McConst.uid, McConst.gid, caps=[prctl.CAP_NET_BIND_SERVICE]):
                try:
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

                    # load plugin, storage, advertiser
                    self.param.pluginManager = McPluginManager(self.param)
                    self.param.pluginManager.loadEnabledPlugins()
                    logging.info("Mirror site plugins loaded: %s" % (",".join(sorted(self.param.pluginManager.getEnabledPluginNameList()))))
                    self.param.pluginManager.loadStorageObjects()           # log by itself
                    self.param.pluginManager.loadAdvertiserObjects()        # log by itself

                    # main advertiser
                    self.param.globalAdvertiser = McMainAdvertiser(self.param)
                    logging.info("Mirror site main advertiser initialized.")

                    # updater
                    self.param.updater = McMirrorSiteUpdater(self.param)
                    logging.info("Mirror site updater initialized.")

                    # register serivce
                    if McConst.avahiSupport:
                        self.param.avahiObj = AvahiServiceRegister()
                        self.param.avahiObj.add_service(socket.gethostname(), McConst.avahiServiceName, self.param.mainPort)
                        self.param.avahiObj.start()

                    # register pserver
                    if McConst.pserverSupport and self.mainCfg["pserver-domain-name"] is not None:
                        import pservers.client
                        self.param.pserversClientObj = pservers.client.PersistClientGLib()
                        self.param.pserversClientObj.register(self.mainCfg["pserver-domain-name"], self.param.mainPort)
                        self.param.pserversClientObj.start()

                    # start main loop
                    logging.info("Mainloop begins.")
                    GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGINT, self._sigHandlerINT, None)
                    GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGTERM, self._sigHandlerTERM, None)
                    self.param.mainloop.run_forever()
                    logging.info("Mainloop exits.")
                finally:
                    if self.param.pserversClientObj is not None:
                        self.param.pserversClientObj.stop()
                    if self.param.avahiObj is not None:
                        self.param.avahiObj.stop()
                    if self.param.updater is not None:
                        self.param.updater.dispose()
                    if self.param.globalAdvertiser is not None:
                        self.param.globalAdvertiser.dispose()
                    for obj in self.param.advertiserDict.values():
                        obj.dispose()
                    for obj in self.param.storageDict.values():
                        obj.dispose()
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
        if "preferedUpdatePeriodList" in dataObj:
            self.param.mainCfg["preferedUpdatePeriodList"] = dataObj["preferedUpdatePeriodList"]
        if "country" in dataObj:
            self.param.mainCfg["country"] = dataObj["country"]
        if "location" in dataObj:
            if "country" not in dataObj:
                raise Exception("only \"location\" specified in main config file")
            self.param.mainCfg["location"] = dataObj["location"]
        if "pserver" in dataObj:
            if "domain-name" not in dataObj:
                raise Exception("no \"domain-name\" specified in \"pserver\" secion in main config file")
            self.param.mainCfg["pserver-domain-name"] = dataObj["pserver"]["domain-name"]

    def _sigHandlerINT(self, signum):
        logging.info("SIGINT received.")
        self.param.mainloop.call_soon_threadsafe(self.param.mainloop.stop)
        return True

    def _sigHandlerTERM(self, signum):
        logging.info("SIGTERM received.")
        self.param.mainloop.call_soon_threadsafe(self.param.mainloop.stop)
        return True
