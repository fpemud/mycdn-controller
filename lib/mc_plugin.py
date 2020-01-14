#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import libxml2
from mc_param import McConst


class McPluginManager:

    def __init__(self, param):
        self.param = param

    def loadPlugins(self):
        for fn in os.listdir(McConst.etcDir):
            if not fn.endswith(".conf"):
                continue
            pluginName = fn.replace(".conf", "")
            pluginPath = os.path.join(McConst.pluginsDir, pluginName)
            if not os.path.isdir(pluginPath):
                raise Exception("Invalid configuration file %s" % (fn))
            self._load(pluginName, pluginPath)

    def _load(self, name, path):
        # get metadata.xml file
        metadata_file = os.path.join(path, "metadata.xml")
        if not os.path.exists(metadata_file):
            raise Exception("plugin %s has no metadata.xml" % (name))
        if not os.path.isfile(metadata_file):
            raise Exception("metadata.xml for plugin %s is not a file" % (name))
        if not os.access(metadata_file, os.R_OK):
            raise Exception("metadata.xml for plugin %s is invalid" % (name))

        # check metadata.xml file content
        # FIXME
        tree = libxml2.parseFile(metadata_file)
        # if True:
        #     dtd = libxml2.parseDTD(None, constants.PATH_PLUGIN_DTD_FILE)
        #     ctxt = libxml2.newValidCtxt()
        #     messages = []
        #     ctxt.setValidityErrorHandler(lambda item, msgs: msgs.append(item), None, messages)
        #     if tree.validateDtd(ctxt, dtd) != 1:
        #         msg = ""
        #         for i in messages:
        #             msg += i
        #         raise exceptions.IncorrectPluginMetaFile(metadata_file, msg)

        # get data from metadata.xml file
        root = tree.getRootElement()

        # create McMirrorSite objects
        for child in root.xpathEval(".//file-mirror"):
            obj = McMirrorSite(self.param, path, child)
            assert obj.id not in self.param.mirrorSiteDict
            self.param.mirrorSiteDict[obj.id] = obj

        # create McMirrorSite objects, use file-mirror and git-mirror as the same yet
        for child in root.xpathEval(".//git-mirror"):
            obj = McMirrorSite(self.param, path, child)
            assert obj.id not in self.param.mirrorSiteDict
            self.param.mirrorSiteDict[obj.id] = obj

        # record plugin id
        self.param.pluginList.append(root.prop("id"))


class McMirrorSite:

    def __init__(self, param, pluginDir, rootElem):
        self.id = rootElem.prop("id")

        # data directory
        self.dataDir = rootElem.xpathEval(".//data-directory")[0].getContent()
        self.dataDir = os.path.join(McConst.cacheDir, self.dataDir)

        # initializer
        self.initializerExe = None
        if True:
            elem = rootElem.xpathEval(".//initializer")[0]
            self.initializerExe = elem.xpathEval(".//executable")[0].getContent()
            self.initializerExe = os.path.join(pluginDir, self.initializerExe)

        # updater
        self.updaterExe = None
        self.schedExpr = None
        if True:
            elem = rootElem.xpathEval(".//updater")[0]
            self.updaterExe = elem.xpathEval(".//executable")[0].getContent()
            self.updaterExe = os.path.join(pluginDir, self.updaterExe)
            self.schedExpr = elem.xpathEval(".//cron-expression")[0].getContent()           # FIXME: add check

        # advertiser
        self.advertiseProtocolList = []
        for child in rootElem.xpathEval(".//advertiser")[0].xpathEval(".//protocol"):
            self.advertiseProtocolList.append(child.getContent())
