#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import json
import libxml2
from mc_util import McUtil
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
            pluginCfg = dict()
            with open(os.path.join(McConst.etcDir, fn), "r") as f:
                buf = f.read()
                if buf != "":
                    pluginCfg = json.loads(buf)
            self._load(pluginName, pluginPath, pluginCfg)

    def _load(self, name, path, cfgDict):
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
        for child in root.xpathEval(".//mirror-site"):
            obj = McMirrorSite(self.param, path, child, cfgDict)
            assert obj.id not in self.param.mirrorSiteDict
            self.param.mirrorSiteDict[obj.id] = obj

        # record plugin id
        self.param.pluginList.append(root.prop("id"))


class McMirrorSite:

    def __init__(self, param, pluginDir, rootElem, cfgDict):
        self.id = rootElem.prop("id")
        self.cfgDict = cfgDict

        # storage
        self.storageDict = dict()
        if True:
            tstr = rootElem.xpathEval(".//storage")[0].getContent()
            for item in tstr.split("|"):
                if item == "file":
                    self.storageDict[item] = McMirrorSiteStorageFile(self)
                elif item == "git":
                    self.storageDict[item] = McMirrorSiteStorageGit(self)
                else:
                    assert False

        # availablity mode
        self.availablityMode = "initialized"
        if True:
            slist = rootElem.xpathEval(".//availablity")
            if len(slist) > 0 and slist[0].getContent() == "always":
                self.availablityMode = "always"

        # advertiser
        self.advertiseDict = dict()
        for child in rootElem.xpathEval(".//advertiser")[0].xpathEval(".//interface"):
            storageName, protocol = McUtil.splitToTuple(child.getContent(), ":")
            if storageName not in self.advertiseDict:
                self.advertiseDict[storageName] = []
            self.advertiseDict[storageName].append(protocol)

        # initializer
        self.initializerExe = None
        if True:
            slist = rootElem.xpathEval(".//initializer")
            if len(slist) > 0:
                self.initializerExe = slist[0].xpathEval(".//executable")[0].getContent()
                self.initializerExe = os.path.join(pluginDir, self.initializerExe)

        # updater
        self.updaterExe = None
        self.schedExpr = None
        if True:
            slist = rootElem.xpathEval(".//updater")
            if len(slist) > 0:
                self.updaterExe = slist[0].xpathEval(".//executable")[0].getContent()
                self.updaterExe = os.path.join(pluginDir, self.updaterExe)
                self.schedExpr = slist[0].xpathEval(".//cron-expression")[0].getContent()   # FIXME: add check


class McMirrorSiteStorageFile:

    def __init__(self, parent):
        self.parent = parent
        self._varDir = os.path.join(McConst.stateDir, self.parent.id, "storage-file")
        self._cacheDir = os.path.join(McConst.cacheDir, self.parent.id, "storage-file")

    @property
    def varDir(self):
        return self._varDir

    @property
    def cacheDir(self):
        return self._cacheDir

    def initialize(self):
        McUtil.ensureDir(self._varDir)
        McUtil.ensureDir(self._cacheDir)

    def getParamForPlugin(self):
        return {
            "data-directory": self._cacheDir
        }


class McMirrorSiteStorageGit:

    def __init__(self, parent):
        self.parent = parent
        self._varDir = os.path.join(McConst.stateDir, self.parent.id, "storage-git")
        self._cacheDir = os.path.join(McConst.cacheDir, self.parent.id, "storage-git")

    @property
    def varDir(self):
        return self._varDir

    @property
    def cacheDir(self):
        return self._cacheDir

    def initialize(self):
        McUtil.ensureDir(self._varDir)
        McUtil.ensureDir(self._cacheDir)

    def getParamForPlugin(self):
        return {
            "data-directory": self._cacheDir
        }
