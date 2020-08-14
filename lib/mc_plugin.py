#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import glob
import json
import libxml2
import sqlparse
from mc_util import McUtil
from mc_util import DynObject
from mc_param import McConst
from mc_advertiser import McAdvertiser


class McPluginManager:

    def __init__(self, param):
        self.param = param

    def loadPlugins(self):
        for fn in glob.glob(McConst.pluginCfgFileGlobPattern):
            pluginName = McUtil.rreplace(os.path.basename(fn).replace("plugin-", "", 1), ".conf", "", 1)
            pluginPath = os.path.join(McConst.pluginsDir, pluginName)
            if not os.path.isdir(pluginPath):
                continue
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

        # availablity mode
        self.availablityMode = "initialized"
        if True:
            slist = rootElem.xpathEval(".//availablity")
            if len(slist) > 0 and slist[0].getContent() == "always":
                self.availablityMode = "always"

        # persist mode
        self.bPersist = False
        if len(rootElem.xpathEval(".//static")) > 0:
            self.bPersist = True
        if cfgDict.get("persist", False):
            self.bPersist = True

        # master directory
        if self.bPersist:
            self.masterDir = os.path.join(McConst.varDir, self.id)
        else:
            self.masterDir = os.path.join(McConst.cacheDir, self.id)

        # storage
        self.storageDict = dict()
        if True:
            for child in rootElem.xpathEval(".//storage"):
                # deprecated
                if not child.hasAttribute("type"):
                    tstr = rootElem.xpathEval(".//storage")[0].getContent()
                    for st in tstr.split("|"):
                        self.storageDict[st] = DynObject()
                        self.storageDict[st].dataDir = os.path.join(self.masterDir, "storage-" + st)
                        self.storageDict[st].pluginParam = {"data-directory": self.storageDict[st].dataDir}
                    continue

                st = child.getAttribute("type")
                if st not in ["file", "git", "mariadb"]:
                    raise Exception("invalid storage type %s" % (st))
                self.storageDict[st] = DynObject()
                self.storageDict[st].dataDir = os.path.join(self.masterDir, "storage-" + st)
                self.storageDict[st].pluginParam = {"data-directory": self.storageDict[st].dataDir}
                if st == "mariadb":
                    self.storageDict[st].tableInfo = dict()
                    tl = child.xpathEval(".//database-schema")
                    if len(tl) > 0:
                        databaseSchemaFile = tl[0].getContent()
                        for sql in sqlparse.split(McUtil.readFile(databaseSchemaFile)):
                            m = re.match("^CREATE +TABLE +(\\S+)", sql)
                            if m is None:
                                raise Exception("invalid database schema for storage type %s" % (st))
                            self.storageDict[st].tableInfo[m.group(1)] = sql

        # advertiser
        self.advertiseDict = dict()
        for child in rootElem.xpathEval(".//advertiser")[0].xpathEval(".//interface"):
            if ":" in child.getContent():
                advertiserName, interface = McUtil.splitToTuple(child.getContent(), ":")
            else:
                advertiserName, interface = child.getContent(), child.getContent()
            if not all(x in self.storageDict.keys() for x in McAdvertiser.storageDependencyOfAdvertiser(advertiserName)):
                raise Exception("lack storage for advertiser %s" % (advertiserName))
            if advertiserName not in self.advertiseDict:
                self.advertiseDict[advertiserName] = []
            self.advertiseDict[advertiserName].append(interface)

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
