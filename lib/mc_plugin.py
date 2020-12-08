#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import glob
import json
import sqlparse
import lxml.etree
from datetime import timedelta
from collections import OrderedDict
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
        rootElem = lxml.etree.parse(metadata_file)
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

        # create McMirrorSite objects
        for child in rootElem.xpath(".//mirror-site"):
            obj = McMirrorSite(self.param, path, child, cfgDict)
            assert obj.id not in self.param.mirrorSiteDict
            self.param.mirrorSiteDict[obj.id] = obj

        # record plugin id
        self.param.pluginList.append(rootElem.get("id"))


class McMirrorSite:

    def __init__(self, param, pluginDir, rootElem, cfgDict):
        self.id = rootElem.get("id")
        self.cfgDict = cfgDict

        # persist mode
        self.bPersist = cfgDict.get("persist", False)

        # master directory
        if self.bPersist:
            self.masterDir = os.path.join(McConst.varDir, self.id)
        else:
            self.masterDir = os.path.join(McConst.cacheDir, self.id)
        McUtil.ensureDir(self.masterDir)

        # state directory (plugin can use it)
        self.pluginStateDir = os.path.join(self.masterDir, "state")
        McUtil.ensureDir(self.pluginStateDir)

        # storage
        self.storageDict = dict()
        for child in rootElem.xpath(".//storage"):
            st = child.get("type")
            if st not in ["file", "git", "mariadb"]:
                raise Exception("mirror site %s: invalid storage type %s" % (self.id, st))

            self.storageDict[st] = DynObject()
            self.storageDict[st].dataDir = os.path.join(self.masterDir, "storage-" + st)
            self.storageDict[st].pluginParam = {"data-directory": self.storageDict[st].dataDir}
            McUtil.ensureDir(self.storageDict[st].dataDir)

            if st == "mariadb":
                self.storageDict[st].tableInfo = OrderedDict()
                tl = child.xpath(".//database-schema")
                if len(tl) > 0:
                    databaseSchemaFile = os.path.join(pluginDir, tl[0].text)
                    for sql in sqlparse.split(McUtil.readFile(databaseSchemaFile)):
                        m = re.match("^CREATE +TABLE +(\\S+)", sql)
                        if m is None:
                            raise Exception("mirror site %s: invalid database schema for storage type %s" % (self.id, st))
                        self.storageDict[st].tableInfo[m.group(1)] = (-1, sql)

        # advertiser
        self.advertiseDict = dict()
        for child in rootElem.xpath(".//advertiser")[0].xpath(".//interface"):
            if ":" in child.text:
                advertiserName, interface = McUtil.splitToTuple(child.text, ":")
            else:
                advertiserName, interface = child.text, child.text
            if not all(x in self.storageDict.keys() for x in McAdvertiser.storageDependencyOfAdvertiser(advertiserName)):
                raise Exception("mirror site %s: lack storage for advertiser %s" % (self.id, advertiserName))
            if advertiserName not in self.advertiseDict:
                self.advertiseDict[advertiserName] = []
            self.advertiseDict[advertiserName].append(interface)

        # initializer
        self.initializerExe = None
        if True:
            slist = rootElem.xpath(".//initializer")
            if len(slist) > 0:
                self.initializerExe = slist[0].xpath(".//executable")[0].text
                self.initializerExe = os.path.join(pluginDir, self.initializerExe)

        # updater
        self.updaterExe = None
        self.schedType = None              # "interval" or "cronexpr"
        self.schedInterval = None          # timedelta
        self.schedCronExpr = None          # string
        self.updateRetryType = None        # "interval" or "cronexpr"
        self.updateRetryInterval = None    # timedelta
        self.updateRetryCronExpr = None    # string
        if True:
            slist = rootElem.xpath(".//updater")
            if len(slist) > 0:
                self.updaterExe = slist[0].xpath(".//executable")[0].text
                self.updaterExe = os.path.join(pluginDir, self.updaterExe)

                tag = slist[0].xpath(".//schedule")[0]
                self.schedType = tag.get("type")
                if self.schedType == "interval":
                    self.schedInterval = self._parseInterval(tag.text)
                elif self.schedType == "cronexpr":
                    self.schedCronExpr = self._parseCronExpr(tag.text)
                else:
                    raise Exception("mirror site %s: invalid schedule type %s" % (self.id, self.schedType))

                if len(slist[0].xpath(".//retry-after-failure")) > 0:
                    tag = slist[0].xpath(".//retry-after-failure")[0]
                    self.updateRetryType = tag.get("type")
                    if self.updateRetryType == "interval":
                        self.updateRetryInterval = self._parseInterval(tag.text)
                    elif self.updateRetryType == "cronexpr":
                        if self.schedType == "interval":
                            raise Exception("mirror site %s: invalid retry-after-update type %s" % (self.id, self.updateRetryType))
                        self.updateRetryCronExpr = self._parseCronExpr(tag.text)
                    else:
                        raise Exception("mirror site %s: invalid retry-after-update type %s" % (self.id, self.updateRetryType))

        # maintainer
        self.maintainerExe = None
        if True:
            slist = rootElem.xpath(".//maintainer")
            if len(slist) > 0:
                self.maintainerExe = slist[0].xpath(".//executable")[0].text
                self.maintainerExe = os.path.join(pluginDir, self.maintainerExe)

    def _parseInterval(self, intervalStr):
        m = re.match("([0-9]+)(h|d|w|m)", intervalStr)
        if m is None:
            raise Exception("invalid interval %s" % (intervalStr))

        if m.group(2) == "h":
            return timedelta(hours=int(m.group(1)))
        elif m.group(2) == "d":
            return timedelta(days=int(m.group(1)))
        elif m.group(2) == "w":
            return timedelta(weeks=int(m.group(1)))
        elif m.group(2) == "m":
            return timedelta(months=int(m.group(1)))
        else:
            assert False

    def _parseCronExpr(self, cronExprStr):
        # FIXME: should add checking
        return cronExprStr
