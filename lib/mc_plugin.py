#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import glob
import json
import lxml.etree
from datetime import timedelta
from mc_util import McUtil
from mc_param import McConst


class McPluginManager:

    def __init__(self, param):
        self.param = param

    def getMirrorSitePluginNameList(self):
        return os.listdir(McConst.pluginsDir)

    def loadMirrorSitePlugins(self):
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
            self._loadOnePlugin(pluginName, pluginPath, pluginCfg)

    def getStorageNameList(self):
        return os.listdir(McConst.storageDir)

    def loadStorageObjects(self):
        nameDict = dict()
        for msId, msObj in self.param.mirrorSiteDict.items():
            for st in msObj.storageDict:
                if st not in nameDict:
                    nameDict[st] = []
                nameDict[st].append(msId)

        nameList = sorted(list(nameDict.keys()))
        if "file" in nameList:
            # always load built-in storage object first
            nameList.remove("file")
            nameList.insert(0, "file")

        for st in nameList:
            obj = self._loadOneStorageObject(st, os.path.join(McConst.storageDir, st), nameDict[st])
            self.param.storageDict[st] = obj

    def getAdvertiserNameList(self):
        return os.listdir(McConst.advertiserDir)

    def loadAdvertiserObjects(self):
        nameDict = dict()
        for msId, msObj in self.param.mirrorSiteDict.items():
            for st in msObj.advertiserDict:
                if st not in nameDict:
                    nameDict[st] = []
                nameDict[st].append(msId)

        for name in sorted(list(nameDict.keys())):
            obj = self._loadOneAdvertiserObject(st, os.path.join(McConst.advertiserDir, name), nameDict[name])
            self.param.advertiserDict[st] = obj

    def _loadOnePlugin(self, name, path, cfgDict):
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

    def _loadOneStorageObject(self, name, path, mirrorSiteIdList):
        # get metadata.xml file
        metadata_file = os.path.join(path, "metadata.xml")
        if not os.path.exists(metadata_file):
            raise Exception("storage %s has no metadata.xml" % (name))
        if not os.path.isfile(metadata_file):
            raise Exception("metadata.xml for storage %s is not a file" % (name))
        if not os.access(metadata_file, os.R_OK):
            raise Exception("metadata.xml for storage %s is invalid" % (name))

        # check metadata.xml file content
        # FIXME
        rootElem = lxml.etree.parse(metadata_file)
        fn = rootElem.xpath(".//file")[0].text
        klass = rootElem.xpath(".//class")[0].text
        bAdvertiser = (len(rootElem.xpath(".//with-integrated-advertiser")) > 0)

        # prepare storage initialization parameter
        param = {
            "mirror-sites": dict(),
        }
        if bAdvertiser:
            param.update({
                "listen-ip": "",            # FIXME
                "temp-directory": "",       # FIXME
                "log-directory": "",        # FIXME
                "log-file-size": -1,
                "log-file-count": -1,
            })
        for msId in mirrorSiteIdList:
            param["mirror-sites"][msId] = {
                "plugin-directory": "",
                "state-directory": self.param.mirrorSiteDict[msId].pluginStateDir,
                "data-directory": self.param.mirrorSiteDict[msId].getDataDirForStorage(name),
                "config-xml": self.param.mirrorSiteDict[msId].storageDict[name][0],
            }

        # create object
        modname, mod = McUtil.loadPythonFile(os.path.join(path, fn))
        return eval("mod.%s(param)" % (klass))
        
    def _loadOneAdvertiserObject(self, name, path, mirrorSiteIdList):
        # get metadata.xml file
        metadata_file = os.path.join(path, "metadata.xml")
        if not os.path.exists(metadata_file):
            raise Exception("advertiser %s has no metadata.xml" % (name))
        if not os.path.isfile(metadata_file):
            raise Exception("metadata.xml for advertiser %s is not a file" % (name))
        if not os.access(metadata_file, os.R_OK):
            raise Exception("metadata.xml for advertiser %s is invalid" % (name))

        # check metadata.xml file content
        # FIXME
        rootElem = lxml.etree.parse(metadata_file)
        fn = rootElem.xpath(".//file")[0].text
        klass = rootElem.xpath(".//class")[0].text

        # prepare advertiser initialization parameter
        param = {
            "listen-ip": "",            # FIXME
            "temp-directory": "",        # FIXME
            "log-directory": "",        # FIXME
            "log-file-size": -1,
            "log-file-count": -1,
            "mirror-sites": dict(),
        }
        for msId in mirrorSiteIdList:
            param["mirror-sites"][msId] = {
                "state-directory": self.param.mirrorSiteDict[msId].pluginStateDir,
                "config-xml": self.param.mirrorSiteDict[msId].advertiserDict[name][0],
                "storage-param": dict()
            }
            for st in self.param.mirrorSiteDict[msId].storageDict:
                param["mirror-sites"][msId]["storage-param"][st] = self.param.storageDict[st].get_param(msId)

        # create object
        modname, mod = McUtil.loadPythonFile(os.path.join(path, fn))
        print(name)
        return eval("mod.%s(param)" % (klass))


class McMirrorSite:

    def __init__(self, param, pluginDir, rootElem, cfgDict):
        self.param = param
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
        self.storageDict = dict()                       # {name:(config-xml,data-directory)}
        for child in rootElem.xpath(".//storage"):
            st = child.get("type")
            if st not in self.param.pluginManager.getStorageNameList():
                raise Exception("mirror site %s: invalid storage type %s" % (self.id, st))
            # record outer xml
            self.storageDict[st] = (lxml.etree.tostring(child, encoding="unicode"), self.getDataDirForStorage(st))
            # create data directory
            McUtil.ensureDir(self.getDataDirForStorage(st))

        # advertiser
        self.advertiserDict = dict()                 # {name:(config-xml)}
        for child in rootElem.xpath(".//advertiser"):
            st = child.get("type")
            if st not in self.param.pluginManager.getAdvertiserNameList():
                raise Exception("mirror site %s: invalid advertiser type %s" % (self.id, st))
            # record outer xml
            self.advertiserDict[st] = (lxml.etree.tostring(child, encoding="unicode"))

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

    def getDataDirForStorage(self, storageName):
        return os.path.join(self.masterDir, "storage-%s" % (storageName))

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
