#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import imp
import libxml2


class McPluginManager:

    def __init__(self, param):
        self.param = param

    def loadPlugins(self):
        for fn in os.listdir(self.param.etcDir):
            if not fn.endswith(".conf"):
                continue
            pluginName = fn.replace(".conf", "")
            pluginPath = os.path.join(self.param.pluginsDir, pluginName)
            if not os.path.isdir(pluginPath):
                raise Exception("Invalid configuration file %s" % (fn))
            self._load(self.param, pluginName, pluginPath)

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

        # create McPublicMirrorDatabase objects
        for child in root.xpathEval(".//public-mirror-database"):
            obj = McPublicMirrorDatabase(self.param, self, path, child)
            assert obj.id not in [x.id for x in self.param.McPublicMirrorDatabase]     # FIXME
            self.param.publicMirrorDatabaseList.append(obj)

        # create McMirrorSite objects
        for child in root.xpathEval(".//mirror-site"):
            obj = McMirrorSite(self.param, self, path, child)
            assert obj.id not in [x.id for x in self.param.mirrorSiteList]             # FIXME
            self.param.mirrorSiteList.append(obj)

        # record plugin id
        self.param.pluginList.append(root.prop("id"))


class McPublicMirrorDatabase:

    def __init__(self, param, plugin, pluginDir, rootElem):
        self.dbObj = None
        if True:
            filename = os.path.join(pluginDir, rootElem.xpathEval(".//filename")[0].getContent())
            classname = rootElem.xpathEval(".//classname")[0].getContent()
            try:
                f = open(filename)
                m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
                plugin_class = getattr(m, classname)
            except:
                raise Exception("syntax error")
            self.dbObj = plugin_class()


class McMirrorSite:

    SCHED_ONESHOT = 0
    SCHED_PERIODICAL = 1
    SCHED_PERSIST = 2

    def __init__(self, param, plugin, pluginDir, rootElem):
        self.plugin = plugin
        self.id = rootElem.prop("id")

        self.dataDir = rootElem.xpathEval(".//data-directory")[0].getContent()
        self.dataDir = os.path.join(param.cacheDir, self.dataDir)

        # updater
        self.updaterObjApi = None
        self.updaterObj = None
        self.sched = None
        if True:
            elem = rootElem.xpathEval(".//updater")[0]
            filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
            classname = elem.xpathEval(".//classname")[0].getContent()
            try:
                f = open(filename)
                m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
                plugin_class = getattr(m, classname)
            except:
                raise Exception("syntax error")
            self.updaterObjApi = McMirrorSiteUpdaterApi(param, self)
            self.updaterObj = plugin_class(self.updaterObjApi)

            self.sched = elem.xpathEval(".//scheduler")[0].getContent()
            if self.sched == "oneshot":
                self.sched = McMirrorSite.SCHED_ONESHOT
            elif self.sched == "periodical":
                self.sched = McMirrorSite.SCHED_PERIODICAL
                self.schedExpr = elem.xpathEval(".//cron-expression")[0].getContent()
            elif self.sched == "persist":
                self.sched = McMirrorSite.SCHED_PERSIST
            else:
                assert False

        # advertiser
        self.advertiseProtocolList = []
        for child in rootElem.xpathEval(".//advertiser")[0].xpathEval(".//protocol"):
            self.advertiseProtocolList.append(child.getContent())


class McMirrorSiteUpdaterApi:

    def __init__(self, param, mirrorSite):
        self.param = param
        self.mirrorSite = mirrorSite

        # set by McMirrorSiteUpdater
        self.updateStatus = None
        self.updateDatetime = None
        self.progress = None
        self.progressNotifier = None

    def get_country(self):
        # FIXME
        return "CN"

    def get_location(self):
        # FIXME
        return None

    def get_data_dir(self):
        return self.mirrorSite.dataDir

    def get_log_dir(self):
        return self.param.logDir

    def notify_progress(self, progress, finished):
        assert 0 <= progress <= 100
        assert finished is not None
        self.progressNotifier(self.mirrorSite, progress, finished)
