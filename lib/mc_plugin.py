#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import imp
import logging
import libxml2


class McPlugin:

    def __init__(self, name, path):
        self.mirrorSiteList = []

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
        self.id = root.prop("id")

        # create objects
        child = root.children
        while child:
            if child.name == "mirror-site":
                obj = McMirrorSite(self, path, child)
                self.mirrorSiteList.append(obj)
            child = child.next

        # logger
        self._logger = logging.getLogger(self.id)


class McMirrorSite:

    SCHED_ONESHOT = 0
    SCHED_PERIODICAL = 1
    SCHED_FOLLOW = 2
    SCHED_PERSIST = 3

    def __init__(self, plugin, pluginDir, rootElem):
        self.plugin = plugin
        self.id = plugin.id + " " + rootElem.prop("id")

        self.dataDir = rootElem.xpath(".//data-directory")[0].text

        # database
        self.dbObj = None
        if True:
            elem = rootElem.xpath(".//public-mirror-database")[0]
            filename = os.path.join(pluginDir, elem.xpath(".//filename"))
            classname = elem.xpath(".//classname")
            try:
                f = open(filename)
                m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
                plugin_class = getattr(m, classname)
            except:
                raise Exception("syntax error")
            self.dbObj = plugin_class()

        # updater
        self.updaterObjApi = None
        self.updaterObj = None
        self.sched = None
        if True:
            elem = rootElem.xpath(".//updater")[0]

            filename = os.path.join(pluginDir, elem.xpath(".//filename"))
            classname = elem.xpath(".//classname")
            try:
                f = open(filename)
                m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
                plugin_class = getattr(m, classname)
            except:
                raise Exception("syntax error")
            self.updaterObjApi = McMirrorSiteUpdaterApi(self)
            self.updaterObj = plugin_class(self.updaterObjApi)

            self.sched = elem.xpath(".//scheduler")[0]
            if self.sched == "oneshot":
                self.sched = McMirrorSite.SCHED_ONESHOT
            elif self.sched == "periodical":
                self.sched = McMirrorSite.SCHED_PERIODICAL
                self.schedExpr = elem.xpath(".//cron-expression")[0].text
            elif self.sched == "follow":
                self.sched = McMirrorSite.SCHED_FOLLOW
                self.followMirrorSiteId = elem.xpath(".//follow-mirror-site")[0].text
            elif self.sched == "persist":
                self.sched = McMirrorSite.SCHED_PERSIST
            else:
                assert False

        # advertiser
        self.advertiseProtocolList = []
        if True:
            elem = rootElem.xpath(".//advertiser")[0]
            for child in elem.children:
                self.advertiseProtocolList.append(child.text)


class McMirrorSiteUpdaterApi:

    def __init__(self, mirrorSite):
        self.mirrorSite = mirrorSite
        self.mcUpdater = None           # set by McMirrorSiteUpdater
        self.updateStatus = None        # same as above
        self.updateDatetime = None      # same as above
        self.updateProgress = None      # same as above

    def get_country(self):
        # FIXME
        return "CN"

    def get_location(self):
        # FIXME
        return None

    def get_data_dir(self):
        return self.parent.dataDir

    def get_log_dir(self):
        # FIXME
        return None

    def notify_progress(self, progress, finished):
        assert 0 <= progress <= 100
        assert finished is not None
        self.mirrorSite._notifyProgress(self, progress, finished)
