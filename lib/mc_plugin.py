#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
from mc_util import McUtil


class McPlugin:

    def __init__(self, name, path):
        self.param = param
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
        if False:
            dtd = libxml2.parseDTD(None, constants.PATH_PLUGIN_DTD_FILE)
            ctxt = libxml2.newValidCtxt()
            messages = []
            ctxt.setValidityErrorHandler(lambda item, msgs: msgs.append(item), None, messages)
            if tree.validateDtd(ctxt, dtd) != 1:
                msg = ""
                for i in messages:
                    msg += i
                raise exceptions.IncorrectPluginMetaFile(metadata_file, msg)

        # get metadata from metadata.xml file
        metadata = {}
        if True:
            root = tree.getRootElement()
            self.ID = root.prop("id")

        # create objects
        child = root.children
        while child:
            if child.name == "mirror-site":
                obj = McMirrorSiteObject(child)
                self.mirrorSiteList.append(obj)
            child = child.next

        # logger
        self._logger = logging.getLogger(self.ID)


class McMirrorSiteObject:

    SCHED_ONESHOT = 0
    SCHED_PERIODICAL = 1
    SCHED_PERSIST = 2

    def __init__(self, pluginDir, rootElem):
        self.dataDir = rootElem.xpath(".//data-directory")[0].text

        # database
        self.dbObj = None
        if True:
            elem = rootElem.xpath(".//public-mirror-database")[0]
            filename = os.path.join(pluginDir, elem.xpath(".//filename"))
            classname = elem.xpath(".//classname")
            try:
                f = open(filename)
                m = imp.load_module(metadata["filename"][:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
                plugin_class = getattr(m, metadata["classname"])
            except:
                raise Exception("syntax error")
            self.dbObj = plugin_class()

        # updater
        self.updaterObj = None
        self.sched = None
        self.schedExpr = None
        if True:
            elem = rootElem.xpath(".//updater")[0]

            filename = os.path.join(pluginDir, elem.xpath(".//filename"))
            classname = elem.xpath(".//classname")
            try:
                f = open(filename)
                m = imp.load_module(metadata["filename"][:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
                plugin_class = getattr(m, metadata["classname"])
            except:
                raise Exception("syntax error")
            self.updaterObj = plugin_class()

            self.sched = elem.xpath(".//scheduler")[0]
            if sched == "oneshot":
                sched = McMirrorSite.SCHED_ONESHOT
            elif sched == "periodical":
                sched = McMirrorSite.SCHED_PERIODICAL
            elif sched == "persist":
                sched = McMirrorSite.SCHED_PERSIST
            else:
                assert False

            self.schedExpr = elem.xpath("../cron-expression")[0].text

        # advertiser
        self.advertiseProtocolList = []
        if True:
            elem = rootElem.xpath(".//advertiser")[0]
            for child in elem.children:
                self.advertiseProtocolList.append(child.text)
