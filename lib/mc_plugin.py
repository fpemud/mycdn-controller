#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
from mc_util import McUtil


class McPluginManager:

    def __init__(self, param):
        self.param = param
        self.pluginList = []
    
    def loadPlugins(self):
        for fn in os.listdir(self.etcDir):
            if not fn.endswith(".conf"):            
                continue
            pluginName = fn.replace(".conf", "")
            pluginPath = os.path.join(self.param.pluginsDir, pluginName)
            if not os.path.isdir(pluginPath):
                raise Exception("Invalid configuration file %s" % (fn))
            pluginObj = McPlugin(pluginName, pluginPath)
            self.pluginList.append(pluginObj)


class McPlugin:

    def __init__(self, name, path):
        self.param = param
        self.mirrorObjList = []

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

        # create real plugin object
        self._obj = None
        if True:
            filename = os.path.join(path, metadata["filename"])
            if not os.path.exists(filename):
                raise Exception("plugin %s has no ")
            if not os.path.isfile(filename):
                raise exceptions.PluginFileNotFile(filename)
            if not os.access(filename, os.R_OK):
                raise exceptions.PluginFileNotReadable(filename)

            plugin_class = None
            try:
                f = open(filename)
                m = imp.load_module(metadata["filename"][:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
                plugin_class = getattr(m, metadata["classname"])
            except:
                raise exceptions.PluginSyntaxError(filename)
            self._obj = plugin_class()

        # static variables
        self._app = app                                                 # FIXME
        self._require_auth = metadata["require_auth"]
        self._require_selenium = metadata["require_selenium"]

        # logger
        self._logger = logging.getLogger(self.ID)

        # login status
        self._credential = None
        self._login_status = None
        self._login_cookie = None

        # search status
        self._search_status = None
        self._search_param = None

        # search results total count
        self._results_total_count_lock = threading.Lock()
        self._results_total_count = None
        self._results_total_count_changed = None

        # loaded search result
        self._results_tmp_queue_lock = threading.Lock()
        self._results_tmp_queue = None

        # loaded result total count
        self._results_loaded = None     # only used in working thread, no lock needed
