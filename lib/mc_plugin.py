#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import imp
import libxml2
import threading
from gi.repository import GLib


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

        # create McPublicMirrorDatabase objects
        for child in root.xpathEval(".//public-mirror-database"):
            obj = McPublicMirrorDatabase(self.param, self, path, child)
            assert obj.id not in [x.id for x in self.param.publicMirrorDatabaseList]   # FIXME
            self.param.publicMirrorDatabaseList.append(obj)

        # create McMirrorSite objects
        for child in root.xpathEval(".//file-mirror"):
            obj = McMirrorSite(self.param, path, child)
            assert obj.id not in [x.id for x in self.param.mirrorSiteList]             # FIXME
            self.param.mirrorSiteList.append(obj)

        # create McMirrorSite objects, use file-mirror and git-mirror as the same yet
        for child in root.xpathEval(".//git-mirror"):
            obj = McMirrorSite(self.param, path, child)
            assert obj.id not in [x.id for x in self.param.mirrorSiteList]             # FIXME
            self.param.mirrorSiteList.append(obj)

        # record plugin id
        self.param.pluginList.append(root.prop("id"))


class McPublicMirrorDatabase:

    def __init__(self, param, plugin, pluginDir, rootElem):
        self.id = rootElem.prop("id")

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

    def __init__(self, param, pluginDir, rootElem):
        self.id = rootElem.prop("id")

        # data directory
        self.dataDir = rootElem.xpathEval(".//data-directory")[0].getContent()
        self.dataDir = os.path.join(param.cacheDir, self.dataDir)

        # updater
        self.updaterObj = None
        self.schedExpr = None
        self.useWorkerProc = None
        if True:
            elem = rootElem.xpathEval(".//updater")[0]

            self.schedExpr = elem.xpathEval(".//cron-expression")[0].getContent()
            # FIXME: add check

            ret = elem.xpathEval(".//runtime")
            if len(ret) > 0:
                self.runtime = ret[0].getContent()
                # FIXME: add check
            else:
                self.runtime = "glib-mainloop"

            filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
            classname = elem.xpathEval(".//classname")[0].getContent()
            while True:
                if self.runtime == "glib-mainloop":
                    try:
                        f = open(filename)
                        m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
                        plugin_class = getattr(m, classname)
                    except:
                        raise Exception("syntax error")
                    self.updaterObj = plugin_class()
                    break

                if self.runtime == "thread":
                    self.updaterObj = _UpdaterObjProxyRuntimeThread(filename, classname)
                    break

                if self.runtime == "process":
                    self.updaterObj = _UpdaterObjProxyRuntimeProcess(param, filename, classname)
                    break

                assert False

        # advertiser
        self.advertiseProtocolList = []
        for child in rootElem.xpathEval(".//advertiser")[0].xpathEval(".//protocol"):
            self.advertiseProtocolList.append(child.getContent())


class _UpdaterObjProxyRuntimeThread:

    def __init__(self, filename, classname):
        self.threadObj = None
        self.realProgressChanged = None
        self.realErrorOccured = None
        self.realErrorOccuredAndHoldFor = None
        try:
            f = open(filename)
            m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
            plugin_class = getattr(m, classname)
        except:
            raise Exception("syntax error")
        self.realUpdaterObj = plugin_class()

    def init_start(self, api):
        self.realProgressChanged = api.progress_changed
        self.realErrorOccured = api.error_occured
        self.realErrorOccuredAndHoldFor = api.error_occured_and_hold_for
        self.threadObj = _UpdaterObjProxyRuntimeThreadImpl(self, api, self.realUpdaterObj.init)
        self.threadObj.start()

    def init_stop(self):
        self.threadObj.stopped = True

    def update_start(self, api):
        self.realProgressChanged = api.progress_changed
        self.realErrorOccured = api.error_occured
        self.realErrorOccuredAndHoldFor = api.error_occured_and_hold_for
        self.threadObj = _UpdaterObjProxyRuntimeThreadImpl(self, api, self.realUpdaterObj.init)
        self.threadObj.start()

    def update_stop(self):
        self.threadObj.stopped = True

    def _progressChangedIdleHandler(self, progress):
        self.realProgressChanged(progress)
        if progress == 100:
            self.threadObj = None
            self.realErrorOccuredAndHoldFor = None
            self.realErrorOccured = None
            self.realProgressChanged = None
        return False

    def _errorOccuredIdleHandler(self, exc_info):
        self.realErrorOccured(exc_info)
        self.threadObj = None
        self.realErrorOccuredAndHoldFor = None
        self.realErrorOccured = None
        self.realProgressChanged = None
        return False

    def _errorOccuredAndHoldForIdleHandler(self, seconds, exc_info):
        self.realErrorOccured(seconds, exc_info)
        self.threadObj = None
        self.realErrorOccuredAndHoldFor = None
        self.realErrorOccured = None
        self.realProgressChanged = None
        return False


class _UpdaterObjProxyRuntimeThreadImpl(threading.Thread):

    def __init__(self, parent, api, targetFunc):
        super().__init__()
        self.parent = parent
        self.targetFunc = targetFunc
        self.stopped = False
        self.api = api
        self.api.is_stopped = lambda: self.stopped
        self.api.progress_changed = self._progressChanged
        self.api.error_occured = self._errorOccured
        self.api.error_occured_and_hold_for = self._errorOccuredAndHoldFor

    def run(self):
        try:
            self.targetFunc(self.api)
            if self.api is not None:
                self.api.progress_changed(100)
        except:
            if self.api is not None:
                self.api.error_occured(sys.exc_info())

    def _progressChanged(self, progress):
        if progress == 100:
            self.api = None
        GLib.idle_add(self.parent._progressChangedIdleHandler, progress)

    def _errorOccured(self, exc_info):
        self.api = None
        GLib.idle_add(self.parent._errorOccuredIdleHandler, exc_info)

    def _errorOccuredAndHoldFor(self, seconds, exc_info):
        self.api = None
        GLib.idle_add(self.parent._errorOccuredAndHoldForIdleHandler, seconds, exc_info)


class _UpdaterObjProxyRuntimeProcess:

    def __init__(self, param, filename, classname):
        self.param = param
        self.filename = filename
        self.classname = classname

    def init_start(self, api):
        assert False

    def init_stop(self):
        assert False

    def update_start(self, api):
        assert False

    def update_stop(self):
        assert False


# public-mirror-database ######################################################

class TemplatePublicMirrorDatabase:

    def get(self, extended=False):
        assert False

    def query(self, country=None, location=None, protocolList=None, extended=False, maximum=1):
        assert False


# file-mirror #################################################################

class TemplateMirrorSiteUpdater:

    def init_start(self, api):
        assert False

    def init_stop(self):
        assert False

    def update_start(self, api):
        assert False

    def update_stop(self):
        assert False


class TemmplateMirrorSiteUpdaterInitApi:

    def get_country(self):
        assert False

    def get_location(self):
        assert False

    def get_data_dir(self):
        assert False

    def get_log_dir(self):
        assert False

    def progress_changed(progress):
        assert False

    def error_occured(exc_info):
        assert False

    def error_occured_and_hold_for(seconds, exc_info):
        assert False


class TemmplateMirrorSiteUpdaterUpdateApi(TemmplateMirrorSiteUpdaterInitApi):

    def get_sched_datetime(self):
        assert False


class TemplateMirrorSiteUpdaterRuntimeThread:

    def init(self, api):
        assert False

    def update(self, api):
        assert False


class TemmplateMirrorSiteUpdaterRuntimeThreadInitApi(TemmplateMirrorSiteUpdaterInitApi):

    def is_stopped(self):
        assert False


class TemmplateMirrorSiteUpdaterRuntimeThreadUpdateApi(TemmplateMirrorSiteUpdaterInitApi):

    def is_stopped(self):
        assert False


class TemplateMirrorSiteUpdaterRuntimeProcess:

    def init(self, api):
        assert False

    def update(self, api):
        assert False


class TemmplateMirrorSiteUpdaterRuntimeProcessInitApi(TemmplateMirrorSiteUpdaterInitApi):
    # stop by SIGTERM signal
    pass


class TemmplateMirrorSiteUpdaterRuntimeProcessUpdateApi(TemmplateMirrorSiteUpdaterInitApi):
    # stop by SIGTERM signal
    pass
