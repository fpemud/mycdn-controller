#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
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
            assert obj.id not in [x.id for x in self.param.McPublicMirrorDatabase]     # FIXME
            self.param.publicMirrorDatabaseList.append(obj)

        # create McMirrorSite objects
        for child in root.xpathEval(".//mirror-site"):
            obj = McMirrorSite(self.param, path, child)
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
                assert False
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


class _UpdaterObjProxyRuntimeThread(threading.Thread):

    def __init__(self, filename, classname):
        super().__init__()
        try:
            f = open(filename)
            m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
            plugin_class = getattr(m, classname)
        except:
            raise Exception("syntax error")
        self.realUpdaterObj = plugin_class()

    def init_start(self, api):
        self.__prepareRun(api, self.realUpdaterObj.init)
        self.start()

    def init_stop(self):
        self.stopped = True

    def update_start(self, api):
        self.__prepareRun(api, self.realUpdaterObj.update)
        self.start()

    def update_stop(self):
        self.stopped = True

    def run(self):
        self.targetFunc(self.api)

    def __prepare(self, api, targetFunc):
        self.targetFunc = targetFunc
        self.stopped = False
        self.realProgressChanged = api.progress_changed
        self.api = api
        self.api.is_stopped = lambda: self.stopped
        self.api.progress_changed = lambda progress, exc_info: GLib.idle_add(self._progress_changed, progress, exc_info)

    def __unprepare(self):
        del self.api
        del self.readProgressChanged
        del self.stopped
        del self.targetFunc

    def _progress_changed(self, progress, exc_info=None):
        self.realProgressChanged(progress, exc_info)
        if exc_info is not None or progress == 100:
            self.__unprepare()
        return False


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


# mirror-site #################################################################

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

    def progress_changed(progress, exc_info=None):
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
