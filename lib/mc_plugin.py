#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import json
import libxml2
import threading
from gi.repository import GLib
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
            obj = McPublicMirrorDatabase.createFromPlugin(path, child)
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

    @staticmethod
    def createFromPlugin(pluginDir, rootElem):
        ret = McPublicMirrorDatabase()
        ret.id = rootElem.prop("id")

        ret.dictOfficial = dict()
        ret.dictExtended = dict()
        if True:
            tlist1 = rootElem.xpathEval(".//filename")
            tlist2 = rootElem.xpathEval(".//classname")
            tlist3 = rootElem.xpathEval(".//json-file")
            if tlist1 != [] and tlist2 != []:
                filename = os.path.join(pluginDir, tlist1[0].getContent())
                classname = tlist2[0].getContent()
                dbObj = McUtil.loadObject(filename, classname)
                ret.dictOfficial, ret.dictExtended = dbObj.get_data()
            elif tlist3 != []:
                for e in tlist3:
                    if e.prop("id") == "official":
                        with open(os.path.join(pluginDir, e.getContent())) as f:
                            jobj = json.load(f)
                            ret.dictOfficial.update(jobj)
                            ret.dictExtended.update(jobj)
                    elif e.prop("id") == "extended":
                        with open(os.path.join(pluginDir, e.getContent())) as f:
                            ret.dictExtended.update(json.load(f))
                    else:
                        raise Exception("invalid json-file")
            else:
                raise Exception("invalid metadata")

        return ret

    @staticmethod
    def createFromJson(id, jsonOfficial, jsonExtended):
        ret = McPublicMirrorDatabase()
        ret.id = id
        ret.dictOfficial = json.loads(jsonOfficial)
        ret.dictExtended = json.loads(jsonExtended)
        return ret

    def get(self, extended=False):
        if not extended:
            return self.dictOfficial
        else:
            return self.dictExtended

    def query(self, country=None, location=None, protocolList=None, extended=False, maximum=1):
        assert location is None or (country is not None and location is not None)
        assert protocolList is None or all(x in ["http", "ftp", "rsync"] for x in protocolList)

        # select database
        srcDict = self.dictOfficial if not extended else self.dictExtended

        # country out of scope, we don't consider this condition
        if country is not None:
            if not any(x.get("country", None) == country for x in srcDict.values()):
                country = None
                location = None

        # location out of scope, same as above
        if location is not None:
            if not any(x["country"] == country and x.get("location", None) == location for x in srcDict.values()):
                location = None

        # do query
        ret = []
        for url, prop in srcDict.items():
            if len(ret) >= maximum:
                break
            if country is not None and prop.get("country", None) != country:
                continue
            if location is not None and prop.get("location", None) != location:
                continue
            if protocolList is not None and prop.get("protocol", None) not in protocolList:
                continue
            ret.append(url)
        return ret


class McMirrorSite:

    def __init__(self, param, pluginDir, rootElem):
        self.id = rootElem.prop("id")

        # data directory
        self.dataDir = rootElem.xpathEval(".//data-directory")[0].getContent()
        self.dataDir = os.path.join(McConst.cacheDir, self.dataDir)

        # initializer
        self.initializerObj = None
        if True:
            elem = rootElem.xpathEval(".//initializer")[0]

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
                    self.initializerObj = McUtil.loadObject(filename, classname)
                    break

                if self.runtime == "thread":
                    self.initializerObj = _UpdaterObjProxyRuntimeThread(filename, classname)
                    break

                if self.runtime == "process":
                    self.initializerObj = _UpdaterObjProxyRuntimeProcess(param, filename, classname)
                    break

                assert False

        # updater
        self.updaterObj = None
        self.schedExpr = None
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
                    self.updaterObj = McUtil.loadObject(filename, classname)
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
        self.realUpdaterObj = McUtil.loadObject(filename, classname)

    def start(self, api):
        self.realProgressChanged = api.progress_changed
        self.realErrorOccured = api.error_occured
        self.realErrorOccuredAndHoldFor = api.error_occured_and_hold_for
        self.threadObj = _UpdaterObjProxyRuntimeThreadImpl(self, api, self.realUpdaterObj.init)
        self.threadObj.start()

    def stop(self):
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

    def start(self, api):
        assert False

    def stop(self):
        assert False


# public-mirror-database ######################################################

class TemplatePublicMirrorDatabase:

    def get_data(self):
        assert False


# file-mirror && git-mirror ###################################################

class TemplateMirrorSiteInitializer:

    def start(self, api):
        assert False

    def stop(self):
        assert False


class TemmplateMirrorSiteInitializerApi:

    def get_country(self):
        assert False

    def get_location(self):
        assert False

    def get_data_dir(self):
        assert False

    def get_log_dir(self):
        assert False

    def get_public_mirror_database(self):
        # FIXME, should be changed to get_public_mirror
        assert False

    def progress_changed(self, progress):
        assert False

    def error_occured(self, exc_info):
        assert False

    def error_occured_and_hold_for(self, seconds, exc_info):
        assert False


class TemplateMirrorSiteInitializerRuntimeThread:

    def run(self, api):
        assert False


class TemmplateMirrorSiteInitializerRuntimeThreadApi(TemmplateMirrorSiteInitializerApi):

    def is_stopped(self):
        assert False


class TemplateMirrorSiteInitializerRuntimeProcess:

    def run(self, api):
        assert False


class TemmplateMirrorSiteInitializerRuntimeProcessApi(TemmplateMirrorSiteInitializerApi):
    # stop by SIGTERM signal
    pass


class TemplateMirrorSiteUpdater:

    def start(self, api):
        assert False

    def stop(self):
        assert False


class TemmplateMirrorSiteUpdaterApi(TemmplateMirrorSiteInitializerApi):

    def get_sched_datetime(self):
        assert False


class TemplateMirrorSiteUpdaterRuntimeThread:

    def run(self, api):
        assert False


class TemmplateMirrorSiteUpdaterRuntimeThreadApi(TemmplateMirrorSiteUpdaterApi):

    def is_stopped(self):
        assert False


class TemplateMirrorSiteUpdaterRuntimeProcess:

    def run(self, api):
        assert False


class TemmplateMirrorSiteUpdaterRuntimeProcessApi(TemmplateMirrorSiteUpdaterApi):
    # stop by SIGTERM signal
    pass
