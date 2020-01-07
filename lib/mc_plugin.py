#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import json
import signal
import pickle
import libxml2
import logging
import threading
from datetime import datetime
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
            assert obj.id not in self.param.mirrorSiteDict
            self.param.mirrorSiteDict[obj.id] = obj

        # create McMirrorSite objects, use file-mirror and git-mirror as the same yet
        for child in root.xpathEval(".//git-mirror"):
            obj = McMirrorSite(self.param, path, child)
            assert obj.id not in self.param.mirrorSiteDict
            self.param.mirrorSiteDict[obj.id] = obj

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
                    if e.prop("type") == "official":
                        with open(os.path.join(pluginDir, e.getContent())) as f:
                            jobj = json.load(f)
                            ret.dictOfficial.update(jobj)
                            ret.dictExtended.update(jobj)
                    elif e.prop("type") == "extended":
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

    def query(self, country=None, location=None, protocolList=None, extended=False):
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

            # we have grown up, so stop support other runtime any more
            # runtime = elem.xpathEval(".//runtime")[0].getContent()
            runtime = "process"

            filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
            classname = elem.xpathEval(".//classname")[0].getContent()

            if runtime == "glib-mainloop":
                self.initializerObj = McUtil.loadObject(filename, classname)
            elif runtime == "thread":
                self.initializerObj = _UpdaterObjProxyRuntimeThread(filename, classname)
            elif runtime == "process":
                self.initializerObj = _UpdaterObjProxyRuntimeProcess(self.id, True, filename, classname)
            else:
                assert False

        # updater
        self.updaterObj = None
        self.schedExpr = None
        if True:
            elem = rootElem.xpathEval(".//updater")[0]

            self.schedExpr = elem.xpathEval(".//cron-expression")[0].getContent()           # FIXME: add check

            # we have grown up, so stop support other runtime any more
            # runtime = elem.xpathEval(".//runtime")[0].getContent()
            runtime = "process"

            filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
            classname = elem.xpathEval(".//classname")[0].getContent()

            if runtime == "glib-mainloop":
                self.updaterObj = McUtil.loadObject(filename, classname)
            elif runtime == "thread":
                self.updaterObj = _UpdaterObjProxyRuntimeThread(filename, classname)
            elif runtime == "process":
                self.updaterObj = _UpdaterObjProxyRuntimeProcess(self.id, False, filename, classname)

        # advertiser
        self.advertiseProtocolList = []
        for child in rootElem.xpathEval(".//advertiser")[0].xpathEval(".//protocol"):
            self.advertiseProtocolList.append(child.getContent())


class _UpdaterObjProxyRuntimeThread:

    def __init__(self, filename, classname):
        self.threadObj = None
        self.realPrintInfo = None
        self.realPrintError = None
        self.realProgressChanged = None
        self.realErrorOccured = None
        self.realErrorOccuredAndHoldFor = None
        self.realUpdaterObj = McUtil.loadObject(filename, classname)

    def start(self, api):
        self.realPrintInfo = api.print_info
        self.realPrintError = api.print_error
        self.realProgressChanged = api.progress_changed
        self.realErrorOccured = api.error_occured
        self.realErrorOccuredAndHoldFor = api.error_occured_and_hold_for
        self.threadObj = _UpdaterObjProxyRuntimeThreadImpl(self, api, self.realUpdaterObj.run)
        self.threadObj.start()

    def stop(self):
        self.threadObj.stopped = True

    def _printInfoIdleHandler(self, message):
        self.realPrintInfo(message)
        return False

    def _printErrorIdleHandler(self, message):
        self.realPrintError(message)
        return False

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
        self.api.print_info = self._printInfo
        self.api.print_error = self._printError
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

    def _printInfo(self, message):
        GLib.idle_add(self.parent._printInfoIdleHandler, message)

    def _printError(self, message):
        GLib.idle_add(self.parent._printErrorIdleHandler, message)

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

    def __init__(self, mirrorSiteId, bInitOrUpdate, filename, classname):
        self._flagError = GLib.IO_PRI | GLib.IO_ERR | GLib.IO_HUP | GLib.IO_NVAL

        self.mirrorSiteId = mirrorSiteId
        self.bInitOrUpdate = bInitOrUpdate
        self.filename = filename
        self.classname = classname

        self.api = None
        self.pid = None
        self.stdin = None
        self.stdout = None
        self.stderr = None
        self.pidWatch = None
        self.stdoutWatch = None
        self.stderrWatch = None

    def start(self, api):
        try:
            self.api = api

            ret = GLib.spawn_async_with_pipes(None,                                         # working_directory
                                              [
                                                  McConst.updaterExe,
                                                  self.mirrorSiteId,
                                                  "init" if self.bInitOrUpdate else "update",
                                              ],
                                              None,                                         # envp
                                              GLib.SpawnFlags.DO_NOT_REAP_CHILD)
            assert ret[0]
            self.pid = ret[1]
            self.stdin = os.fdopen(ret[2], "w")
            self.stdout = os.fdopen(ret[3], "rb")
            self.stderr = os.fdopen(ret[4], "r")

            self.pidWatch = GLib.child_watch_add(self.pid, self.onExit)
            self.stdoutWatch = GLib.io_add_watch(self.stdout, GLib.IO_IN | self._flagError, self.onStdout)
            self.stderrWatch = GLib.io_add_watch(self.stderr, GLib.IO_IN | self._flagError, self.onStderr)

            self._writeTo(McConst.tmpDir)
            self._writeTo(self.api.get_data_dir())

            self._writeTo(self.filename)
            self._writeTo(self.classname)

            if not self.bInitOrUpdate:
                self._writeTo(datetime.strftime(self.api.get_sched_datetime(), "%Y-%m-%d %H:%M"))
        except:
            if self.pid is not None:
                try:
                    os.kill(self.pid, signal.SIGTERM)
                    os.waitpid(self.pid, 0)
                except ProcessLookupError:
                    pass                        # process already exited
            self._partiallyClear()
            self.api = None
            raise

    def stop(self):
        if self.pid is not None:
            try:
                os.kill(self.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass                            # process already exited

    def onStdout(self, source, cb_condition):
        print("onStdout")
        line = self.stdout.readline()
        obj = pickle.loads(line)
        if obj[0] == "print-info":
            print(obj[1])
            return True
        elif obj[0] == "progress":
            progress = obj[1]
            self.api.progress_changed(progress)
            if progress == 100:
                self.api = None
            return True
        elif obj[0] == "error":
            self.api.error_occured(obj[1])
            self.api = None
            return True
        elif obj[0] == "error-and-hold-for":
            self.api.error_occured_and_hold_for(obj[1], obj[2])
            self.api = None
            return True
        else:
            assert False

    def onStderr(self, source, cb_condition):
        print("onStderr")
        logging.error(self.stderr.read())
        return True

    def onExit(self, status, data):
        self._partiallyClear()
        if self.api is not None:
            if status == 0:
                self.api.progress_changed(100)
            else:
                exc_info = (None, None, None)               # FIXME
                self.api.error_occured(exc_info)
            self.api = None
        return True

    def _writeTo(self, s):
        self.stdin.write(s)
        self.stdin.write("\n")
        self.stdin.flush()

    def _partiallyClear(self):
        # 1. this method should be called after self.pid exit
        # 2. self.api is not cleared

        if self.pidWatch is not None:
            GLib.source_remove(self.pidWatch)
            self.pidWatch = None

        if self.stderrWatch is not None:
            GLib.source_remove(self.stderrWatch)
            self.stderrWatch = None

        if self.stdoutWatch is not None:
            GLib.source_remove(self.stdoutWatch)
            self.stdoutWatch = None

        # no close needed
        self.stdin = None
        self.stdout = None
        self.stderr = None

        if self.pid is not None:
            GLib.spawn_close_pid(self.pid)
            self.pid = None


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
-

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
