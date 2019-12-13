#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import imp
import libxml2
from datetime import datetime
from gi.repository import GLib
sys.path.append("/usr/lib64/mirrors")
from mc_util import McUtil
from mc_util import DynObject
from mc_param import McConst


class FakeParam:

    def __init__(self):
        self.tmpDir = "/tmp/mirrors"


class FakePublicMirrorDatabase:

    def __init__(self, param, plugin, pluginDir, rootElem):
        self.id = rootElem.prop("id")

        self.dictOfficial = dict()
        self.dictExtended = dict()
        if True:
            tlist1 = rootElem.xpathEval(".//filename")
            tlist2 = rootElem.xpathEval(".//classname")
            tlist3 = rootElem.xpathEval(".//json-file")
            if tlist1 != [] and tlist2 != []:
                filename = os.path.join(pluginDir, tlist1[0].getContent())
                classname = tlist2[0].getContent()
                dbObj = McUtil.loadObject(filename, classname)
                self.dictOfficial, self.dictExtended = dbObj.get_data()
            elif tlist3 != []:
                for e in tlist3:
                    if e.prop("id") == "official":
                        with open(os.path.join(pluginDir, e.getContent())) as f:
                            jobj = json.load(f)
                            self.dictOfficial.update(jobj)
                            self.dictExtended.update(jobj)
                    elif e.prop("id") == "extended":
                        with open(os.path.join(pluginDir, e.getContent())) as f:
                            self.dictExtended.update(json.load(f))
                    else:
                        raise Exception("invalid json-file")
            else:
                raise Exception("invalid metadata")

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


def _progress_changed(progress):
    print("progress %s" % (progress))
    if progress == 100 and runtime == "glib-mainloop":
        mainloop.quit()


def _error_occured(exc_info):
    print("error %s" % (str(exc_info)))
    if runtime == "glib-mainloop":
        mainloop.quit()


def _error_occured_and_hold_for(seconds, exc_info):
    print("error_and_hold_for %d %s" % (seconds, str(exc_info)))
    if runtime == "glib-mainloop":
        mainloop.quit()


def createInitApi(param, db, dataDir, runtime):
    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: dataDir
    api.get_log_dir = lambda: McConst.logDir
    api.get_public_mirror_database = lambda: db
    api.progress_changed = _progress_changed
    api.error_occured = _error_occured
    api.error_occured_and_hold_for = _error_occured_and_hold_for
    return api


def createUpdateApi(param, db, dataDir, runtime):
    schedDatetime = datetime.now()
    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: dataDir
    api.get_log_dir = lambda: McConst.logDir
    api.get_public_mirror_database = lambda: db
    api.get_sched_datetime = lambda: schedDatetime
    api.progress_changed = _progress_changed
    api.error_occured = _error_occured
    api.error_occured_and_hold_for = _error_occured_and_hold_for
    return api


def loadPublicMirrorDatabase(param, mainloop, path, publicMirrorDatabaseId):
                     # get metadata.xml file
    metadata_file = os.path.join(path, "metadata.xml")
    if not os.path.exists(metadata_file):
        raise Exception("plugin %s has no metadata.xml" % (name))
    if not os.path.isfile(metadata_file):
        raise Exception("metadata.xml for plugin %s is not a file" % (name))
    if not os.access(metadata_file, os.R_OK):
        raise Exception("metadata.xml for plugin %s is invalid" % (name))

    root = libxml2.parseFile(metadata_file).getRootElement()

    # create Database object
    for child in root.xpathEval(".//public-mirror-database"):
        obj = McPublicMirrorDatabase(atysaz     param, self, path, child)

        dataDir = os.path.join(McConst.cacheDir, child.xpathEval(".//data-directory")[0].getContent())

        elem = root.xpathEval(".//updater")[0]

        runtime = "glib-mainloop"
        if len(elem.xpathEval(".//runtime")) > 0:
            runtime = elem.xpathEval(".//runtime")[0].getContent()
            assert runtime in ["thread", "process"]

        filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
        classname = elem.xpathEval(".//classname")[0].getContent()
        dbObj = McUtil.loadObject(filename, classname)

        return dataDir, runtime, dbObj



def loadUpdater(param, mainloop, path, mirrorSiteId):
    # get metadata.xml file
    metadata_file = os.path.join(path, "metadata.xml")
    if not os.path.exists(metadata_file):
        raise Exception("plugin %s has no metadata.xml" % (name))
    if not os.path.isfile(metadata_file):
        raise Exception("metadata.xml for plugin %s is not a file" % (name))
    if not os.access(metadata_file, os.R_OK):
        raise Exception("metadata.xml for plugin %s is invalid" % (name))

    root = libxml2.parseFile(metadata_file).getRootElement()

    # create Updater object
    for child in root.xpathEval(".//file-mirror"):
        dataDir = os.path.join(McConst.cacheDir, child.xpathEval(".//data-directory")[0].getContent())

        elem = root.xpathEval(".//updater")[0]

        runtime = "glib-mainloop"
        if len(elem.xpathEval(".//runtime")) > 0:
            runtime = elem.xpathEval(".//runtime")[0].getContent()
            assert runtime in ["thread", "process"]

        filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
        classname = elem.xpathEval(".//classname")[0].getContent()
        updaterObj = McUtil.loadObject(filename, classname)

        return dataDir, runtime, updaterObj

    # create Updater object
    for child in root.xpathEval(".//git-mirror"):
        dataDir = os.path.join(McConst.cacheDir, child.xpathEval(".//data-directory")[0].getContent())

        elem = root.xpathEval(".//updater")[0]

        runtime = "glib-mainloop"
        if len(elem.xpathEval(".//runtime")) > 0:
            runtime = elem.xpathEval(".//runtime")[0].getContent()
            assert runtime in ["thread", "process"]

        filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
        classname = elem.xpathEval(".//classname")[0].getContent()
        updaterObj = McUtil.loadObject(filename, classname)

        return dataDir, runtime, updaterObj

    return None


if len(sys.argv) < 3:
    print("syntax: test-plugin-updater.py <plugin-directory> <file-mirror-id>")
    sys.exit(1)

pluginDir = sys.argv[1]
mirrorSiteId = sys.argv[2]

param = FakeParam()
mainloop = GLib.MainLoop()
db = loadPublicMirrorDatabase(param, mainloop, pluginDir, mirrorSiteId)
dataDir, runtime, updater = loadUpdater(param, mainloop, pluginDir, mirrorSiteId)
initFlagFile = dataDir + ".uninitialized"

if not os.path.exists(dataDir):
    os.makedirs(dataDir)
    McUtil.touchFile(initFlagFile)

if os.path.exists(initFlagFile):
    print("init start begin")
    api = createInitApi(param, db, dataDir, runtime)
    if runtime == "glib-mainloop":
        updater.init_start(api)
    elif runtime == "thread":
        api.is_stopped = lambda: False
        updater.init(api)
    elif runtime == "process":
        updater.init(api)
    else:
        assert False
    print("init start end")
else:
    print("update start begin")
    api = createUpdateApi(param, db, dataDir, runtime)
    if runtime == "glib-mainloop":
        updater.update_start(api)
    elif runtime == "thread":
        api.is_stopped = lambda: False
        updater.update(api)
    elif runtime == "process":
        updater.update(api)
    else:
        assert False
    print("update start end")

mainloop.run()
