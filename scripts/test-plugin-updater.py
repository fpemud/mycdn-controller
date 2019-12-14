#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import imp
import json
import libxml2
from datetime import datetime
from gi.repository import GLib
sys.path.append("/usr/lib64/mirrors")
from mc_util import McUtil
from mc_util import DynObject
from mc_param import McConst
from mc_plugin import McPublicMirrorDatabase


def _progress_changed(runtime, progress):
    print("progress %s" % (progress))
    if progress == 100 and runtime == "glib-mainloop":
        mainloop.quit()


def _error_occured(runtime, exc_info):
    print("error %s" % (str(exc_info)))
    if runtime == "glib-mainloop":
        mainloop.quit()


def _error_occured_and_hold_for(runtime, seconds, exc_info):
    print("error_and_hold_for %d %s" % (seconds, str(exc_info)))
    if runtime == "glib-mainloop":
        mainloop.quit()


def createInitOrUpdateApi(db, dataDir, runtime, bInitOrUpdate):
    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: dataDir
    api.get_log_dir = lambda: McConst.logDir
    api.get_public_mirror_database = lambda: db
    if not bInitOrUpdate:
        schedDatetime = datetime.now()
        api.get_sched_datetime = lambda: schedDatetime
    api.progress_changed = lambda progress: _progress_changed(runtime, progress)
    api.error_occured = lambda exc_info: _error_occured(runtime, exc_info)
    api.error_occured_and_hold_for = lambda seconds, exc_info: _error_occured_and_hold_for(runtime, seconds, exc_info)
    return api


def loadPublicMirrorDatabase(path, publicMirrorDatabaseId):
    metadata_file = os.path.join(path, "metadata.xml")
    root = libxml2.parseFile(metadata_file).getRootElement()

    # create McPublicMirrorDatabase objects
    for child in root.xpathEval(".//public-mirror-database"):
        if child.prop("id") == publicMirrorDatabaseId:
            return McPublicMirrorDatabase.createFromPlugin(path, child)
    return None


def loadInitializerAndUpdater(path, mirrorSiteId):
    metadata_file = os.path.join(path, "metadata.xml")
    root = libxml2.parseFile(metadata_file).getRootElement()
    msRoot = None

    # find mirror site
    if msRoot is None:
        for child in root.xpathEval(".//file-mirror"):
            if child.prop("id") == mirrorSiteId:
                msRoot = child
                break
    if msRoot is None:
        for child in root.xpathEval(".//git-mirror"):
            if child.prop("id") == mirrorSiteId:
                msRoot = child
                break
    assert msRoot is not None

    # load dataDir
    dataDir = os.path.join(McConst.cacheDir, child.xpathEval(".//data-directory")[0].getContent())

    # load initializer
    initializerRuntime = None
    initializerObj = None
    if True:
        elem = msRoot.xpathEval(".//initializer")[0]

        initializerRuntime = "glib-mainloop"
        if len(elem.xpathEval(".//runtime")) > 0:
            initializerRuntime = elem.xpathEval(".//runtime")[0].getContent()
            assert initializerRuntime in ["glib-mainloop", "thread", "process"]

        filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
        classname = elem.xpathEval(".//classname")[0].getContent()
        initializerObj = McUtil.loadObject(filename, classname)

    # load updater
    updaterRuntime = None
    updaterObj = None
    if True:
        elem = msRoot.xpathEval(".//updater")[0]

        runtime = "glib-mainloop"
        if len(elem.xpathEval(".//runtime")) > 0:
            runtime = elem.xpathEval(".//runtime")[0].getContent()
            assert runtime in ["glib-mainloop", "thread", "process"]

        filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
        classname = elem.xpathEval(".//classname")[0].getContent()
        updaterObj = McUtil.loadObject(filename, classname)

    return dataDir, initializerRuntime, initializerObj, updaterRuntime, updaterObj


if len(sys.argv) < 3:
    print("syntax: test-plugin-updater.py <plugin-directory> <mirror-site-id>")
    sys.exit(1)

pluginDir = sys.argv[1]
mirrorSiteId = sys.argv[2]

mainloop = GLib.MainLoop()
db = loadPublicMirrorDatabase(pluginDir, mirrorSiteId)
dataDir, initRuntime, initerObj, updaterRuntime, updaterObj = loadInitializerAndUpdater(pluginDir, mirrorSiteId)
initFlagFile = dataDir + ".uninitialized"

if not os.path.exists(dataDir):
    os.makedirs(dataDir)
    McUtil.touchFile(initFlagFile)

if os.path.exists(initFlagFile):
    print("init start begin")
    api = createInitOrUpdateApi(db, dataDir, initRuntime, True)
    if initRuntime == "glib-mainloop":
        initerObj.start(api)
    elif initRuntime == "thread":
        api.is_stopped = lambda: False
        initerObj.run(api)
    elif initRuntime == "process":
        initerObj.run(api)
    else:
        assert False
    print("init start end")
else:
    print("update start begin")
    api = createInitOrUpdateApi(db, dataDir, updaterRuntime, False)
    if updaterRuntime == "glib-mainloop":
        updaterObj.start(api)
    elif updaterRuntime == "thread":
        api.is_stopped = lambda: False
        updaterObj.run(api)
    elif updaterRuntime == "process":
        updaterObj.run(api)
    else:
        assert False
    print("update start end")

mainloop.run()
