#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import imp
import libxml2
from datetime import datetime
from gi.repository import GLib
sys.path.append("/usr/lib64/mycdn")
from mc_util import DynObject


class FakeParam:

    def __init__(self):
        self.etcDir = "/etc/mycdn"
        self.libDir = "/usr/lib64/mycdn"
        self.cacheDir = "/var/cache/mycdn"
        self.runDir = "/run/mycdn"
        self.logDir = "/var/log/mycdn"
        self.tmpDir = "/tmp/mycdn"


def _progress_changed(progress, exc_info=None):
    print("progress %s, %s" % (progress, exc_info))
    if progress == 100 and runtime == "glib-mainloop":
        mainloop.quit()


def createInitApi(param, dataDir, runtime):
    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: dataDir
    api.get_log_dir = lambda: param.logDir
    api.progress_changed = _progress_changed
    return api


def createUpdateApi(param, dataDir, runtime):
    schedDatetime = datetime.now()
    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: dataDir
    api.get_log_dir = lambda: param.logDir
    api.get_sched_datetime = lambda: schedDatetime
    api.progress_changed = _progress_changed
    return api


def loadUpdater(param, mainloop, path, mirrorSiteId):
    # get metadata.xml file
    metadata_file = os.path.join(path, "metadata.xml")
    if not os.path.exists(metadata_file):
        raise Exception("plugin %s has no metadata.xml" % (name))
    if not os.path.isfile(metadata_file):
        raise Exception("metadata.xml for plugin %s is not a file" % (name))
    if not os.access(metadata_file, os.R_OK):
        raise Exception("metadata.xml for plugin %s is invalid" % (name))

    # create Updater object
    root = libxml2.parseFile(metadata_file).getRootElement()
    for child in root.xpathEval(".//mirror-site"):
        dataDir = os.path.join(param.cacheDir, child.xpathEval(".//data-directory")[0].getContent())

        elem = root.xpathEval(".//updater")[0]

        runtime = "glib-mainloop"
        if len(elem.xpathEval(".//runtime")) > 0:
            runtime = elem.xpathEval(".//runtime")[0].getContent()
            assert runtime in ["thread", "process"]

        filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
        classname = elem.xpathEval(".//classname")[0].getContent()
        try:
            f = open(filename)
            m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
            plugin_class = getattr(m, classname)
        except:
            raise Exception("syntax error")

        return dataDir, runtime, plugin_class()

    return None


if len(sys.argv) < 3:
    print("syntax: test-plugin-updater.py <plugin-directory> <mirror-site-id>")
    sys.exit(1)

pluginDir = sys.argv[1]
mirrorSiteId = sys.argv[2]

param = FakeParam()
mainloop = GLib.MainLoop()
dataDir, runtime, updater = loadUpdater(param, mainloop, pluginDir, mirrorSiteId)
if os.path.exists(os.path.join(dataDir, ".uninitialized")):
    print("init start begin")
    api = createInitApi(param, dataDir, runtime)
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
    api = createUpdateApi(param, dataDir, runtime)
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
