#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import imp
sys.path.append("/usr/lib64/mycdn")


class FakeParam:

    def __init__(self):
        self.etcDir = "/etc/mycdn"
        self.libDir = "/usr/lib/mycdn"
        self.pluginsDir = os.path.join(self.libDir, "plugins")
        self.cacheDir = "/var/cache/mycdn"
        self.runDir = "/run/mycdn"
        self.logDir = "/var/log/mycdn"
        self.tmpDir = "/tmp/mycdn"


class FakeUpdaterApi:

    def __init__(self, mainloop, dataDir, logDir):
        self.mainloop = mainloop
        self.dataDir = dataDir
        self.logDir = logDir

    def get_country(self):
        return "CN"

    def get_location(self):
        return None

    def get_data_dir(self):
        return self.dataDir

    def get_log_dir(self):
        return self.logDir

    def notify_progress(self, progress, finished):
        print("progress %s, %d" % (progress, finished))
        if progress == 100:
            mainloop.quit()


def loadUpdater(mainloop, path, mirrorSiteId):
    param = FakeParam()

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
        apiObj = FakeUpdaterApi(dataDir, param.logDir)

        elem = rootElem.xpathEval(".//updater")[0]
        filename = os.path.join(pluginDir, elem.xpathEval(".//filename")[0].getContent())
        classname = elem.xpathEval(".//classname")[0].getContent()
        try:
            f = open(filename)
            m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
            plugin_class = getattr(m, classname)
        except:
            raise Exception("syntax error")
        return plugin_class(apiObj)

    return None


if len(sys.argv) < 3:
    print("syntax: test-plugin-updater.py <plugin-directory> <mirror-site-id>")
    sys.exit(1)

pluginDir = sys.argv[1]
mirrorSiteId = sys.argv[2]

mainloop = GLib.MainLoop()
updater = loadUpdater(mainloop, pluginDir, mirrorSiteId)
if os.path.exists(os.path.join(updater.api.dataDir, ".uninitialized")):
    print("init start begin")
    updater.init_start()
    print("init start end")
else:
    print("update start begin")
    updater.update_start()
    print("update start end")

mainloop.run()
