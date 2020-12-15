#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import json
import shutil
import subprocess
import lxml.etree
from datetime import datetime
from gi.repository import GLib
sys.path.append("/usr/lib64/mirrors")
from mc_util import McUtil
from mc_util import UnixDomainSocketApiServer
from mc_param import McConst
from mc_updater import _UpdateHistory


class MirrorSite:

    def __init__(self, pluginId, path, mirrorSiteId):
        metadata_file = os.path.join(path, "metadata.xml")
        root = lxml.etree.parse(metadata_file)

        # find mirror site
        rootElem = None
        for child in root.xpath(".//mirror-site"):
            if child.get("id") == mirrorSiteId:
                rootElem = child
                break
        assert rootElem is not None

        # read config from current directory
        cfgFile = pluginId + ".conf"
        cfg = dict()
        if os.path.exists(cfgFile):
            with open(cfgFile) as f:
                cfg = json.load(f)

        # load elements
        self.id = mirrorSiteId
        self.cfgDict = cfg
        self.masterDir = os.path.join(McConst.cacheDir, mirrorSiteId)
        self.pluginStateDir = os.path.join(self.masterDir, "state")

        # executables
        if len(rootElem.xpath(".//initializer")) > 0:
            self.initExec = os.path.join(path, rootElem.xpath(".//initializer")[0].xpath(".//executable")[0].text)
        else:
            self.initExec = None
        if len(rootElem.xpath(".//updater")) > 0:
            self.updateExec = os.path.join(path, rootElem.xpath(".//updater")[0].xpath(".//executable")[0].text)
        else:
            self.updateExec = None

        # storage
        self.storageDict = dict()                       # {name:storage-object}
        for child in rootElem.xpath(".//storage"):
            st = child.get("type")
            configXml = lxml.etree.tostring(child, encoding="unicode")
            dataDir = os.path.join(self.masterDir, "storage-%s" % (st))
            self.storageDict[st] = self._loadOneStorageObject(st, self.id, configXml, dataDir)
            McUtil.ensureDir(dataDir)

    def _loadOneStorageObject(self, name, msId, configXml, dataDir):
        mod = __import__("storage.%s" % (name))
        mod = getattr(mod, name)

        # prepare storage initialization parameter
        param = {
            "mirror-sites": dict(),
        }
        if mod.Storage.get_properties().get("with-integrated-advertiser", False):
            param.update({
                "listen-ip": "0.0.0.0",
                "temp-directory": McConst.tmpDir,
                "log-directory": McConst.logDir,
            })
        param["mirror-sites"][msId] = {
            "plugin-directory": "",
            "state-directory": self.pluginStateDir,
            "data-directory": dataDir,
            "config-xml": configXml,
        }

        # create object
        return mod.Storage(param)


class InitOrUpdateProc:

    def __init__(self, pluginId, msObj, debugFlag, bInitOrUpdate):
        if bInitOrUpdate:
            cmd = [msObj.initExec]
        else:
            cmd = [msObj.updateExec]

        # create log directory
        logDir = os.path.join(McConst.logDir, msObj.id)
        McUtil.ensureDir(logDir)

        args = {
            "id": msObj.id,
            "config": msObj.cfgDict,
            "state-directory": msObj.pluginStateDir,
            "log-directory": logDir,
            "debug-flag": debugFlag,
            "country": "CN",
            "location": "",
        }
        for storageName, storageObj in msObj.storageDict.items():
            args["storage-" + storageName] = storageObj.get_param(msObj.id)
        if bInitOrUpdate:
            args["run-mode"] = "init"
        else:
            args["run-mode"] = "update"
            args["sched-datetime"] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")
        cmd.append(json.dumps(args))

        self.proc = subprocess.Popen(cmd, universal_newlines=True)
        self.pidWatch = GLib.child_watch_add(self.proc.pid, self._exitCallback)

    def dispose(self):
        if self.proc is not None:
            self.proc.terminate()
            self.proc.wait()
            self.proc = None

    def _exitCallback(self, status, data):
        mainloop.quit()
        self.proc = None


class ApiServer(UnixDomainSocketApiServer):

    def __init__(self, mirrorSiteId):
        self.mirrorSiteId = mirrorSiteId
        super().__init__(McConst.apiServerFile, self._clientInitFunc, None, self._clientNoitfyFunc)

    def _clientInitFunc(self, sock):
        return self.mirrorSiteId

    def _clientNoitfyFunc(self, mirrorId, data):
        if data["message"] == "progress":
            progress = data["data"]["progress"]
            print("progress %s" % (progress))
            if progress == 100:
                mainloop.quit()
        elif data["message"] == "error":
            print("error %s" % (data["data"]["exc_info"]))
            mainloop.quit()
        elif data["message"] == "error-and-hold-for":
            print("error_and_hold_for %d %s" % (data["data"]["seconds"], data["data"]["exc_info"]))
            mainloop.quit()
        else:
            assert False


if len(sys.argv) < 3:
    print("syntax: test-plugin-updater.py <plugin-directory> <mirror-site-id> [debug-flag]")
    sys.exit(1)

# parameters
pluginDir = sys.argv[1]
pluginId = os.path.basename(pluginDir)
mirrorSiteId = sys.argv[2]
if len(sys.argv) >= 4:
    debugFlag = sys.argv[3]
else:
    debugFlag = ""

# global objects
apiServer = None
proc = None
mainloop = GLib.MainLoop()
msObj = MirrorSite(pluginId, pluginDir, mirrorSiteId)

# directories
McUtil.mkDirAndClear(McConst.runDir)
if not os.path.exists(msObj.masterDir):
    os.makedirs(msObj.masterDir)

# do test
updateHistory = _UpdateHistory(os.path.join(msObj.masterDir, "UPDATE_HISTORY"), msObj.initExec is not None)
apiServer = ApiServer(mirrorSiteId)
if not updateHistory.isInitialized():
    print("init start begin")
    proc = InitOrUpdateProc(pluginId, msObj, debugFlag, True)
    mainloop.run()
    print("init start end (we don't save to UPDATE_HISTORY file)")
else:
    print("update start begin")
    proc = InitOrUpdateProc(pluginId, msObj, debugFlag, False)
    mainloop.run()
    print("update start end (we don't save to UPDATE_HISTORY file)")

# dispose
proc.dispose()
apiServer.dispose()
shutil.rmtree(McConst.runDir)
