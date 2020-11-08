#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import json
import struct
import socket
import shutil
import libxml2
import subprocess
from datetime import datetime
from gi.repository import GLib
sys.path.append("/usr/lib64/mirrors")
from mc_util import DynObject
from mc_util import McUtil
from mc_util import UnixDomainSocketApiServer
from mc_param import McConst


class MirrorSite:

    def __init__(self, pluginId, path, mirrorSiteId):
        metadata_file = os.path.join(path, "metadata.xml")
        root = libxml2.parseFile(metadata_file).getRootElement()
        rootElem = None

        # find mirror site
        if rootElem is None:
            for child in root.xpathEval(".//mirror-site"):
                if child.prop("id") == mirrorSiteId:
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
        self.initExec = os.path.join(path, rootElem.xpathEval(".//initializer")[0].xpathEval(".//executable")[0].getContent())
        self.updateExec = os.path.join(path, rootElem.xpathEval(".//updater")[0].xpathEval(".//executable")[0].getContent())

        # storage
        self.storageDict = dict()
        for child in rootElem.xpathEval(".//storage"):
            st = child.prop("type")
            if st not in ["file", "git", "mariadb"]:
                raise Exception("mirror site %s: invalid storage type %s" % (self.id, st))

            self.storageDict[st] = DynObject()
            self.storageDict[st].dataDir = os.path.join(self.masterDir, "storage-" + st)
            self.storageDict[st].pluginParam = {"data-directory": self.storageDict[st].dataDir}
            McUtil.ensureDir(self.storageDict[st].dataDir)

            if st == "mariadb":
                self.storageDict[st].tableInfo = OrderedDict()
                tl = child.xpathEval(".//database-schema")
                if len(tl) > 0:
                    databaseSchemaFile = os.path.join(pluginDir, tl[0].getContent())
                    for sql in sqlparse.split(McUtil.readFile(databaseSchemaFile)):
                        m = re.match("^CREATE +TABLE +(\\S+)", sql)
                        if m is None:
                            raise Exception("mirror site %s: invalid database schema for storage type %s" % (self.id, st))
                        self.storageDict[st].tableInfo[m.group(1)] = (-1, sql)


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
            args["storage-" + storageName] = storageObj.pluginParam
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
initFlagFile = msObj.masterDir + ".uninitialized"

# directories
McUtil.mkDirAndClear(McConst.runDir)
if not os.path.exists(msObj.masterDir):
    os.makedirs(msObj.masterDir)
    McUtil.touchFile(initFlagFile)

# do test
apiServer = ApiServer(mirrorSiteId)
if os.path.exists(initFlagFile):
    print("init start begin")
    proc = InitOrUpdateProc(pluginId, msObj, debugFlag, True)
    mainloop.run()
    print("init start end, we don't delete the init-flag-file")
else:
    print("update start begin")
    proc = InitOrUpdateProc(pluginId, msObj, debugFlag, False)
    mainloop.run()
    print("update start end")

# dispose
proc.dispose()
apiServer.dispose()
shutil.rmtree(McConst.runDir)
