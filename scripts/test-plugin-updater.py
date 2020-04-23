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
from mc_util import McUtil
from mc_util import UnixDomainSocketApiServer
from mc_param import McConst


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

    # load elements
    dataDir = os.path.join(McConst.cacheDir, child.xpathEval(".//data-directory")[0].getContent())
    initExec = os.path.join(path, msRoot.xpathEval(".//initializer")[0].xpathEval(".//executable")[0].getContent())
    updateExec = os.path.join(path, msRoot.xpathEval(".//updater")[0].xpathEval(".//executable")[0].getContent())

    return dataDir, initExec, updateExec


class InitOrUpdateProc:

    def __init__(self, execFile, dataDir, debugFlag, bInitOrUpdate):
        assert debugFlag in ["0", "1"]

        cmd = [execFile]

        args = {
            "data-directory": dataDir,
            "debug-flag": "show-ui",
            "country": "CN",
            "location": "",
        }
        if not bInitOrUpdate:
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
        super().__init__(McConst.apiServerFile, self._clientInitFunc, self._clientNoitfyFunc)

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

pluginDir = sys.argv[1]
mirrorSiteId = sys.argv[2]
if len(sys.argv) >= 4:
    debugFlag = sys.argv[3]
    if debugFlag not in ["0", "1"]:
        raise Exception("debug-flag must be 0 or 1")
else:
    debugFlag = "0"

apiServer = None
proc = None
mainloop = GLib.MainLoop()
dataDir, initExec, updateExec = loadInitializerAndUpdater(pluginDir, mirrorSiteId)
initFlagFile = dataDir + ".uninitialized"

McUtil.mkDirAndClear(McConst.runDir)

if not os.path.exists(dataDir):
    os.makedirs(dataDir)
    McUtil.touchFile(initFlagFile)

apiServer = ApiServer(mirrorSiteId)
if os.path.exists(initFlagFile):
    print("init start begin")
    proc = InitOrUpdateProc(initExec, dataDir, debugFlag, True)
    mainloop.run()
    print("init start end, we don't delete the init-flag-file")
else:
    print("update start begin")
    proc = InitOrUpdateProc(updateExec, dataDir, debugFlag, False)
    mainloop.run()
    print("update start end")

proc.dispose()
apiServer.dispose()
shutil.rmtree(McConst.runDir)
