#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
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


def createInitOrUpdateProc(execFile, dataDir, bInitOrUpdate):
    cmd = [
        execFile,
        dataDir,
        McConst.logDir,
        "CN",
        "",
    ]
    if not bInitOrUpdate:
        cmd.append(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M"))
    return subprocess.Popen(cmd)


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
    print("syntax: test-plugin-updater.py <plugin-directory> <mirror-site-id>")
    sys.exit(1)

pluginDir = sys.argv[1]
mirrorSiteId = sys.argv[2]

apiServer = None
mainloop = GLib.MainLoop()
dataDir, initExec, updateExec = loadInitializerAndUpdater(pluginDir, mirrorSiteId)
initFlagFile = dataDir + ".uninitialized"

McUtil.mkDirAndClear(McConst.runDir)
apiServer = ApiServer(mirrorSiteId)

if not os.path.exists(dataDir):
    os.makedirs(dataDir)
    McUtil.touchFile(initFlagFile)

if os.path.exists(initFlagFile):
    print("init start begin")
    proc = createInitOrUpdateProc(initExec, dataDir, True)
    print("init start end")
else:
    print("update start begin")
    proc = createInitOrUpdateProc(updateExec, dataDir, False)
    print("update start end")

mainloop.run()
apiServer.dispose()
shutil.rmtree(McConst.runDir)
