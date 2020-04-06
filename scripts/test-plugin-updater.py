#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import imp
import json
import libxml2
import subprocess
from datetime import datetime
from gi.repository import GLib
sys.path.append("/usr/lib64/mirrors")
from mc_util import McUtil
from mc_util import DynObject
from mc_param import McConst
from mc_plugin import McPublicMirrorDatabase


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
    initExec = msRoot.xpathEval(".//initializer")[0].xpathEval(".//executable")[0].getContent()
    updateExec = msRoot.xpathEval(".//updater")[0].xpathEval(".//executable")[0].getContent()

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
        pid = None
        if True:
            pattern = "=iii"
            length = struct.calcsize(pattern)
            ret = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, length)
            pid, uid, gid = struct.unpack(pattern, ret)

        for mirrorId, obj in self.updaterDict.items():
            if obj.proc is not None and obj.proc.pid == pid:
                return mirrorId
        return None

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

mainloop = GLib.MainLoop()
dataDir, initExec, updateExec = loadInitializerAndUpdater(pluginDir, mirrorSiteId)
initFlagFile = dataDir + ".uninitialized"

if not os.path.exists(dataDir):
    os.makedirs(dataDir)
    McUtil.touchFile(initFlagFile)

if os.path.exists(initFlagFile):
    print("init start begin")
    proc = createInitOrUpdateProc(initExec, updateExec, True)
    print("init start end")
else:
    print("update start begin")
    proc = createInitOrUpdateProc(initExec, updateExec, False)
    print("update start end")

mainloop.run()
