#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import json
import signal
import logging
import subprocess
from mc_util import McUtil


class Advertiser:

    @staticmethod
    def get_properties():
        return {
            "storage-dependencies": ["file"],
        }

    def __init__(self, param):
        self._execFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "ftpd.py")
        self._tmpDir = param["temp-directory"]
        self._logFileSize = param["log-file-size"]
        self._logFileCount = param["log-file-count"]
        self._cfgFile = os.path.join(self._tmpDir, "advertiser-ftp.cfg")
        self._logFile = os.path.join(param["log-directory"], "advertiser-ftp.log")
        self._listenIp = param["listen-ip"]
        self._mirrorSiteDict = param["mirror-sites"]

        self._port = None
        self._proc = None
        self._advertisedMirrorSiteIdList = []
        try:
            self._port = McUtil.getFreeSocketPort("tcp")
            self._generateCfgFile()
            self._proc = subprocess.Popen([self._execFile, self._cfgFile], cwd=self._tmpDir)
            McUtil.waitSocketPortForProc("tcp", self._listenIp, self._port, self._proc)
            logging.info("Advertiser (ftp) started, listening on port %d." % (self._port))
        except Exception:
            self.dispose()
            raise

    def dispose(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        if self._port is not None:
            self._port = None

    def get_access_info(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "url": "ftp://{IP}:%d/%s" % (self._port, mirror_site_id),
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        self._advertisedMirrorSiteIdList.append(mirror_site_id)
        self._generateCfgFile()
        os.kill(self._proc.pid, signal.SIGUSR1)

    def _generateCfgFile(self):
        # generate file content
        dataObj = dict()
        dataObj["logFile"] = self._logFile
        dataObj["logMaxBytes"] = self._logFileSize
        dataObj["logBackupCount"] = self._logFileCount
        dataObj["ip"] = self._listenIp
        dataObj["port"] = self._port
        dataObj["dirmap"] = {x: self._mirrorSiteDict[x]["storage-param"]["file"]["data-directory"] for x in self._advertisedMirrorSiteIdList}

        # write file atomically
        with open(self._cfgFile + ".tmp", "w") as f:
            json.dump(dataObj, f)
        os.rename(self._cfgFile + ".tmp", self._cfgFile)
