#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import logging
import subprocess
from mc_util import McUtil


class Advertiser:

    @staticmethod
    def get_properties():
        return {
            "storage-dependencies": ["file", "mariadb"],
        }

    def __init__(self, param):
        self._tmpDir = param["temp-directory"]
        self._cfgFile = os.path.join(self._tmpDir, "rsync.conf")
        self._lockFile = os.path.join(self._tmpDir, "rsyncd.lock")
        self._logFile = os.path.join(param["log-directory"], "rsyncd.log")
        self._listenIp = param["listen-ip"]
        self._mirrorSiteDict = param["mirror-sites"]

        self._port = None
        self._proc = None
        self._advertisedMirrorSiteIdList = []
        try:
            self._port = McUtil.getFreeSocketPort("tcp")
            self._generateCfgFile()
            self._proc = subprocess.Popen(["/usr/bin/rsync", "-v", "--daemon", "--no-detach", "--config=%s" % (self._cfgFile)], cwd=self._tmpDir)
            McUtil.waitSocketPortForProc("tcp", self._listenIp, self._port, self._proc)
            logging.info("Advertiser (rsync) started, listening on port %d." % (self._port))
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
            "url": "rsync://{IP}:%d/%s" % (self._port, mirror_site_id),
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        self._advertisedMirrorSiteIdList.append(mirror_site_id)
        self._generateCfgFile()             # rsync picks the new cfg-file when new connection comes in

    def _generateCfgFile(self):
        # generate file content
        buf = ""
        buf += "lock file = %s\n" % (self._lockFile)
        buf += "log file = %s\n" % (self._logFile)
        buf += "\n"
        buf += "port = %s\n" % (self._port)
        buf += "timeout = 600\n"
        buf += "\n"
        buf += "use chroot = no\n"      # we are not running rsyncd using the root user
        buf += "\n"
        for msId in self._advertisedMirrorSiteIdList:
            buf += "[%s]\n" % (msId)
            buf += "path = %s\n" % (self._mirrorSiteDict[msId]["storage-param"]["file"]["data-directory"])
            buf += "read only = yes\n"
            buf += "\n"

        # write file atomically
        with open(self._cfgFile + ".tmp", "w") as f:
            f.write(buf)
        os.rename(self._cfgFile + ".tmp", self._cfgFile)
