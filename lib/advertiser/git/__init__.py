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
            "storage-dependencies": ["file"],
        }

    def __init__(self, param):
        self._tmpDir = param["temp-directory"]
        self._virtRootDir = os.path.join(self._tmpDir, "advertiser-git-vroot")
        self._listenIp = param["listen-ip"]
        self._mirrorSiteDict = param["mirror-sites"]

        self._port = None
        self._proc = None
        try:
            McUtil.ensureDir(self._virtRootDir)
            self._port = McUtil.getFreeSocketPort("tcp")
            self._proc = subprocess.Popen([
                "/usr/libexec/git-core/git-daemon",
                "--export-all",
                "--listen=%s" % (self._listenIp),
                "--port=%d" % (self._port),
                "--base-path=%s" % (self._virtRootDir),
            ], cwd=self._tmpDir)
            McUtil.waitSocketPortForProc("tcp", self._listenIp, self._port, self._proc)
            logging.info("Advertiser (git) started, listening on port %d." % (self._port))
        except Exception:
            self.dispose()
            raise

    @property
    def port(self):
        return self._port

    def dispose(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        if self._port is not None:
            self._port = None
        McUtil.forceDelete(self._virtRootDir)

    def advertise_mirror_site(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        realPath = self._mirrorSiteDict[mirror_site_id]["storage-param"]["file"]["data-directory"]
        os.symlink(realPath, os.path.join(self._virtRootDir, mirror_site_id))
