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
        self._libraryFile = os.path.join(self._tmpDir, "library.xml")
        self._listenIp = param["listen-ip"]
        self._mirrorSiteDict = param["mirror-sites"]

        self._port = None
        self._proc = None
        self._advertisedMirrorSiteIdList = []
        try:
            self._generateLibraryXml()
            self._port = McUtil.getFreeSocketPort("tcp")
            self._proc = self._startProc()
            McUtil.waitSocketPortForProc("tcp", self._listenIp, self._port, self._proc)
            logging.info("Advertiser (kiwix) started, listening on port %d." % (self._port))
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
        McUtil.forceDelete(self._libraryFile)

    def get_access_info(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "url": "http://{IP}:%d/%s" % (self._port, mirror_site_id),
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        self._advertisedMirrorSiteIdList.append(mirror_site_id)

        # restart kiwix-serve
        # ugly, kiwix-serve does not support reload library.xml by signal
        self._proc.terminate()
        self._proc.wait()
        self._generateLibraryXml()
        self._proc = self._startProc()
        McUtil.waitSocketPortForProc("tcp", self._listenIp, self._port, self._proc)

    def _generateLibraryXml(self):
        with open(self._libraryFile, "w") as f:
            f.write("")
        bProcessed = False
        for msId in self._advertisedMirrorSiteIdList:
            buf = McUtil.readFile(os.path.join(self._mirrorSiteDict[msId]["state-directory"], "library.list"))
            for line in buf.split("\n"):
                line = line.strip()
                if line == "" or line.startswith("#"):
                    continue
                McUtil.cmdCall("/usr/bin/kiwix-manage", self._libraryFile, "add", line)
                bProcessed = True
        if not bProcessed:
            # so that the library file is legal
            McUtil.cmdCall("/usr/bin/kiwix-manage", self._libraryFile, "add", "dummy")

    def _startProc(self):
        return subprocess.Popen([
            "/usr/bin/kiwix-serve",
            "--library",
            "--address=%s" % (self._listenIp),
            "--port=%d" % (self._port),
            self._libraryFile,
        ], stderr=subprocess.STDOUT, cwd=self._tmpDir)
