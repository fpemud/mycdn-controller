#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import time
import socket
import logging
import subprocess


class Advertiser:

    @staticmethod
    def get_properties():
        return {
            "storage-dependencies": ["file", "mariadb"],
        }

    def __init__(self, param):
        self._tmpDir = param["temp-directory"]
        self._cfgFile = os.path.join(self._tmpDir, "advertiser-rsync.conf")
        self._lockFile = os.path.join(self._tmpDir, "advertiser-rsyncd.lock")
        self._logFile = os.path.join(param["log-directory"], "advertiser-rsyncd.log")
        self._listenIp = param["listen-ip"]
        self._mirrorSiteDict = param["mirror-sites"]

        self._port = None
        self._proc = None
        self._advertisedMirrorSiteIdList = []
        try:
            self._port = _Util.getFreeTcpPort()
            self._generateCfgFile()
            self._proc = subprocess.Popen(["/usr/bin/rsync", "-v", "--daemon", "--no-detach", "--config=%s" % (self._cfgFile)], cwd=self._tmpDir)
            _Util.waitTcpServiceForProc(self._listenIp, self._port, self._proc)
            logging.info("Advertiser (rsync) started, listening on port %d." % (self._port))
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


class _Util:

    @staticmethod
    def getFreeTcpPort():
        for port in range(10000, 65536):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.bind((('', port)))
                return port
            except socket.error:
                pass
            finally:
                s.close()
        raise Exception("no valid port")

    @staticmethod
    def waitTcpServiceForProc(ip, port, proc):
        ip = ip.replace(".", "\\.")
        while proc.poll() is None:
            time.sleep(0.1)
            out = _Util.cmdCall("/bin/netstat", "-lant")
            m = re.search("tcp +[0-9]+ +[0-9]+ +(%s:%d) +.*" % (ip, port), out)
            if m is not None:
                return
        raise Exception("process terminated")

    @staticmethod
    def cmdCall(cmd, *kargs):
        # call command to execute backstage job
        #
        # scenario 1, process group receives SIGTERM, SIGINT and SIGHUP:
        #   * callee must auto-terminate, and cause no side-effect
        #   * caller must be terminated by signal, not by detecting child-process failure
        # scenario 2, caller receives SIGTERM, SIGINT, SIGHUP:
        #   * caller is terminated by signal, and NOT notify callee
        #   * callee must auto-terminate, and cause no side-effect, after caller is terminated
        # scenario 3, callee receives SIGTERM, SIGINT, SIGHUP:
        #   * caller detects child-process failure and do appopriate treatment

        ret = subprocess.run([cmd] + list(kargs),
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             universal_newlines=True)
        if ret.returncode > 128:
            # for scenario 1, caller's signal handler has the oppotunity to get executed during sleep
            time.sleep(1.0)
        if ret.returncode != 0:
            print(ret.stdout)
            ret.check_returncode()
        return ret.stdout.rstrip()
