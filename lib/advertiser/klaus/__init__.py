#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
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
        self._wsgiFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "wsgi_autoreloading.py")

        self._tmpDir = param["temp-directory"]
        self._logDir = param["log-directory"]
        self._virtRootDir = os.path.join(self._tmpDir, "vroot")
        self._cfgFn = os.path.join(self._tmpDir, "httpd.conf")
        self._pidFile = os.path.join(self._tmpDir, "httpd.pid")
        self._errorLogFile = os.path.join(self._logDir, "error.log")
        self._accessLogFile = os.path.join(self._logDir, "access.log")
        self._listenIp = param["listen-ip"]
        self._mirrorSiteDict = param["mirror-sites"]

        self._port = None
        self._proc = None
        self._advertisedMirrorSiteIdList = []
        try:
            self._port = McUtil.getFreeSocketPort("tcp")
            self._generateVirtualRootDir()
            self._generateCfgFn()
            self._proc = subprocess.Popen(["/usr/sbin/apache2", "-f", self._cfgFn, "-DFOREGROUND"], cwd=self._virtRootDir)
            McUtil.waitSocketPortForProc("tcp", self._listenIp, self._port, self._proc)
            logging.info("Advertiser (klaus) started, listening on port %d." % (self._port))
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
        McUtil.forceDelete(self._virtRootDir)

    def get_access_info(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "url": "http://{IP}:%d/%s" % (self._port, mirror_site_id),
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        self._advertisedMirrorSiteIdList.append(mirror_site_id)
        self._generateVirtualRootDir()
        self._generateCfgFn()
        os.kill(self._proc.pid, signal.SIGUSR1)

    def _generateVirtualRootDir(self):
        McUtil.ensureDir(self._virtRootDir)

        for msId in self._advertisedMirrorSiteIdList:
            # create new fake directories
            McUtil.ensureDir(os.path.join(self._virtRootDir, msId))

            # create wsgi script
            realPath = self._mirrorSiteDict[msId]["storage-param"]["file"]["data-directory"]
            srcBuf = McUtil.readFile(self._wsgiFile)
            with open(self.__wsgiFn(msId), "w") as f:
                buf = srcBuf
                buf += '\n'
                buf += 'application = make_autoreloading_app("%s", "%s",\n' % (realPath, msId)
                buf += '                                     use_smarthttp=True,\n'
                buf += '                                     disable_push=True)\n'
                f.write(buf)

    def _generateCfgFn(self):
        modulesDir = "/usr/lib64/apache2/modules"
        buf = ""

        buf += "LoadModule log_config_module      %s/mod_log_config.so\n" % (modulesDir)
        buf += "LoadModule unixd_module           %s/mod_unixd.so\n" % (modulesDir)
        buf += "LoadModule alias_module           %s/mod_alias.so\n" % (modulesDir)
        buf += "LoadModule authz_core_module      %s/mod_authz_core.so\n" % (modulesDir)            # it's strange why we need this module and Require directive when we have no auth at all
        buf += "LoadModule autoindex_module       %s/mod_autoindex.so\n" % (modulesDir)
        buf += "LoadModule wsgi_module            %s/mod_wsgi.so\n" % (modulesDir)
        buf += "\n"
        buf += 'PidFile "%s"\n' % (self._pidFile)
        buf += 'ErrorLog "%s"\n' % (self._errorLogFile)
        buf += r'LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" common' + "\n"
        buf += 'CustomLog "%s" common\n' % (self._accessLogFile)
        buf += "\n"
        buf += "Listen %d http\n" % (self._port)
        buf += "\n"
        buf += "ServerName none\n"                          # dummy value
        buf += 'DocumentRoot "%s"\n' % (self._virtRootDir)
        buf += '<Directory "%s">\n' % (self._virtRootDir)
        buf += '    Options Indexes\n'
        buf += '    Require all granted\n'
        buf += '</Directory>\n'
        buf += "\n"
        for msId in self._advertisedMirrorSiteIdList:
            buf += 'WSGIScriptAlias /%s %s\n' % (msId, self.__wsgiFn(msId))
        buf += 'WSGIChunkedRequest On\n'
        buf += "\n"

        # write file atomically
        with open(self._cfgFn + ".tmp", "w") as f:
            f.write(buf)
        os.rename(self._cfgFn + ".tmp", self._cfgFn)

    def __wsgiFn(self, msId):
        return os.path.join(self._tmpDir, "wsgi-%s.py" % (msId))
