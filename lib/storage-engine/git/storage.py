#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import json
import time
import shutil
import signal
import socket
import logging
import subprocess


class StorageEngine:

    def __init__(self, listenIp, tmpDir, logDir):
        self._listenIp = listenIp
        self._tmpDir = tmpDir
        self._logDir = logDir

        self._bStart = False
        self._httpServer = None
        self._gitServer = None

        self._mirrorSiteDict = dict()

    def addMirrorSite(self, mirrorSiteId, masterDir, xmlElem):
        assert not self._bStart

        msParam = _Param()
        msParam.masterDir = masterDir
        msParam.dataDir = os.path.join(masterDir, "storage-git")
        if len(xmlElem.xpathEval("./advertise-protocols")) > 0:
            for proto in xmlElem.xpathEval("./advertise-protocols")[-1].text.split(":"):
                if proto not in ["http", "git"]:
                    raise Exception("invalid protocol %s" % (proto))
                msParam.advertiseProtocolList.append(proto)
        self._mirrorSiteDict[mirrorSiteId] = msParam

    def start(self):
        assert not self._bStart

        for msId, msParam in self._mirrorSiteDict.items():
            _Util.ensureDir(self._mirrorSiteDict[msId].dataDir)
            if "http" in self._mirrorSiteDict[msId].advertiseProtocolList:
                if self._httpServer is None:
                    self._httpServer = _HttpServer(self._listenIp, self._tmpDir, self._logDir)
                    self._httpServer.start()
            if "git" in self._mirrorSiteDict[msId].advertiseProtocolList:
                if self._gitServer is None:
                    self._gitServer = _GitServer(self._listenIp, self._tmpDir, self._logDir)
                    self._gitServer.start()
        self._bStart = True

    def stop(self):
        assert self._bStart

        self._bStart = False
        if self._gitServer is not None:
            self._gitServer.stop()
            self._gitServer = None
        if self._httpServer is not None:
            self._httpServer.stop()
            self._httpServer = None

    def getPluginParam(self, mirrorSiteId):
        assert self._bStart

        return {
            "data-directory": self._mirrorSiteDict[mirrorSiteId].dataDir,
        }

    def advertiseMirrorSite(self, mirrorSiteId):
        assert self._bStart

        if "http" in self._mirrorSiteDict[mirrorSiteId].advertiseProtocolList:
            assert self._httpServer is not None
            self._httpServer.addDir(mirrorSiteId, self._mirrorSiteDict[mirrorSiteId]["data-directory"])
        if "git" in self._mirrorSiteDict[mirrorSiteId].advertiseProtocolList:
            assert self._gitServer is not None
            self._gitServer.addDir(mirrorSiteId, self._mirrorSiteDict[mirrorSiteId]["data-directory"])


class _Param:

    def __init__(self):
        self.masterDir = None
        self.dataDir = None
        self.advertiseProtocolList = []


class _HttpServer:

    def __init__(self, listenIp, tmpDir, logDir):
        self._virtRootDir = os.path.join(tmpDir, "git-httpd")
        self._cfgFn = os.path.join(tmpDir, "git-httpd.conf")
        self._pidFile = os.path.join(tmpDir, "git-httpd.pid")
        self._errorLogFile = os.path.join(logDir, "git-httpd-error.log")
        self._accessLogFile = os.path.join(logDir, "git-httpd-access.log")

        self._dirDict = dict()

        self._listenIp = listenIp
        self._port = None
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def start(self):
        assert self._proc is None
        self._port = _Util.getFreeTcpPort()
        self._generateVirtualRootDir()
        self._generateCfgFn()
        self._proc = subprocess.Popen(["/usr/sbin/apache2", "-f", self._cfgFn, "-DFOREGROUND"])
        _Util.waitTcpServiceForProc(self._listenIp, self._port, self._proc)
        logging.info("Slave server (http-git) started, listening on port %d." % (self._port))

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        if self._port is not None:
            self._port = None
        _Util.forceDelete(self._virtRootDir)

    def addDir(self, name, realPath):
        assert self._proc is not None
        assert _Util.checkNameAndRealPath(self._dirDict, name, realPath)
        self._dirDict[name] = realPath
        self._generateVirtualRootDir()
        self._generateCfgFn()
        os.kill(self._proc.pid, signal.SIGUSR1)

    def _generateVirtualRootDir(self):
        _Util.ensureDir(self._virtRootDir)

        # create new directories
        for name, realPath in self._dirDict.items():
            dn = os.path.join(self._virtRootDir, name)
            if not os.path.exists(dn):
                os.symlink(realPath, dn)

    def _generateCfgFn(self):
        modulesDir = "/usr/lib64/apache2/modules"
        buf = ""

        # modules
        buf += "LoadModule log_config_module      %s/mod_log_config.so\n" % (modulesDir)
        buf += "LoadModule unixd_module           %s/mod_unixd.so\n" % (modulesDir)
        buf += "LoadModule alias_module           %s/mod_alias.so\n" % (modulesDir)
        buf += "LoadModule authz_core_module      %s/mod_authz_core.so\n" % (modulesDir)            # it's strange why we need this module and Require directive since we have no auth at all
        buf += "LoadModule autoindex_module       %s/mod_autoindex.so\n" % (modulesDir)
        # buf += "LoadModule env_module             %s/mod_env.so\n" % (modulesDir)
        # buf += "LoadModule cgi_module             %s/mod_cgi.so\n" % (modulesDir)
        buf += "\n"

        # global settings
        buf += 'PidFile "%s"\n' % (self._pidFile)
        buf += 'ErrorLog "%s"\n' % (self._errorLogFile)
        buf += r'LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" common' + "\n"
        buf += 'CustomLog "%s" common\n' % (self._accessLogFile)
        buf += "\n"
        buf += "Listen %d http\n" % (self._port)
        buf += "ServerName none\n"                              # dummy value
        buf += "\n"
        buf += 'DocumentRoot "%s"\n' % (self._virtRootDir)
        # buf += "SetEnv GIT_PROJECT_ROOT \"${REPO_ROOT_DIR}\""
        # buf += "SetEnv GIT_HTTP_EXPORT_ALL"
        # buf += ""
        # buf += "  AliasMatch ^/(.*/objects/[0-9a-f]{2}/[0-9a-f]{38})$          \"${REPO_ROOT_DIR}/\$1\""
        # buf += "  AliasMatch ^/(.*/objects/pack/pack-[0-9a-f]{40}.(pack|idx))$ \"${REPO_ROOT_DIR}/\$1\""
        # buf += ""
        # buf += "  ScriptAlias / /usr/libexec/git-core/git-http-backend/"
        # buf += ""
        # buf += "  <Directory \"${REPO_ROOT_DIR}\">"
        # buf += "    AllowOverride None"
        # buf += "  </Directory>"

        # write file atomically
        with open(self._cfgFn + ".tmp", "w") as f:
            f.write(buf)
        os.rename(self._cfgFn + ".tmp", self._cfgFn)


class _GitServer:

    def __init__(self, listenIp, tmpDir, logDir):
        self._virtRootDir = os.path.join(tmpDir, "vroot-git-daemon")

        self._listenIp = listenIp
        self._port = None
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def start(self):
        assert self._proc is None

        McUtil.ensureDir(self._virtRootDir)
        self._port = _Util.getFreeTcpPort()
        self._proc = subprocess.Popen([
            "/usr/libexec/git-core/git-daemon",
            "--export-all",
            "--listen=%s" % (self.param.listenIp),
            "--port=%d" % (self._port),
            "--base-path=%s" % (self._virtRootDir),
        ])
        _Util.waitTcpServiceForProc(self.param.listenIp, self._port, self._proc)
        logging.info("Slave server (git) started, listening on port %d." % (self._port))

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        if self._port is not None:
            self._port = None
        _Util.forceDelete(self._virtRootDir)

    def addDir(self, name, realPath):
        assert self._proc is not None
        assert _checkNameAndRealPath(self._dirDict, name, realPath)
        os.symlink(realPath, os.path.join(self._virtRootDirFile, name))


class _Util:

    @staticmethod
    def ensureDir(dirname):
        if not os.path.exists(dirname):
            os.makedirs(dirname)

    @staticmethod
    def forceDelete(filename):
        if os.path.islink(filename):
            os.remove(filename)
        elif os.path.isfile(filename):
            os.remove(filename)
        elif os.path.isdir(filename):
            shutil.rmtree(filename)

    @staticmethod
    def checkNameAndRealPath(dictObj, name, realPath):
        if name in dictObj:
            return False
        if not os.path.isabs(realPath) or realPath.endswith("/"):
            return False
        if _Util.isPathOverlap(realPath, dictObj.values()):
            return False
        return True

    @staticmethod
    def getFreeTcpPort(portType):
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
    def isPathOverlap(path, pathList):
        for p in pathList:
            if path == p or p.startswith(path + "/") or path.startswith(p + "/"):
                return True
        return False

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
