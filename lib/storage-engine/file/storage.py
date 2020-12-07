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
        self._ftpServer = None
        self._rsyncServer = None

        self._mirrorSiteDict = dict()

    def addMirrorSite(self, mirrorSiteId, masterDir, xmlElem):
        assert not self._bStart

        msParam = _Param()
        msParam.masterDir = masterDir
        msParam.dataDir = os.path.join(masterDir, "storage-file")
        if len(xmlElem.xpathEval("./advertise-protocols")) > 0:
            for proto in xmlElem.xpathEval("./advertise-protocols")[-1].text.split(":"):
                if proto not in ["http", "ftp", "rsync"]:
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
            if "ftp" in self._mirrorSiteDict[msId].advertiseProtocolList:
                if self._ftpServer is None:
                    self._ftpServer = _FtpServer(self._listenIp, self._tmpDir, self._logDir)
                    self._ftpServer.start()
            if "rsync" in self._mirrorSiteDict[msId].advertiseProtocolList:
                if self._rsyncServer is None:
                    self._rsyncServer = _RsyncServer(self._listenIp, self._tmpDir, self._logDir)
                    self._rsyncServer.start()
        self._bStart = True

    def stop(self):
        assert self._bStart

        self._bStart = False
        if self._rsyncServer is not None:
            self._rsyncServer.stop()
            self._rsyncServer = None
        if self._ftpServer is not None:
            self._ftpServer.stop()
            self._ftpServer = None
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
        if "ftp" in self._mirrorSiteDict[mirrorSiteId].advertiseProtocolList:
            assert self._ftpServer is not None
            self._ftpServer.addDir(mirrorSiteId, self._mirrorSiteDict[mirrorSiteId]["data-directory"])
        if "rsync" in self._mirrorSiteDict[mirrorSiteId].advertiseProtocolList:
            assert self._rsyncServer is not None
            self._rsyncServer.addDir(mirrorSiteId, self._mirrorSiteDict[mirrorSiteId]["data-directory"])


class _Param:

    def __init__(self):
        self.masterDir = None
        self.dataDir = None
        self.advertiseProtocolList = []


class _HttpServer:

    def __init__(self, listenIp, tmpDir, logDir):
        self._virtRootDir = os.path.join(tmpDir, "file-httpd")
        self._cfgFn = os.path.join(tmpDir, "file-httpd.conf")
        self._pidFile = os.path.join(tmpDir, "file-httpd.pid")
        self._errorLogFile = os.path.join(logDir, "file-httpd-error.log")
        self._accessLogFile = os.path.join(logDir, "file-httpd-access.log")

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
        logging.info("Slave server (http-file) started, listening on port %d." % (self._port))

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
        buf += '<Directory "%s">\n' % (self._virtRootDir)
        buf += '  Require all granted\n'
        buf += '</Directory>\n'

        # write file atomically
        with open(self._cfgFn + ".tmp", "w") as f:
            f.write(buf)
        os.rename(self._cfgFn + ".tmp", self._cfgFn)


class _FtpServer:

    def __init__(self, listenIp, tmpDir, logDir, updaterLogFileSize, updaterLogFileCount):
        self._execFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "ftpd.py")
        self._cfgFile = os.path.join(tmpDir, "ftpd.cfg")
        self._logFile = os.path.join(logDir, "ftpd.log")
        self._updaterLogFileSize = updaterLogFileSize
        self._updaterLogFileCount = updaterLogFileCount

        self._dirDict = dict()

        self._tmpDir = tmpDir
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
        self._generateCfgFile()
        self._proc = subprocess.Popen([self._execFile, self._cfgFile], cwd=self._tmpDir)
        _Util.waitTcpServiceForProc(self.param.listenIp, self._port, self._proc)
        logging.info("Slave server (ftp) started, listening on port %d." % (self._port))

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        if self._port is not None:
            self._port = None

    def addDir(self, name, realPath):
        assert self._proc is not None
        assert _Util.checkNameAndRealPath(self._dirDict, name, realPath)
        self._dirDict[name] = realPath
        self._generateCfgFile()
        os.kill(self._proc.pid, signal.SIGUSR1)

    def _generateCfgFile(self):
        # generate file content
        dataObj = dict()
        dataObj["logFile"] = self._logFile
        dataObj["logMaxBytes"] = self._updaterLogFileSize
        dataObj["logBackupCount"] = self._updaterLogFileCount
        dataObj["ip"] = self._listenIp
        dataObj["port"] = self._port
        dataObj["dirmap"] = self._dirDict

        # write file atomically
        with open(self._cfgFile + ".tmp", "w") as f:
            json.dump(dataObj, f)
        os.rename(self._cfgFile + ".tmp", self._cfgFile)


class _RsyncServer:

    def __init__(self, listenIp, tmpDir, logDir):
        self._dirDict = dict()

        self._cfgFile = os.path.join(tmpDir, "rsyncd.conf")
        self._lockFile = os.path.join(tmpDir, "rsyncd.lock")
        self._logFile = os.path.join(logDir, "rsyncd.log")

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
        self._generateCfgFile()
        self._proc = subprocess.Popen(["/usr/bin/rsync", "-v", "--daemon", "--no-detach", "--config=%s" % (self._cfgFile)])
        _Util.waitTcpServiceForProc(self._listenIp, self._port, self._proc)
        logging.info("Slave server (rsync) started, listening on port %d." % (self._port))

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None

    def addDir(self, name, realPath):
        assert self._proc is not None
        assert _Util.checkNameAndRealPath(self._dirDict, name, realPath)
        self._dirDict[name] = realPath
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
        for name, d in self._dirDict.items():
            buf += "[%s]\n" % (name)
            buf += "path = %s\n" % (d)
            buf += "read only = yes\n"
            buf += "\n"

        # write file atomically
        with open(self._cfgFile + ".tmp", "w") as f:
            f.write(buf)
        os.rename(self._cfgFile + ".tmp", self._cfgFile)


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
