#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import json
import signal
import logging
import subprocess
from mc_util import McUtil
from mc_param import McConst


class McSlaveServers:

    def __init__(self, param):
        self.param = param
        self.httpServer = None
        self.ftpServer = None
        self.rsyncServer = None
        self.gitServer = None

        # register servers by storage
        pass

        # register servers by advertiser
        for ms in self.param.mirrorSiteDict.values():
            for storageName, protocolList in ms.advertiseDict.items():
                if storageName == "file":
                    if "http" in protocolList:
                        self.httpServer = True
                    if "ftp" in protocolList:
                        self.ftpServer = True
                    if "rsync" in protocolList:
                        self.rsyncServer = True
                if storageName == "git":
                    if "git" in protocolList:
                        self.gitServer = True
                    if "http" in protocolList:
                        self.httpServer = True

        # create servers
        if self.httpServer is not None:
            self.httpServer = _HttpServer(self.param)
            self.httpServer.start()
        if self.ftpServer is not None:
            self.ftpServer = _FtpServer(self.param)
            self.ftpServer.start()
        if self.rsyncServer is not None:
            self.rsyncServer = _RsyncServer(self.param)
            self.rsyncServer.start()
        if self.gitServer is not None:
            self.gitServer = _GitServer(self.param)
            self.gitServer.start()

    def dispose(self):
        if self.gitServer is not None:
            self.gitServer.stop()
        if self.rsyncServer is not None:
            self.rsyncServer.stop()
        if self.ftpServer is not None:
            self.ftpServer.stop()
        if self.httpServer is not None:
            self.httpServer.stop()


class _HttpServer:

    def __init__(self, param):
        self.param = param
        self._port = None

        self._dirDict = dict()          # files
        self._gitDirDict = dict()       # git repositories

        self._virtRootDir = os.path.join(McConst.tmpDir, "vroot-httpd")
        self._cfgFn = os.path.join(McConst.tmpDir, "httpd.conf")
        self._pidFile = os.path.join(McConst.tmpDir, "httpd.pid")
        self._errorLogFile = os.path.join(McConst.logDir, "httpd-error.log")
        self._accessLogFile = os.path.join(McConst.logDir, "httpd-access.log")

        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def start(self):
        assert self._proc is None
        self._port = McUtil.getFreeSocketPort("tcp")
        self._generateVirtualRootDir()
        self._generateVirtualRootDirFile()
        self._generateVirtualRootDirGit()
        self._generateCfgFn()
        self._proc = subprocess.Popen(["/usr/sbin/apache2", "-f", self._cfgFn, "-DFOREGROUND"])
        McUtil.waitTcpServiceForProc(self.param.listenIp, self._port, self._proc)
        logging.info("Slave server (http) started, listening on port %d." % (self._port))

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None

    def addFileDir(self, name, realPath):
        assert self._proc is not None
        assert _checkNameAndRealPath(self._dirDict, name, realPath)
        self._dirDict[name] = realPath
        self._generateVirtualRootDirFile()
        self._generateCfgFn()
        os.kill(self._proc.pid, signal.SIGUSR1)

    def addGitDir(self, name, realPath):
        assert self._proc is not None
        assert _checkNameAndRealPath(self._gitDirDict, name, realPath)
        self._gitDirDict[name] = realPath
        self._generateVirtualRootDirGit()
        self._generateCfgFn()
        os.kill(self._proc.pid, signal.SIGUSR1)

    def _generateVirtualRootDir(self):
        McUtil.ensureDir(self._virtRootDir)

    def _generateVirtualRootDirFile(self):
        if len(self._dirDict) == 0:
            return

        virtRootDirFile = os.path.join(self._virtRootDir, "file")

        McUtil.ensureDir(virtRootDirFile)

        # create new directories
        for name, realPath in self._dirDict.items():
            dn = os.path.join(virtRootDirFile, name)
            if not os.path.exists(dn):
                os.symlink(realPath, dn)

        # remove old directories
        for dn in os.listdir(virtRootDirFile):
            if dn not in self._dirDict:
                os.unlink(dn)

    def _generateVirtualRootDirGit(self):
        if len(self._gitDirDict) == 0:
            return

        virtRootDirGit = os.path.join(self._virtRootDir, "git")
        McUtil.ensureDir(virtRootDirGit)

        # create new directories
        for name, realPath in self._gitDirDict.items():
            dn = os.path.join(virtRootDirGit, name)
            if not os.path.exists(dn):
                os.symlink(realPath, dn)

        # remove old directories
        for dn in os.listdir(virtRootDirGit):
            if dn not in self._gitDirDict:
                os.unlink(dn)

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
        buf += '<Directory "%s">\n' % (self._virtRootDir)
        buf += '  Options Indexes FollowSymLinks\n'
        buf += '  Require all denied\n'
        buf += '</Directory>\n'
        if len(self._dirDict) > 0:
            buf += '<Directory "%s">\n' % (os.path.join(self._virtRootDir, "file"))
            buf += '  Require all granted\n'
            buf += '</Directory>\n'
        buf += "\n"

        # git settings
        if len(self._gitDirDict) > 0:
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
            buf += "\n"

        # write file atomically
        with open(self._cfgFn + ".tmp", "w") as f:
            f.write(buf)
        os.rename(self._cfgFn + ".tmp", self._cfgFn)


class _FtpServer:

    def __init__(self, param):
        self.param = param
        self._port = None

        self._dirDict = dict()

        self._execFile = os.path.join(McConst.libexecDir, "ftpd.py")
        self._cfgFile = os.path.join(McConst.tmpDir, "ftpd.cfg")
        self._logFile = os.path.join(McConst.logDir, "ftpd.log")
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def start(self):
        assert self._proc is None
        self._port = McUtil.getFreeSocketPort("tcp")
        self._generateCfgFile()
        self._proc = subprocess.Popen([self._execFile, self._cfgFile], cwd=McConst.cacheDir)
        McUtil.waitTcpServiceForProc(self.param.listenIp, self._port, self._proc)
        logging.info("Slave server (ftp) started, listening on port %d." % (self._port))

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None

    def addFileDir(self, name, realPath):
        assert self._proc is not None
        assert _checkNameAndRealPath(self._dirDict, name, realPath)
        self._dirDict[name] = realPath
        self._generateCfgFile()
        os.kill(self._proc.pid, signal.SIGUSR1)

    def _generateCfgFile(self):
        # generate file content
        dataObj = dict()
        dataObj["logFile"] = self._logFile
        dataObj["logMaxBytes"] = McConst.updaterLogFileSize
        dataObj["logBackupCount"] = McConst.updaterLogFileCount
        dataObj["ip"] = self.param.listenIp
        dataObj["port"] = self._port
        dataObj["dirmap"] = self._dirDict

        # write file atomically
        with open(self._cfgFile + ".tmp", "w") as f:
            json.dump(dataObj, f)
        os.rename(self._cfgFile + ".tmp", self._cfgFile)


class _RsyncServer:

    def __init__(self, param):
        self.param = param
        self._port = None

        self._dirDict = dict()

        self._cfgFile = os.path.join(McConst.tmpDir, "rsyncd.conf")
        self._lockFile = os.path.join(McConst.tmpDir, "rsyncd.lock")
        self._logFile = os.path.join(McConst.logDir, "rsyncd.log")
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def start(self):
        assert self._proc is None
        self._port = McUtil.getFreeSocketPort("tcp")
        self._generateCfgFile()
        self._proc = subprocess.Popen(["/usr/bin/rsync", "-v", "--daemon", "--no-detach", "--config=%s" % (self._cfgFile)])
        McUtil.waitTcpServiceForProc(self.param.listenIp, self._port, self._proc)
        logging.info("Slave server (rsync) started, listening on port %d." % (self._port))

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None

    def addFileDir(self, name, realPath):
        assert self._proc is not None
        assert _checkNameAndRealPath(self._dirDict, name, realPath)
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


class _GitServer:

    def __init__(self, param):
        self.param = param
        self._port = None

        self._virtRootDir = os.path.join(McConst.tmpDir, "vroot-git-daemon")
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def start(self):
        assert self._proc is None

        McUtil.ensureDir(self._virtRootDir)
        self._port = McUtil.getFreeSocketPort("tcp")
        self._proc = subprocess.Popen([
            "/usr/libexec/git-core/git-daemon",
            "--export-all",
            "--listen=%s" % (self.param.listenIp),
            "--port=%d" % (self._port),
            "--base-path=%s" % (self._virtRootDir),
        ])
        McUtil.waitTcpServiceForProc(self.param.listenIp, self._port, self._proc)
        logging.info("Slave server (git) started, listening on port %d." % (self._port))

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        McUtil.forceDelete(self._virtRootDir)

    def addGitDir(self, name, realPath):
        assert self._proc is not None
        assert _checkNameAndRealPath(self._dirDict, name, realPath)
        os.symlink(realPath, os.path.join(self._virtRootDirFile, name))


def _checkNameAndRealPath(dictObj, name, realPath):
    if name in dictObj:
        return False
    if not os.path.isabs(realPath) or realPath.endswith("/"):
        return False
    if McUtil.isPathOverlap(realPath, dictObj.values()):
        return False
    return True
