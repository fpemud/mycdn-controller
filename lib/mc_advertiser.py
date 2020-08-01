#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import json
import jinja2
import signal
import logging
import logging.handlers
import aiohttp
import aiohttp_jinja2
import subprocess
from mc_util import McUtil
from mc_param import McConst


class McAdvertiser:

    def __init__(self, param):
        self.param = param

        # create advertise servers
        self.httpServer = _HttpServer(self.param, "Advertising Server (http)")
        self.ftpServer = _FtpServer(self.param, "Advertising Server (ftp)")
        self.rsyncServer = _RsyncServer(self.param, "Advertising Server (rsync)")

        # register
        for ms in self.param.mirrorSiteDict.values():
            for storageName, protocolList in ms.advertiseDict.items():
                for proto in protocolList:
                    if proto == "http":
                        self.httpServer.useBy(ms.id)
                    elif proto == "ftp":
                        self.ftpServer.useBy(ms.id)
                    elif proto == "rsync":
                        self.rsyncServer.useBy(ms.id)
                    elif proto == "git-http":
                        self.httpServer.useBy(ms.id)
                    else:
                        assert False

    def start(self):
        # start main server
        self.param.mainloop.run_until_complete(self._start())
        logging.info("Main server started.")

        # start advertise servers
        self.httpServer.start()
        self.ftpServer.start()
        self.rsyncServer.start()

    def stop(self):
        # stop advertise servers
        if self.rsyncServer is not None:
            self.rsyncServer.stop()
            self.rsyncServer = None
        if self.ftpServer is not None:
            self.ftpServer.stop()
            self.ftpServer = None
        if self.httpServer is not None:
            self.httpServer.stop()
            self.httpServer = None

        # stop main server
        if self._runner is not None:
            self.param.mainloop.run_until_complete(self._stop())

    def advertiseMirrorSite(self, mirrorSiteId):
        msObj = self.param.mirrorSiteDict[mirrorSiteId]
        if "file" in msObj.advertiseDict:
            if "http" in msObj.advertiseDict["file"]:
                self.httpServer.addFileDir(msObj.id, msObj.storageDict["file"].cacheDir)
            if "ftp" in msObj.advertiseDict["file"]:
                self.ftpServer.addFileDir(msObj.id, msObj.storageDict["file"].cacheDir)
            if "rsync" in msObj.advertiseDict["file"]:
                self.rsyncServer.addFileDir(msObj.id, msObj.storageDict["file"].cacheDir)
        if "git" in msObj.advertiseDict:
            if "git" in msObj.advertiseDict["git"]:
                assert False        # FIXME
            if "ssh" in msObj.advertiseDict["git"]:
                assert False        # FIXME
            if "http" in msObj.advertiseDict["git"]:
                pass                # FIXME

    async def _start(self):
        try:
            if True:
                self._app = aiohttp.web.Application(loop=self.param.mainloop)
                self._app.router.add_route("GET", "/api/mirrors", self._apiMirrorsHandler)
                self._app.router.add_route("GET", "/", self._indexHandler)
            if True:
                self._log = logging.getLogger("aiohttp")
                self._log.propagate = False
                self._log.addHandler(logging.handlers.RotatingFileHandler(os.path.join(McConst.logDir, 'main-httpd.log'),
                                                                          maxBytes=McConst.updaterLogFileSize,
                                                                          backupCount=McConst.updaterLogFileCount))
            if True:
                aiohttp_jinja2.setup(self._app, loader=jinja2.FileSystemLoader('/usr/share/mirrors'))       # FIXME, we should use VUE alike, not jinja
                self._runner = aiohttp.web.AppRunner(self._app)
                await self._runner.setup()
                site = aiohttp.web.TCPSite(self._runner, self.param.listenIp, self.param.mainPort)
                await site.start()
        except Exception:
            await self._stop()
            raise

    async def _stop(self):
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        if self._log is not None:
            for h in self._log.handlers:
                self._log.removeHandler(h)
            self._log = None
        if self._app is not None:
            # how to dispose self._app?
            self._app = None

    async def _indexHandler(self, request):
        data = {
            "static": {
                "title": "mirror site",
                "name": "镜像名",
                "update_time": "上次更新时间",
                "help": "使用帮助",
            },
            "mirror_site_dict": self.__getMirrorSiteDict(),
        }
        return aiohttp_jinja2.render_template('index.jinja2', request, data)

    async def _apiMirrorsHandler(self, request):
        return aiohttp.web.json_response(self.__getMirrorSiteDict())

    def __getMirrorSiteDict(self):
        ret = dict()
        for msId, msObj in self.param.mirrorSiteDict.items():
            if msObj.availablityMode == "always":
                bAvail = True
            elif msObj.availablityMode == "initialized":
                bAvail = self.param.updater.isMirrorSiteInitialized(msId)
            else:
                assert False

            ret[msId] = {
                "available": bAvail,
                "update_status": self.param.updater.getMirrorSiteUpdateStatus(msId),
                "update_progress": -1,
                "last_update_time": "",
                "help": {
                    "title": "",
                    "filename": "",
                },
            }

            if "file" in msObj.advertiseDict:
                ret[msId]["interface-file"] = dict()
                for proto in msObj.advertiseDict["file"]:
                    if proto == "http":
                        port = self.httpServer.port
                        ret[msId]["interface-file"]["http"] = {
                            "url": "http://{IP}%s/m/%s" % (":%d" % (port) if port != 80 else "", msId)
                        }
                        continue
                    if proto == "ftp":
                        port = self.ftpServer.port
                        ret[msId]["interface-file"]["ftp"] = {
                            "url": "ftp://{IP}%s/%s" % (":%d" % (port) if port != 21 else "", msId)
                        }
                        continue
                    if proto == "rsync":
                        port = self.rsyncServer.port
                        ret[msId]["interface-file"]["rsync"] = {
                            "url": "rsync://{IP}%s/%s" % (":%d" % (port) if port != 873 else "", msId)
                        }
                        continue

            if "git" in msObj.advertiseDict:
                ret[msId]["interface-git"] = dict()
                for proto in msObj.advertiseDict["git"]:
                    if proto == "git":
                        pass
                    if proto == "ssh":
                        pass
                    if proto == "http":
                        pass

        return ret


class _HttpServer:

    def __init__(self, param, serverName):
        self.param = param
        self._serverName = serverName
        self._port = None

        self._userSet = set()
        self._dirDict = dict()          # files
        self._gitDirDict = dict()       # git repositories

        self._cfgFile = os.path.join(McConst.tmpDir, "httpd.conf")
        self._pidFile = os.path.join(McConst.tmpDir, "httpd.pid")
        self._errorLogFile = os.path.join(McConst.logDir, "httpd-error.log")
        self._accessLogFile = os.path.join(McConst.logDir, "httpd-access.log")
        self._bStart = False
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def useBy(self, user):
        assert not self._bStart
        self._userSet.add(user)

    def start(self):
        assert not self._bStart
        self._bStart = True
        try:
            if len(self._userSet) > 0:
                self._port = McUtil.getFreeSocketPort("tcp")
                self._generateCfgFile()
                self._proc = subprocess.Popen(["/usr/sbin/apache2", "-d", os.path.dirname(self._cfgFile), "-f", self._cfgFile, "-DFOREGROUND"])
                McUtil.waitTcpServiceForProc(self.param.listenIp, self._port, self._proc)
                logging.info("%s started, listening on port %d." % (self._serverName, self._port))
        except Exception:
            self._bStart = False
            raise

    def stop(self):
        assert self._bStart
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        self._bStart = False

    def isStarted(self):
        return self._bStart

    def isRunning(self):
        assert self._bStart
        return self._proc is not None

    def addFileDir(self, name, realPath):
        assert self._proc is not None
        self._dirDict[name] = realPath
        self._generateCfgFile()

    def addGitDir(self, name, realPath):
        assert self._proc is not None
        self._gitDirDict[name] = realPath
        self._generateCfgFile()
        os.kill(self._proc.pid, signal.SIGUSR1)

    def _generateCfgFile(self):
        modulesDir = "/usr/lib64/apache2/modules"

        buf = ""
        buf += "LoadModule log_config_module      %s/mod_log_config.so\n" % (modulesDir)
        buf += "LoadModule env_module             %s/mod_env.so\n" % (modulesDir)
        buf += "LoadModule unixd_module           %s/mod_unixd.so\n" % (modulesDir)
        buf += "LoadModule alias_module           %s/mod_alias.so\n" % (modulesDir)
        buf += "LoadModule cgi_module             %s/mod_cgi.so\n" % (modulesDir)
        buf += "\n"
        buf += "ServerName mirrors\n"                                                                       # FIXME
        buf += "DocumentRoot /var/cache/mirrors\n"                                                          # FIXME
        buf += "\n"
        buf += 'PidFile "%s"\n' % (self._pidFile)
        buf += 'ErrorLog "%s"\n' % (self._errorLogFile)
        buf += r'LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" common' + "\n"
        buf += 'CustomLog "%s" common\n' % (self._accessLogFile)
        buf += "\n"
        buf += "Listen %d http\n" % (self._port)
        buf += "\n"

        # for file
        for name, realDir in self._dirDict.items():
            buf += '  <Directory "%s">\n' % (realDir)
            buf += '    AllowOverride None\n'
            buf += '  </Directory>\n'

        # for git
        if True:
            pass
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

        with open(self._cfgFile, "w") as f:
            f.write(buf)


class _FtpServer:

    def __init__(self, param, serverName):
        self.param = param
        self._serverName = serverName
        self._port = None

        self._userSet = set()
        self._dirDict = dict()

        self._ftpdExecFile = os.path.join(McConst.libexecDir, "ftpd.py")
        self._ftpdCfgFile = os.path.join(McConst.tmpDir, "mirrors-ftpd.cfg")
        self._bStart = False
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def useBy(self, user):
        assert not self._bStart
        self._userSet.add(user)

    def start(self):
        assert not self._bStart
        self._bStart = True
        try:
            if len(self._userSet) > 0:
                self._port = McUtil.getFreeSocketPort("tcp")
                self._generateCfgFile()
                self._proc = subprocess.Popen([self._ftpdExecFile, self._ftpdCfgFile])
                McUtil.waitTcpServiceForProc(self.param.listenIp, self._port, self._proc)
                logging.info("%s started, listening on port %d." % (self._serverName, self._port))
        except Exception:
            self._bStart = False
            raise

    def stop(self):
        assert self._bStart
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        self._bStart = False

    def isStarted(self):
        return self._bStart

    def isRunning(self):
        assert self._bStart
        return self._proc is not None

    def addFileDir(self, name, realPath):
        assert self._proc is not None
        self._dirDict[name] = realPath
        self._generateCfgFile()
        os.kill(self._proc.pid, signal.SIGUSR1)

    def _generateCfgFile(self):
        dataObj = dict()
        dataObj["ip"] = self.param.listenIp
        dataObj["port"] = self._port
        dataObj["dirmap"] = self._dirDict
        with open(self._ftpdCfgFile, "w") as f:
            json.dump(dataObj, f)


class _RsyncServer:

    def __init__(self, param, serverName):
        self.param = param
        self._serverName = serverName
        self._port = None

        self._userSet = set()
        self._dirDict = dict()

        self._cfgFile = os.path.join(McConst.tmpDir, "rsyncd.conf")
        self._lockFile = os.path.join(McConst.tmpDir, "rsyncd.lock")
        self._logFile = os.path.join(McConst.logDir, "rsyncd.log")
        self._bStart = False
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    def useBy(self, user):
        assert not self._bStart
        self._userSet.add(user)

    def start(self):
        assert not self._bStart
        self._bStart = True
        try:
            if len(self._userSet) > 0:
                self._port = McUtil.getFreeSocketPort("tcp")
                self._generateCfgFile()
                self._proc = subprocess.Popen(["/usr/bin/rsync", "-v", "--daemon", "--no-detach", "--config=%s" % (self._cfgFile)])
                McUtil.waitTcpServiceForProc(self.param.listenIp, self._port, self._proc)
                logging.info("%s started, listening on port %d." % (self._serverName, self._port))
        except Exception:
            self._bStart = False
            raise

    def stop(self):
        assert self._bStart
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        self._bStart = False

    def isStarted(self):
        return self._bStart

    def isRunning(self):
        assert self._bStart
        return self._proc is not None

    def addFileDir(self, name, realPath):
        assert self._proc is not None
        self._dirDict[name] = realPath
        self._generateCfgFile()             # rsync picks the new cfg-file when new connection comes in

    def _generateCfgFile(self):
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
        with open(self._cfgFile, "w") as f:
            f.write(buf)
