
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
        McUtil.forceDelete(self._virtRootDir)

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

