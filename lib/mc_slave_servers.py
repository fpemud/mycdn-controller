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
        self._virtRootDir = os.path.join(McConst.tmpDir, "vroot-httpd")
        self._cfgFn = os.path.join(McConst.tmpDir, "httpd.conf")
        self._pidFile = os.path.join(McConst.tmpDir, "httpd.pid")
        self._errorLogFile = os.path.join(McConst.logDir, "httpd-error.log")
        self._accessLogFile = os.path.join(McConst.logDir, "httpd-access.log")

        self._dirDict = dict()          # files
        self._gitDirDict = dict()       # git repositories

        self._port = None
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
        if self._port is not None:
            self._port = None

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
        self._execFile = os.path.join(McConst.libexecDir, "ftpd.py")
        self._cfgFile = os.path.join(McConst.tmpDir, "ftpd.cfg")
        self._logFile = os.path.join(McConst.logDir, "ftpd.log")

        self._dirDict = dict()

        self._port = None
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
        if self._port is not None:
            self._port = None

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
        self._virtRootDir = os.path.join(McConst.tmpDir, "vroot-git-daemon")

        self._port = None
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
        if self._port is not None:
            self._port = None
        McUtil.forceDelete(self._virtRootDir)

    def addGitDir(self, name, realPath):
        assert self._proc is not None
        assert _checkNameAndRealPath(self._dirDict, name, realPath)
        os.symlink(realPath, os.path.join(self._virtRootDirFile, name))


class _MultiInstanceMariadbServer:

    """
    The best solution would be using a one-instance-mariadb-server, and dynamically
    add table files stored in seperate directories as different databases. Although
    basically mariadb supports this kind of operation, but there're
    corner cases (for example when the server crashes).
    """

    def __init__(self, param):
        self.param = param
        self._dirDict = dict()              # <database-name,data-dir>
        self._tableInfoDict = dict()        # <database-name,table-info>
        self._procDict = dict()             # <database-name,(proc,port,cfg-file,log-ile)>
        self._dbRootPassword = "root"       # FIXME

    def start(self):
        assert self._proc is None
        logging.info("Slave server (multi-instanced-mariadb) started.")

    def stop(self):
        for value in self._procDict.values():
            proc = value[0]
            proc.terminate()
            proc.wait()
        self._procDict.clear()
        self._tableInfoDict.clear()
        self._dirDict.clear()

    def addDatabaseDir(self, databaseName, dataDir, tableInfo):
        # tableInfo { "table-name": ( block-size, "table-schema" ) }
        import mariadb
        assert self._proc is not None
        assert _checkNameAndRealPath(self._dirDict, databaseName, dataDir)

        cfgFile = os.path.join(McConst.tmpDir, "mariadb-%s.cnf" % (databaseName))
        logFile = os.path.join(McConst.logDir, "mariadb-%s.log" % (databaseName))

        # initialize if needed
        if self._isInitialized(dataDir):
            self._initialize(databaseName, dataDir, tableInfo, logFile)

        # start process
        proc = None
        port = None
        try:
            port = McUtil.getFreeSocketPort("tcp")

            # generate mariadb config file
            with open(cfgFile, "w") as f:
                buf = ""
                buf += "[mysqld]\n"
                f.write(buf)

            # start mariadb
            with open(logFile, "a") as f:
                f.write("\n\n")
                f.write("## mariadb-db #######################\n")
            proc = subprocess.Popen(["/usr/sbin/mysqld"] + self.__commonOptions(dataDir, port))
            McUtil.waitTcpServiceForProc(self.param.listenIp, port, proc)

            # check
            with mariadb.connect(unix_socket=self._socketFile, user="root", password=self._dbRootPassword) as conn:
                cur = conn.cursor()
                cur.execute("USE %s;" % (databaseName))
                for tableName, value in tableInfo.items():
                    blockSize, tableSchema = value
                    out = cur.execute("SHOW CREATE TABLE %s;" % (tableName))
                    if out != tableSchema:
                        raise Exception("table schema error")

            # save
            self._dirDict[databaseName] = dataDir
            self._tableInfoDict[databaseName] = tableInfo
            self._procDict[databaseName] = (proc, port, cfgFile, logFile)
        except Exception:
            if databaseName in self._procDict:
                self._procDict[databaseName]
            if databaseName in self._tableInfoDict:
                self._tableInfoDict[databaseName]
            if databaseName in self._dirDict:
                del self._dirDict[databaseName]
            if proc is not None:
                proc.terminate()
                proc.wait()
            if os.path.exists(cfgFile):
                os.unlink(cfgFile)
            raise

    def getDatabasePort(self, databaseName):
        assert self._proc is not None
        return self._procDict[databaseName][1]

    def _isInitialized(self, dataDir):
        if not os.path.exists(os.path.join(dataDir, "mysql", "user.frm")):
            # from /usr/share/mariadb/scripts/mysql_install_db
            return False
        elif os.path.exists(os.path.join(dataDir, "initialize.failed")):
            # from self._initialize()
            return False
        else:
            return True

    def _initialize(self, databaseName, dataDir, tableInfo, logFile):
        McUtil.mkDirAndClear(dataDir)
        try:
            commands = []

            # the following commands are from script /usr/share/mariadb/scripts/mariadb-install-db
            if True:
                commands += [
                    "CREATE DATABASE IF NOT EXISTS mysql;",
                    "USE mysql;",
                    "SET @auth_root_socket=NULL;",
                ]
                tables = [
                    "/usr/share/mariadb/fill_help_tables.sql",
                    "/usr/share/mariadb/mysql_system_tables.sql",
                    "/usr/share/mariadb/mysql_performance_tables.sql",
                    "/usr/share/mariadb/mysql_system_tables_data.sql",
                    "/usr/share/mariadb/maria_add_gis_sp_bootstrap.sql",
                ]
                for fn in tables:
                    tlist = McUtil.readFile(fn).split("\n")
                    for line in tlist:
                        if "@current_hostname" in line:
                            continue
                        commands.append(line)

            # create our database
            if True:
                commands.append("CREATE DATABASE IF NOT EXISTS %s;" % (databaseName)),
                commands.append("USE %s;" % (databaseName))
                for dummy, sql in tableInfo.values():
                    commands += sql.split("\n")

            # execute
            if True:
                out = McUtil.cmdCallWithInput("/usr/sbin/mysqld",                                 # command
                                              "\n".join(commands),                                # input
                                              "--no-defaults", "--bootstrap", "--basedir=/usr",   # arguments
                                              "--datadir=%s" % (dataDir), "--log-warnings=0",     # arguments
                                              "--enforce-storage-engine=''")                      # arguments
                with open(logFile, "w") as f:
                    f.write("## mariadb-install-db #######################\n")
                    f.write(out)
        except Exception:
            McUtil.touchFile(os.path.join(dataDir, "initialize.failed"))
            raise

    def __commonOptions(self, dataDir, port):
        return [
            "--no-defaults",
            "--basedir=/usr",
            "--datadir=%s" % (dataDir),
            "--bind-address=%s" % (self.param.listenIp),
            "--port=%d" % (port),
        ]


class _MongodbServer:
    pass


class _Neo4jServer:
    pass


def _checkNameAndRealPath(dictObj, name, realPath):
    if name in dictObj:
        return False
    if not os.path.isabs(realPath) or realPath.endswith("/"):
        return False
    if McUtil.isPathOverlap(realPath, dictObj.values()):
        return False
    return True


# # mysql_secure_installation
# with open(logFile, "a") as f:
#     f.write("\n")
#     f.write("## mysql_secure_installation #######################\n")
# proc = None
# child = None
# try:
#     proc = subprocess.Popen(["/usr/sbin/mysqld"] + self.__commonOptions())
#     McUtil.waitTcpServiceForProc(self.param.listenIp, self._port, proc)
#     with open(logFile, "ab") as f:
#         child = pexpect.spawn("/usr/bin/mysql_secure_installation --no-defaults --socket=%s" % (self._socketFile), logfile=f)
#         child.expect('Enter current password for root \\(enter for none\\): ')
#         child.sendline("")
#         child.expect("Switch to unix_socket authentication \\[Y/n\\] ")
#         child.sendline('n')
#         child.expect('Change the root password\\? \\[Y/n\\] ')
#         child.sendline('Y')
#         child.expect('New password: ')
#         child.sendline(self._dbRootPassword)
#         child.expect('Re-enter new password: ')
#         child.sendline(self._dbRootPassword)
#         child.expect('Remove anonymous users\\? \\[Y/n\\] ')
#         child.sendline('Y')
#         child.expect('Disallow root login remotely\\? \\[Y/n\\] ')
#         child.sendline('Y')
#         child.expect('Remove test database and access to it\\? \\[Y/n\\] ')
#         child.sendline('Y')
#         child.expect('Reload privilege tables now\\? \\[Y/n\\] ')
#         child.sendline('n')
#         child.expect(pexpect.EOF)
# finally:
#     if child is not None:
#         child.terminate()
#         child.wait()
#     if proc is not None:
#         proc.terminate()
#         proc.wait()
