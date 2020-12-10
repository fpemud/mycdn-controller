#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import json
import signal
import mariadb
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
        self.mariadbServer = None
        self.neo4jServer = None

        # register servers by storage
        for ms in self.param.mirrorSiteDict.values():
            for storageName, obj in ms.storageDict.items():
                if storageName == "mariadb":
                    self.mariadbServer = True
                if storageName == "neo4j":
                    self.neo4jServer = True

        # register servers by advertiser
        for ms in self.param.mirrorSiteDict.values():
            for advertiserName, interfaceList in ms.advertiseDict.items():
                if advertiserName == "file":
                    if "http" in interfaceList:
                        self.httpServer = True
                    if "ftp" in interfaceList:
                        self.ftpServer = True
                    if "rsync" in interfaceList:
                        self.rsyncServer = True
                if advertiserName == "git":
                    if "git" in interfaceList:
                        self.gitServer = True
                    if "http" in interfaceList:
                        self.httpServer = True
                if advertiserName == "mediawiki":
                    if "database" in interfaceList:
                        self.mariadbServer = True
                    if "web" in interfaceList:
                        self.httpServer = True              # export as mediawiki web page
                if advertiserName == "mariadb":
                    if "database" in interfaceList:
                        self.mariadbServer = True
                    if "http" in interfaceList:
                        self.httpServer = True              # export as database web interface
                if advertiserName == "neo4j":
                    if "database" in interfaceList:
                        self.neo4jServer = True

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
        if self.mariadbServer is not None:
            self.mariadbServer = _MultiInstanceMariadbServer(self.param)
            self.mariadbServer.start()
        if self.neo4jServer is not None:
            self.neo4jServer = _MultiInstanceNeo4jServer(self.param)
            self.neo4jServer.start()

        # FIXME: register database
        for ms in self.param.mirrorSiteDict.values():
            for storageName, obj in ms.storageDict.items():
                if storageName == "mariadb":
                    tableInfoRecordFile = os.path.join(ms.masterDir, "TABLE_SCHEMAS_PLUGIN")
                    databaseTableSchemaRecordFile = os.path.join(ms.masterDir, "TABLE_SCHEMAS_DATABASE")
                    self.mariadbServer.addDatabaseDir(ms.id, ms.storageDict["mariadb"].dataDir,
                                                      ms.storageDict["mariadb"].tableInfo,
                                                      tableInfoRecordFile,
                                                      databaseTableSchemaRecordFile)
                if storageName == "neo4j":
                    self.neo4jServer.addDatabaseDir(ms.id, ms.storageDict["neo4j"].dataDir)

    def dispose(self):
        if self.neo4jServer is not None:
            self.neo4jServer.stop()
        if self.mariadbServer is not None:
            self.mariadbServer.stop()
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
        McUtil.waitSocketPortForProc("tcp", self.param.listenIp, self._port, self._proc)
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
        McUtil.waitSocketPortForProc("tcp", self.param.listenIp, self._port, self._proc)
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
        McUtil.waitSocketPortForProc("tcp", self.param.listenIp, self._port, self._proc)
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
        McUtil.waitSocketPortForProc("tcp", self.param.listenIp, self._port, self._proc)
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
        self._dbWriteUser = "write"
        self._dbWritePasswd = "write"
        self._dbReadUser = "anonymous"
        self._bStarted = False

    def start(self):
        assert not self._bStarted
        self._bStarted = True
        logging.info("Slave server (multi-instanced-mariadb) started.")

    def stop(self):
        for value in self._procDict.values():
            proc = value[0]
            proc.terminate()
            proc.wait()
        self._procDict.clear()
        self._tableInfoDict.clear()
        self._dirDict.clear()

    def addDatabaseDir(self, databaseName, dataDir, tableInfo, tableInfoRecordFile, databaseTableSchemaRecordFile):
        # tableInfo is OrderedDict, content format: { "table-name": ( block-size, "table-schema" ) }
        assert self._bStarted
        assert _checkNameAndRealPath(self._dirDict, databaseName, dataDir)

        cfgFile = os.path.join(McConst.tmpDir, "mariadb-%s.cnf" % (databaseName))
        socketFile = os.path.join(McConst.tmpDir, "mariadb-%s.socket" % (databaseName))
        logFile = os.path.join(McConst.logDir, "mariadb-%s.log" % (databaseName))
        proc = None
        port = None
        try:
            # initialize if needed
            if not self._isInitialized(dataDir):
                self._initialize(databaseName, dataDir, tableInfo, logFile)
                bJustInitialized = True
            else:
                bJustInitialized = False

            # generate mariadb config file
            with open(cfgFile, "w") as f:
                buf = ""
                buf += "[mysqld]\n"
                f.write(buf)

            # allocate listening port
            port = McUtil.getFreeSocketPort("tcp")

            # start mariadb
            with open(logFile, "a") as f:
                f.write("\n\n")
                f.write("## mariadb-db #######################\n")
            cmd = [
                "/usr/sbin/mysqld",
                "--no-defaults",
                "--datadir=%s" % (dataDir),
                "--socket=%s" % (socketFile),
                "--bind-address=%s" % (self.param.listenIp),
                "--port=%d" % (port),
            ]
            proc = subprocess.Popen(cmd)
            McUtil.waitSocketPortForProc("tcp", self.param.listenIp, port, proc)

            # post-initialize if needed
            if bJustInitialized:
                self._initializePostStart(databaseName, tableInfo, tableInfoRecordFile, databaseTableSchemaRecordFile, socketFile)

            # check
            self._check(databaseName, tableInfo, tableInfoRecordFile, databaseTableSchemaRecordFile, socketFile)

            # save
            self._dirDict[databaseName] = dataDir
            self._tableInfoDict[databaseName] = tableInfo
            self._procDict[databaseName] = (proc, port, cfgFile, socketFile, logFile)
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

    def exportDatabaseDir(self, databaseName):
        # FIXME, currently addDatabaseDir does the export work which is obviously insecure
        assert self._bStarted

    def getDatabasePort(self, databaseName):
        assert self._bStarted
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
        McUtil.touchFile(os.path.join(dataDir, "initialize.failed"))

        commands = []

        # the following commands are from script /usr/share/mariadb/scripts/mariadb-install-db
        if True:
            commands += [
                "CREATE DATABASE IF NOT EXISTS mysql;",
                "USE mysql;",
                "SET @auth_root_socket=NULL;",
            ]
            tables = [
                "/usr/share/mariadb/mysql_system_tables.sql",
                "/usr/share/mariadb/mysql_performance_tables.sql",
                "/usr/share/mariadb/mysql_system_tables_data.sql",
                "/usr/share/mariadb/fill_help_tables.sql",
                "/usr/share/mariadb/maria_add_gis_sp_bootstrap.sql",
            ]
            for fn in tables:
                tlist = McUtil.readFile(fn).split("\n")
                for line in tlist:
                    if "@current_hostname" in line:
                        continue
                    commands.append(line)

        # keep root@localhost using unix_socket plugin
        # remove root@127.0.0.1 and root@::1, so that no network access is allowed
        # remove proxy priviledges for root user
        commands += [
            "UPDATE global_priv SET Priv = '{\"access\":0,\"plugin\":\"unix_socket\"}' WHERE Host = 'localhost' AND User = 'root';",
        ]
        commands += [
            "DELETE FROM global_priv WHERE Host = '127.0.0.1' AND User = 'root';",
            "DELETE FROM user WHERE Host = '127.0.0.1' AND User = 'root';",
        ]
        commands += [
            "DELETE FROM global_priv WHERE Host = '::1' AND User = 'root';",
            "DELETE FROM user WHERE Host = '::1' AND User = 'root';",
        ]
        commands += [
            "DELETE FROM proxies_priv WHERE Host = 'localhost' AND User = 'root';",
        ]

        # create write account
        commands += [
            McUtil.sqlInsertStatement("global_priv", {
                "Host": "localhost",
                "User": self._dbWriteUser,
                "Priv": McUtil.mysqlPrivJson(self._dbWritePasswd),
            }),
            McUtil.sqlInsertStatement("db", {
                "Host": "localhost",
                "User": self._dbWriteUser,
                "Db": databaseName,
                "Select_priv": "Y",
                "Insert_priv": "Y",
                "Update_priv": "Y",
                "Delete_priv": "Y",
                "Create_priv": "Y",
                "Drop_priv": "Y",
                "Grant_priv": "N",              # this value is "N"
                "References_priv": "Y",
                "Index_priv": "Y",
                "Alter_priv": "Y",
                "Create_tmp_table_priv": "Y",
                "Lock_tables_priv": "Y",
                "Create_view_priv": "Y",
                "Show_view_priv": "Y",
                "Create_routine_priv": "Y",
                "Alter_routine_priv": "Y",
                "Execute_priv": "Y",
                "Event_priv": "Y",
                "Trigger_priv": "Y",
                "Delete_history_priv": "Y",
            }),
        ]

        # create anonymous read-only account
        commands += [
            McUtil.sqlInsertStatement("global_priv", {
                "Host": "%",
                "User": self._dbReadUser,
                "Priv": '{"access":0}',
            }),
            McUtil.sqlInsertStatement("db", {
                "Host": "%",
                "User": self._dbReadUser,
                "Db": databaseName,
                "Select_priv": "Y",
                "Insert_priv": "N",             # this value is "N"
                "Update_priv": "N",             # this value is "N"
                "Delete_priv": "N",             # this value is "N"
                "Create_priv": "N",             # this value is "N"
                "Drop_priv": "N",               # this value is "N"
                "Grant_priv": "N",              # this value is "N"
                "References_priv": "Y",
                "Index_priv": "Y",
                "Alter_priv": "N",              # this value is "N"
                "Create_tmp_table_priv": "N",   # this value is "N"
                "Lock_tables_priv": "Y",
                "Create_view_priv": "N",        # this value is "N"
                "Show_view_priv": "Y",
                "Create_routine_priv": "N",     # this value is "N"
                "Alter_routine_priv": "N",      # this value is "N"
                "Execute_priv": "Y",
                "Event_priv": "Y",
                "Trigger_priv": "Y",
                "Delete_history_priv": "N",     # this value is "N"
            }),
        ]

        # create our database
        if True:
            commands.append("CREATE DATABASE IF NOT EXISTS %s;" % (databaseName)),
            commands.append("USE %s;" % (databaseName))
            for tableName, value in tableInfo.items():
                for line in value[1].split("\n"):
                    commands.append(line)

        # execute
        if True:
            out = McUtil.cmdCallWithInput("/usr/sbin/mysqld",                                 # command
                                          "\n".join(commands),                                # input
                                          "--no-defaults", "--bootstrap",                     # arguments
                                          "--datadir=%s" % (dataDir), "--log-warnings=0")     # arguments
            with open(logFile, "w") as f:
                f.write("## mariadb-install-db #######################\n")
                f.write(out)

        os.unlink(os.path.join(dataDir, "initialize.failed"))

    def _initializePostStart(self, databaseName, tableInfo, tableInfoRecordFile, databaseTableSchemaRecordFile, socketFile):
        # record table schema
        #
        # two seperate files "tableInfoRecordFile" and "databaseTableSchemaRecordFile" must be used
        # because we can't use simple string comparasion for "table info" and "table schema in database":
        # ====================================
        # CREATE TABLE MovieDirector (
        #     directorId INTEGER,
        #     movieId INTEGER,
        #     FOREIGN KEY (directorId) REFERENCES Director(id),
        #     FOREIGN KEY (movieId) REFERENCES Movie(id)
        # );
        # ====================================
        # CREATE TABLE `MovieDirector` (
        # `directorId` int(11) DEFAULT NULL,
        # `movieId` int(11) DEFAULT NULL,
        # KEY `directorId` (`directorId`),
        # KEY `movieId` (`movieId`),
        # CONSTRAINT `MovieDirector_ibfk_1` FOREIGN KEY (`directorId`) REFERENCES `Director` (`id`),
        # CONSTRAINT `MovieDirector_ibfk_2` FOREIGN KEY (`movieId`) REFERENCES `Movie` (`id`)
        # ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        # ====================================

        with open(tableInfoRecordFile, "w") as f:
            for tableName, value in tableInfo.items():
                f.write("---- " + tableName + " ----\n")
                f.write(value[1] + "\n")
                f.write("\n")

        with mariadb.connect(unix_socket=socketFile, database=databaseName, user=self._dbWriteUser, password=self._dbWritePasswd) as conn:
            cur = conn.cursor()
            cur.execute("SHOW TABLES;")
            tableNameList = [x[0] for x in cur.fetchall()]
            with open(databaseTableSchemaRecordFile, "w") as f:
                for tableName in tableNameList:
                    cur.execute("SHOW CREATE TABLE %s;" % (tableName))
                    out = cur.fetchall()[0][1]
                    f.write("---- " + tableName + " ----\n")
                    f.write(out + "\n")
                    f.write("\n")

    def _check(self, databaseName, tableInfo, tableInfoRecordFile, databaseTableSchemaRecordFile, socketFile):
        with mariadb.connect(unix_socket=socketFile, database=databaseName, user=self._dbWriteUser, password=self._dbWritePasswd) as conn:
            cur = conn.cursor()

            # check priviledge for write user
            if True:
                cur.execute("SHOW GRANTS;")
                lineList = [x[0] for x in cur.fetchall()]
                if len(lineList) != 2:
                    raise Exception("invalid priviledge for %s user" % (self._dbWriteUser))
                if not lineList[0].startswith("GRANT USAGE ON *.* TO `%s`@`localhost` IDENTIFIED BY PASSWORD " % (self._dbWriteUser)):
                    raise Exception("invalid priviledge for %s user" % (self._dbWriteUser))
                if not lineList[1] == "GRANT ALL PRIVILEGES ON `%s`.* TO `%s`@`localhost`" % (databaseName, self._dbWriteUser):
                    raise Exception("invalid priviledge for %s user" % (self._dbWriteUser))

            # check table info
            if True:
                buf = ""
                for tableName, value in tableInfo.items():
                    buf += "---- " + tableName + " ----\n"
                    buf += value[1] + "\n"
                    buf += "\n"
                if buf != McUtil.readFile(tableInfoRecordFile):
                    raise Exception("table info changed")
            if True:
                cur.execute("SHOW TABLES;")
                tableNameList = [x[0] for x in cur.fetchall()]
                buf = ""
                for tableName in tableNameList:
                    cur.execute("SHOW CREATE TABLE %s;" % (tableName))
                    out = cur.fetchall()[0][1]
                    buf += "---- " + tableName + " ----\n"
                    buf += out + "\n"
                    buf += "\n"
                if buf != McUtil.readFile(databaseTableSchemaRecordFile):
                    raise Exception("table schema in database changed")

        with mariadb.connect(unix_socket=socketFile, database=databaseName, user=self._dbReadUser) as conn:
            # check priviledge for anonymous user
            cur = conn.cursor()
            cur.execute("SHOW GRANTS;")
            lineList = [x[0] for x in cur.fetchall()]
            if len(lineList) != 2:
                raise Exception("invalid priviledge for %s user" % (self._dbWriteUser))
            if not lineList[0] == "GRANT USAGE ON *.* TO `%s`@`%%`" % (self._dbReadUser):
                raise Exception("invalid priviledge for %s user" % (self._dbWriteUser))
            if not lineList[1] == "GRANT SELECT, REFERENCES, INDEX, LOCK TABLES, EXECUTE, SHOW VIEW, EVENT, TRIGGER ON `%s`.* TO `%s`@`%%`" % (databaseName, self._dbReadUser):
                raise Exception("invalid priviledge for %s user" % (self._dbWriteUser))


class _MongodbServer:
    pass


class _MultiInstanceNeo4jServer:

    """
    The best solution would be using a one-instance-neo4j-server, and dynamically
    add databases stored in seperate directories.
    """

    def __init__(self, param):
        self.param = param
        self._dirDict = dict()              # <database-name,data-dir>
        self._procDict = dict()             # <database-name,(proc,port,cfg-file,log-ile)>
        self._dbWriteUser = "publisher"
        self._dbWritePasswd = "publisher"
        self._dbReadUser = "reader"
        self._bStarted = False

    def start(self):
        assert not self._bStarted
        self._bStarted = True
        logging.info("Slave server (multi-instanced-neo4j) started.")

    def stop(self):
        for value in self._procDict.values():
            proc = value[0]
            proc.terminate()
            proc.wait()
        self._procDict.clear()
        self._dirDict.clear()

    def addDatabaseDir(self, databaseName, dataDir):
        assert self._bStarted
        assert _checkNameAndRealPath(self._dirDict, databaseName, dataDir)

        cfgFile = os.path.join(McConst.tmpDir, "neo4j-%s.cnf" % (databaseName))
        pidFile = os.path.join(McConst.tmpDir, "neo4j-%s.pid" % (databaseName))
        logFile = os.path.join(McConst.logDir, "neo4j-%s.log" % (databaseName))
        proc = None
        port = None
        try:
            # initialize if needed
            if not self._isInitialized(dataDir):
                self._initialize(databaseName, dataDir, logFile)
                bJustInitialized = True
            else:
                bJustInitialized = False

            # generate neo4j config file
            with open(cfgFile, "w") as f:
                buf = ""
                f.write(buf)

            # allocate listening port
            port = McUtil.getFreeSocketPort("tcp")

            # start neo4j
            with open(logFile, "a") as f:
                f.write("\n\n")
                f.write("## neo4j #######################\n")
            envDict = {
                "NEO4J_CONF": os.path.dirname(cfgFile),
                "NEO4J_DATA": dataDir,
                "NEO4J_LOGS": os.path.dirname(logFile),
                "NEO4J_PIDFILE": pidFile,
            }
            proc = subprocess.Popen(["/opt/bin/neo4j", "console"], env=envDict)
            McUtil.waitSocketPortForProc("tcp", self.param.listenIp, port, proc)

            # post-initialize if needed
            if bJustInitialized:
                self._initializePostStart(databaseName)

            # check
            self._check(databaseName)

            # save
            self._dirDict[databaseName] = dataDir
            self._procDict[databaseName] = (proc, port, cfgFile, pidFile, logFile)
        except Exception:
            if databaseName in self._procDict:
                self._procDict[databaseName]
            if databaseName in self._dirDict:
                del self._dirDict[databaseName]
            if proc is not None:
                proc.terminate()
                proc.wait()
            if os.path.exists(pidFile):
                assert False
            if os.path.exists(cfgFile):
                os.unlink(cfgFile)
            raise

    def exportDatabaseDir(self, databaseName):
        # FIXME, currently addDatabaseDir does the export work which is obviously insecure
        assert self._bStarted

    def getDatabasePort(self, databaseName):
        assert self._bStarted
        return self._procDict[databaseName][1]

    def _isInitialized(self, dataDir):
        return len(os.listdir(dataDir)) > 0

    def _initialize(self, databaseName, dataDir, logFile):
        McUtil.mkDirAndClear(dataDir)

    def _initializePostStart(self, databaseName):
        pass

    def _check(self, databaseName):
        pass


def _checkNameAndRealPath(dictObj, name, realPath):
    if name in dictObj:
        return False
    if not os.path.isabs(realPath) or realPath.endswith("/"):
        return False
    if McUtil.isPathOverlap(realPath, dictObj.values()):
        return False
    return True
