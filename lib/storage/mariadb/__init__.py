#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import logging
import sqlparse
import lxml.etree
import subprocess
from mc_util import McUtil


class Storage:

    @staticmethod
    def get_properties():
        return {
            "with-integrated-advertiser": True,
        }

    def __init__(self, param):
        self._listenIp = param["listen-ip"]
        self._tmpDir = param["temp-directory"]
        self._logDir = param["log-directory"]
        self._mirrorSiteDict = param["mirror-sites"]
        self._tableInfoDict = dict()                    # {mirror-site-id:{table-name:table-sql}}
        self._bAdvertiseDict = dict()                   # {mirror-site-id:bAdvertise}

        # create data directory and table information structure
        for msId in self._mirrorSiteDict:
            xmlElem = lxml.etree.fromstring(self._mirrorSiteDict[msId]["config-xml"])

            # create table information structure
            self._tableInfoDict[msId] = dict()
            if True:
                tl = xmlElem.xpath(".//database-schema")
                if len(tl) > 0:
                    dbSchemaFile = os.path.join(self._mirrorSiteDict[msId]["plugin-directory"], tl[0].text)
                    for sql in sqlparse.split(McUtil.readFile(dbSchemaFile)):
                        m = re.match("^CREATE +TABLE +(\\S+)", sql)
                        if m is None:
                            raise Exception("mirror site %s: invalid mariadb database schema" % (msId))
                        self._tableInfoDict[msId][m.group(1)] = (-1, sql)
            # get advertise flag
            self._bAdvertiseDict[msId] = (len(xmlElem.xpath(".//advertise")) > 0)

        self._mariadbServer = None
        try:
            self._mariadbServer = _MultiInstanceMariadbServer(self._listenIp, self._tmpDir, self._logDir)
            self._mariadbServer.start()
            for msId in self._mirrorSiteDict:
                self._mariadbServer.addDatabaseDir(msId, self._mirrorSiteDict[msId]["data-directory"], self._tableInfoDict[msId], None, None)
        except Exception:
            self.dispose()
            raise

    def dispose(self):
        if self._mariadbServer is not None:
            self._mariadbServer.stop()
            self._mariadbServer = None

    def get_param(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "port": self._mariadbServer.port,
            "database": mirror_site_id,
        }

    def get_access_info(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "url": "mariadb://{IP}:%d/%s" % (self._mariadbServer.port, mirror_site_id),
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        self._mariadbServer.exportDatabase(mirror_site_id)


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
