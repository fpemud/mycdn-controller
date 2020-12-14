#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import re
import logging
import mariadb
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
        self._mirrorSiteDict = param["mirror-sites"]
        self._tableInfoDict = dict()                    # {mirror-site-id:{table-name:table-sql}}
        self._bAdvertiseDict = dict()                   # {mirror-site-id:bAdvertise}

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

        self._serverDict = dict()                                   # {mirror-site-id:mariadb-server-object}
        try:
            # create server objects
            # The best solution would be using a one-instance-mariadb-server, and dynamically
            # add table files stored in seperate directories as different databases.
            # Although basically mariadb supports this kind of operation, but there're
            # corner cases (for example when the server crashes).
            for msId in self._mirrorSiteDict:
                self._serverDict[msId] = _MariadbServer(param["listen-ip"], param["temp-directory"], param["log-directory"],
                                                        msId,
                                                        self._mirrorSiteDict[msId]["state-directory"],
                                                        self._mirrorSiteDict[msId]["data-directory"],
                                                        self._tableInfoDict[msId])
            # show log
            if any(self._bAdvertiseDict.values()):
                logging.info("Advertiser (mariadb) started.")       # here we can not give out port information
        except Exception:
            self.dispose()
            raise

    def dispose(self):
        for msObj in self._serverDict.values():
            msObj.dispose()
        self._serverDict = dict()

    def get_param(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "unix-socket-file": self._serverDict[mirror_site_id].dbSocketFile,
            "database": mirror_site_id,
        }

    def get_access_info(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "url": "mariadb://{IP}:%d/%s" % (self._serverDict[mirror_site_id].dbPort, mirror_site_id),
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        self._serverDict[mirror_site_id].exportDatabase(mirror_site_id)


class _MariadbServer:

    def __init__(self, listenIp, tmpDir, logDir, databaseName, stateDir, dataDir, tableInfo):
        self._cfgFile = os.path.join(tmpDir, "mariadb-%s.cnf" % (databaseName))
        self._pidFile = os.path.join(tmpDir, "mariadb-%s.pid" % (databaseName))
        tableInfoRecordFile = os.path.join(stateDir, "MARIADB_TABLE_RECORD")
        tableSchemaRecordFile = os.path.join(stateDir, "MARIADB_TABLE_SCHEMA_RECORD")

        self._dbSocketFile = os.path.join(tmpDir, "mariadb-%s.socket" % (databaseName))
        self._dbWriteUser = "write"
        self._dbWritePasswd = "write"
        self._dbReadUser = "anonymous"

        self._port = None
        self._proc = None
        try:
            # initialize if needed
            if not self._isInitialized(dataDir):
                self._initialize(databaseName, dataDir, tableInfo,
                                 os.path.join(logDir, "mariadb-install-db-%s.log" % (databaseName)))
                bJustInitialized = True
            else:
                bJustInitialized = False

            # allocate listening port
            self._port = McUtil.getFreeSocketPort("tcp")

            # generate mariadb config file
            with open(self._cfgFile, "w") as f:
                buf = ""
                buf += "[mariadb]\n"
                if True:
                    buf += "pid-file = %s\n" % (self._pidFile)
                if True:
                    buf += "socket = %s\n" % (self._dbSocketFile)
                    buf += "bind-address = %s\n" % (listenIp)
                    buf += "port = %d\n" % (self._port)
                if True:
                    buf += "datadir = %s\n" % (dataDir)
                    buf += "transaction-isolation = SERIALIZABLE\n"
                if True:
                    buf += "log-error = %s\n" % (os.path.join(logDir, "mariadb-%s.err" % (databaseName)))
                f.write(buf)

            # start mariadb
            self._proc = subprocess.Popen(["/usr/sbin/mysqld", "--defaults-file=%s" % (self._cfgFile)], cwd=tmpDir)
            McUtil.waitSocketPortForProc("tcp", listenIp, self._port, self._proc)

            # post-initialize if needed
            if bJustInitialized:
                self._initializePostStart(databaseName, tableInfo, tableInfoRecordFile, tableSchemaRecordFile, self._dbSocketFile)

            # check
            self._check(databaseName, tableInfo, tableInfoRecordFile, tableSchemaRecordFile, self._dbSocketFile)
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
        if os.path.exists(self._pidFile):
            os.unlink(self._pidFile)
        if os.path.exists(self._cfgFile):
            os.unlink(self._cfgFile)

    @property
    def dbSocketFile(self):
        return self._dbSocketFile

    @property
    def dbPort(self):
        return self._port

    @property
    def dbReadUser(self):
        return self._dbReadUser

    @property
    def dbWriteUser(self):
        return self._dbWriteUser

    @property
    def dbWritePasword(self):
        return self._dbWritePasswd

    def exportDatabaseDir(self, databaseName):
        # FIXME, currently addDatabaseDir does the export work which is obviously insecure
        pass

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

    def _initializePostStart(self, databaseName, tableInfo, tableInfoRecordFile, tableSchemaRecordFile, socketFile):
        # record table schema
        #
        # two seperate files "tableInfoRecordFile" and "tableSchemaRecordFile" must be used
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
            with open(tableSchemaRecordFile, "w") as f:
                for tableName in tableNameList:
                    cur.execute("SHOW CREATE TABLE %s;" % (tableName))
                    out = cur.fetchall()[0][1]
                    f.write("---- " + tableName + " ----\n")
                    f.write(out + "\n")
                    f.write("\n")

    def _check(self, databaseName, tableInfo, tableInfoRecordFile, tableSchemaRecordFile, socketFile):
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
                if buf != McUtil.readFile(tableSchemaRecordFile):
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
