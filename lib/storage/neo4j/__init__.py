#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import shutil
import logging
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
        self._bAdvertiseDict = dict()                   # {mirror-site-id:bAdvertise}

        self._serverDict = dict()                       # {mirror-site-id:neo4j-server-object}
        try:
            # create server objects
            # The best solution would be using a one-instance-neo4j-server, but we can not do it
            # See the comment in "class storage.mariadb.Storage"
            for msId in self._mirrorSiteDict:
                self._serverDict[msId] = _Neo4jServer(param["listen-ip"], param["temp-directory"], param["log-directory"],
                                                      msId, self._mirrorSiteDict[msId]["data-directory"])
            # show log
            if any(self._bAdvertiseDict.values()):
                logging.info("Advertiser (neo4j) started.")       # here we can not give out port information
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
            "url-bolt": "bolt://{IP}:%d" % (self._serverDict[mirror_site_id].dbBoltPort),
            "url-http": "http://{IP}:%d" % (self._serverDict[mirror_site_id].dbHttpPort),
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        # FIXME
        pass


class _Neo4jServer:

    def __init__(self, listenIp, tmpDir, logDir, databaseName, dataDir, tableInfo):
        self._cfgDir = os.path.join(tmpDir, "advertiser-neo4j-%s.conf" % (databaseName))
        self._logDir = os.path.join(logDir, "advertiser-neo4j-%s.log" % (databaseName))
        self._tmpDir = tmpDir

        self._port = None
        self._proc = None
        try:
            # allocate listening port
            self._boltPort = McUtil.getFreeSocketPort("tcp")
            self._httpPort = 20000

            # generate mariadb config file
            os.mkdir(self._cfgDir)
            os.mkdir(self._logDir)
            with open(os.path.join(self._cfgDir, "neo4j.conf"), "w") as f:
                buf = ""
                buf += "dbms.default_database=%s" % (databaseName)
                buf += "dbms.default_advertised_address=%s" % (listenIp)
                buf += "dbms.connector.bolt.listen_address=:%d" % (self._boltPort)
                buf += "dbms.connector.http.listen_address=:%d" % (self._httpPort)
                buf += "dbms.directories.data=%s\n" % (dataDir)
                buf += "sdbms.directories.logs=%s\n" % (self._logDir)
                f.write(buf)
                # buf += "dbms.connector.bolt.advertised_address=%s:%d" % (listenIp, self._boltPort)        FIXME
                # buf += "dbms.connector.http.advertised_address=%s:%d" % (listenIp, self._boltPort)        FIXME
                # buf += "dbms.directories.dumps.root"                                                      FIXME

            # start neo4j
            with open(self._logFile, "a") as f:
                f.write("\n\n")
                f.write("## neo4j #######################\n")
            self._proc = subprocess.Popen(["/opt/neo4j-community-3.5.8/bin/neo4j", "console"],
                                          env={"NEO4J_CONF": self._cfgDir},
                                          cwd=self._tmpDir)
            McUtil.waitSocketPortForProc("tcp", listenIp, self._port, self._proc)
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
        if os.path.exists(self._cfgDir):
            shutil.rmtree(self._cfgDir)

    @property
    def dbSocketFile(self):
        return os.path.join(self._tmpDir, "mongodb-%d.sock" % (self._port))

    @property
    def dbBoltPort(self):
        return self._boltPort

    @property
    def dbHttpPort(self):
        return self._httpPort

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
