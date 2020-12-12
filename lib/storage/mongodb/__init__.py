#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
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

        self._serverDict = dict()                       # {mirror-site-id:mongodb-server-object}
        try:
            # create server objects
            # The best solution would be using a one-instance-mongodb-server, but we can not do it
            # See the comment in "class storage.mariadb.Storage"
            for msId in self._mirrorSiteDict:
                self._serverDict[msId] = _MongodbServer(param["listen-ip"], param["temp-directory"], param["log-directory"],
                                                        msId, self._mirrorSiteDict[msId]["data-directory"])
            # show log
            if any(self._bAdvertiseDict.values()):
                logging.info("Advertiser (mongodb) started.")       # here we can not give out port information
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
            "url": "mongodb://{IP}:%d/%s" % (self._serverDict[mirror_site_id].dbPort, mirror_site_id),
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        # FIXME
        pass


class _MongodbServer:

    def __init__(self, listenIp, tmpDir, logDir, databaseName, dataDir, tableInfo):
        self._cfgFile = os.path.join(tmpDir, "advertiser-mongodb-%s.conf" % (databaseName))
        self._logFile = os.path.join(logDir, "advertiser-mongodb-%s.log" % (databaseName))
        self._tmpDir = tmpDir

        self._port = None
        self._proc = None
        try:
            # allocate listening port
            self._port = McUtil.getFreeSocketPort("tcp")

            # generate mariadb config file
            with open(self._cfgFile, "w") as f:
                buf = ""
                buf += 'storage:\n'
                buf += '    dbPath: "%s"\n' % (dataDir)
                buf += '\n'
                buf += 'systemLog:\n'
                buf += '    destination: file\n'
                buf += '    path: "%s"\n' % (self._logFile)
                buf += '    logAppend: true\n'
                buf += '\n'
                buf += 'net:\n'
                buf += '    bindIp: %s\n' % (listenIp)
                buf += '    port: %d\n' % (self._port)
                buf += '    unixDomainSocket:\n'
                buf += '        pathPrefix: %s\n' % (self._tmpDir)
                f.write(buf)

            # start mongodb
            with open(self._logFile, "a") as f:
                f.write("\n\n")
                f.write("## mongodb #######################\n")
            self._proc = subprocess.Popen(["/usr/bin/mongod", "--config", self._cfgFile], cwd=self._tmpDir)
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
        if os.path.exists(self._cfgFile):
            os.unlink(self._cfgFile)

    @property
    def dbSocketFile(self):
        return os.path.join(self._tmpDir, "mongodb-%d.sock" % (self._port))

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
