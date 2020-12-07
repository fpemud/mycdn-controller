#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os


class StorageEngine:

    def __init__(self, listenIp, tmpDir, logDir):
        self._listenIp = listenIp
        self._tmpDir = tmpDir
        self._logDir = logDir

        self._bStart = False
        self._neo4jServer = None 

        self._mirrorSiteDict = dict()

    def addMirrorSite(self, mirrorSiteId, masterDir, xmlElem):
        assert not self._bStart

        msParam = _Param()
        msParam.masterDir = masterDir
        msParam.dataDir = os.path.join(masterDir, "storage-mongodb")

        tl = xmlElem.xpathEval(".//database-schema")
        if len(tl) > 0:
            databaseSchemaFile = os.path.join(pluginDir, tl[0].getContent())
            for sql in sqlparse.split(McUtil.readFile(databaseSchemaFile)):
                m = re.match("^CREATE +TABLE +(\\S+)", sql)
                if m is None:
                    raise Exception("mirror site %s: invalid database schema for storage type %s" % (self.id, st))
                msParam.tableInfo[m.group(1)] = (-1, sql)

    def start(self):
        assert not self._bStart

        for msId, msParam in self._mirrorSiteDict.items():
            _Util.ensureDir(self._mirrorSiteDict[msId].dataDir)
            if self._neo4jServer is None:
                self._neo4jServer = _MultiInstanceMariadbServer(self._listenIp, self._tmpDir, self._logDir)
                self._neo4jServer.start()
        self._bStart = True

    def stop(self):
        assert self._bStart

        self._bStart = False
        if self._neo4jServer is not None:
            self._neo4jServer.stop()
            self._neo4jServer = None

    def getPluginParam(self, mirrorSiteId):
        assert self._bStart

        return {
            "data-directory": self._mirrorSiteDict[mirrorSiteId].dataDir,
        }

    def advertiseMirrorSite(self, mirrorSiteId):
        assert self._bStart
        self._neo4jServer.exportDatabase(mirrorSiteId)


class _Param:

    def __init__(self):
        self.masterDir = None
        self.dataDir = None
        self.tableInfo = OrderedDict()
