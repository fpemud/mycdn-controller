#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import aiohttp
import aiohttp_jinja2


class McAdvertiser:

    def __init__(self, param):
        self.param = param
        self.param.httpServer.addRoute("GET", "/api/mirrors", self._apiMirrorsHandler)
        self.param.httpServer.addRoute("GET", "/", self._indexHandler)

    def advertiseMirrorSite(self, mirrorSiteId):
        msObj = self.param.mirrorSiteDict[mirrorSiteId]
        if "http" in msObj.advertiseProtocolList:
            self.httpServer.addFileDir(msObj.id, msObj.dataDir)
        if "ftp" in msObj.advertiseProtocolList:
            self.ftpServer.addFileDir(msObj.id, msObj.dataDir)
        if "rsync" in msObj.advertiseProtocolList:
            self.rsyncServer.addFileDir(msObj.id, msObj.dataDir)
        if "git-http" in msObj.advertiseProtocolList:
            # http server checks mirror status on the fly
            pass

    def dispose(self):
        pass

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
                "protocol": {},
            }

            for proto in msObj.advertiseProtocolList:
                if proto == "http":
                    port = self.param.advertiser.httpServer.port
                    ret[msId]["protocol"]["http"] = {
                        "url": "http://{IP}%s/m/%s" % (":%d" % (port) if port != 80 else "", msId)
                    }
                    # port = self.param.advertiser.httpServer.portHttps
                    # portStandard = self.param.advertiser.httpServer.portHttpsStandard
                    # ret[msId]["protocol"]["https"] = {
                    #     "url": "https://{IP}%s/%s" % (":%d" % (port) if port != portStandard else "", msId)
                    # }
                    continue
                if proto == "ftp":
                    port = self.param.advertiser.ftpServer.port
                    ret[msId]["protocol"]["ftp"] = {
                        "url": "ftp://{IP}%s/%s" % (":%d" % (port) if port != 21 else "", msId)
                    }
                    continue
                if proto == "rsync":
                    port = self.param.advertiser.rsyncServer.port
                    ret[msId]["protocol"]["rsync"] = {
                        "url": "rsync://{IP}%s/%s" % (":%d" % (port) if port != 873 else "", msId)
                    }
                    continue
                if proto == "git-http":
                    assert False

        return ret
