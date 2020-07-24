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
        if "file" in msObj.advertiseDict:
            if "http" in msObj.advertiseDict["file"]:
                self.param.httpServer.addFileDir(msObj.id, msObj.storageDict["file"].cacheDir)
            if "ftp" in msObj.advertiseDict["file"]:
                self.param.ftpServer.addFileDir(msObj.id, msObj.storageDict["file"].cacheDir)
            if "rsync" in msObj.advertiseDict["file"]:
                self.param.rsyncServer.addFileDir(msObj.id, msObj.storageDict["file"].cacheDir)
        if "git" in msObj.advertiseDict:
            if "git" in msObj.advertiseDict["git"]:
                assert False        # FIXME
            if "ssh" in msObj.advertiseDict["git"]:
                assert False        # FIXME
            if "http" in msObj.advertiseDict["git"]:
                pass                # FIXME

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
            }

            if "file" in msObj.advertiseDict:
                ret[msId]["interface-file"] = dict()
                for proto in msObj.advertiseDict["file"]:
                    if proto == "http":
                        port = self.param.httpServer.port
                        ret[msId]["interface-file"]["http"] = {
                            "url": "http://{IP}%s/m/%s" % (":%d" % (port) if port != 80 else "", msId)
                        }
                        # port = self.param.advertiser.httpServer.portHttps
                        # portStandard = self.param.advertiser.httpServer.portHttpsStandard
                        # ret[msId]["interface-file"]["https"] = {
                        #     "url": "https://{IP}%s/%s" % (":%d" % (port) if port != portStandard else "", msId)
                        # }
                        continue
                    if proto == "ftp":
                        port = self.param.ftpServer.port
                        ret[msId]["interface-file"]["ftp"] = {
                            "url": "ftp://{IP}%s/%s" % (":%d" % (port) if port != 21 else "", msId)
                        }
                        continue
                    if proto == "rsync":
                        port = self.param.rsyncServer.port
                        ret[msId]["interface-file"]["rsync"] = {
                            "url": "rsync://{IP}%s/%s" % (":%d" % (port) if port != 873 else "", msId)
                        }
                        continue

                # deprecated
                ret[msId]["protocol"] = ret[msId]["interface-file"]

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
