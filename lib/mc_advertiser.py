#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import jinja2
import logging
import logging.handlers
import aiohttp
import aiohttp_jinja2
from mc_param import McConst
from mc_updater import McMirrorSiteUpdater


class McGlobalAdvertiser:

    def __init__(self, param):
        self.param = param
        self.param.mainloop.run_until_complete(self._start())

    def dispose(self):
        if self._runner is not None:
            self.param.mainloop.run_until_complete(self._stop())

    async def _start(self):
        try:
            if True:
                self._app = aiohttp.web.Application(loop=self.param.mainloop)
                self._app.router.add_route("GET", "/api/mirrors", self._apiMirrorsHandler)
                self._app.router.add_route("GET", "/", self._indexHandler)
            if True:
                self._app.router.add_route("POST", "/api/mirror/{id}/update-now", self._apiMirrorsHandler)
            if True:
                self._log = logging.getLogger("aiohttp")
                self._log.propagate = False
                self._log.addHandler(logging.handlers.RotatingFileHandler(os.path.join(McConst.logDir, 'main-httpd.log'),
                                                                          maxBytes=McConst.updaterLogFileSize,
                                                                          backupCount=McConst.updaterLogFileCount))
            if True:
                aiohttp_jinja2.setup(self._app, loader=jinja2.FileSystemLoader('/usr/share/mirrors'))       # FIXME, we should use VUE alike, not jinja
                self._runner = aiohttp.web.AppRunner(self._app)
                await self._runner.setup()
                site = aiohttp.web.TCPSite(self._runner, self.param.listenIp, self.param.mainPort)
                await site.start()
        except Exception:
            await self._stop()
            raise

    async def _stop(self):
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        if self._log is not None:
            for h in self._log.handlers:
                self._log.removeHandler(h)
            self._log = None
        if self._app is not None:
            # how to dispose self._app?
            self._app = None

    async def _indexHandler(self, request):
        data = {
            "static": {
                "title": "mirror site",
                "name": "镜像名",
                "last_update_time": "上次更新时间",
                "next_update_time": "",
                "help": "使用帮助",
            },
            "mirror_site_dict": self.__getMirrorSiteDict(),
        }
        return aiohttp_jinja2.render_template('index.jinja2', request, data)

    async def _apiMirrorsHandler(self, request):
        return aiohttp.web.json_response(self.__getMirrorSiteDict())

    async def _apiMirrorUpdateNow(self, request):
        mirrorSiteId = request.match_info["id"]
        try:
            if self.param.mirrorSiteDict.get(mirrorSiteId) is None:
                raise _WebException("mirror site not found")

            if not self.param.updater.isMirrorSiteInitialized(mirrorSiteId):
                raise _WebException("mirror site has not been initialized")

            s = self.param.updater.getMirrorSiteUpdateState(mirrorSiteId)["update_status"]
            if s in [McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_UPDATING, McMirrorSiteUpdater.MIRROR_SITE_UPDATE_STATUS_MAINTAINING]:
                raise _WebException("mirror site is updating")

            self.updateMirrorSiteNow(mirrorSiteId)
            return aiohttp.web.Response()
        except _WebException as e:
            return aiohttp.web.json_response({"message": e.message}, status=400)

    def __getMirrorSiteDict(self):
        ret = dict()
        for msId, msObj in self.param.mirrorSiteDict.items():
            updateState = self.param.updater.getMirrorSiteUpdateState(msId)
            if updateState["last_update_time"] is None:
                updateState["last_update_time"] = ""
            else:
                updateState["last_update_time"] = updateState["last_update_time"].strftime("%Y-%m-%d %H:%M")
            if updateState["next_update_time"] is None:
                updateState["next_update_time"] = ""
            else:
                updateState["next_update_time"] = updateState["next_update_time"].strftime("%Y-%m-%d %H:%M")

            ret[msId] = {
                "update-status": updateState["update_status"],
                "last-update-time": updateState["last_update_time"],
                "next-update-time": updateState["next_update_time"],
                "update-progress": updateState.get("update_progress", -1),
                "help": {
                    "title": "",
                    "filename": "",
                },
            }
            if self.param.updater.isMirrorSiteInitialized(msId):
                ret[msId]["access"] = dict()
                for key in msObj.advertiserDict:
                    ret[msId]["access"][key] = self.param.advertiseDict[key].get_access_info(msId)
        return ret


class _WebException(Exception):
    pass
