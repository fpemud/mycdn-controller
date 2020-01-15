#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import logging
import jinja2
import aiohttp.web
import aiohttp_jinja2


class McHttpServer:
    """
    http server has 3 roles:
    1. api server
    2. web site
    3. accessing mirror content by http/https protocol
    """

    def __init__(self, param, mainloop, ip, port, logDir):
        assert 0 < port < 65536

        self.param = param
        self._mainloop = mainloop
        self._ip = ip
        self._port = port
        self._logDir = logDir

        self._app = aiohttp.web.Application(loop=self._mainloop)

        self._runner = None

    @property
    def portStandard(self):
        return 80

    @property
    def portHttpsStandard(self):
        return 443

    @property
    def port(self):
        assert self._runner is not None
        return self._port

    # @property
    # def portHttps(self):
    #     assert self._runner is not None
    #     return self._port

    @property
    def running(self):
        return self._runner is None

    def addFileDir(self, name, realPath):
        pass
        # FIXME
        # self._app.router.add_static("/m/" + name + "/", realPath, name=name, show_index=True, follow_symlinks=True)

    def start(self):
        self._mainloop.create_task(self._start())

    def stop(self):
        self._mainloop.run_until_complete(self._stop())

    async def _start(self):
        aiohttp_jinja2.setup(self._app, loader=jinja2.FileSystemLoader('/usr/share/mirrors'))
        self._app.router.add_route("GET", "/api/mirrors", self._apiMirrorsHandler)
        self._app.router.add_route("GET", "/", self._indexHandler)
        self._runner = aiohttp.web.AppRunner(self._app)
        await self._runner.setup()
        site = aiohttp.web.TCPSite(self._runner, self._ip, self._port)
        await site.start()
        logging.info("Advertising server (HTTP) started, listening on port %d." % (self._port))

    async def _stop(self):
        await self._runner.cleanup()

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
            ret[msId] = {
                "is_initialized": self.param.updater.isMirrorSiteInitialized(msId),
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
                    portStandard = self.param.advertiser.httpServer.portStandard
                    ret[msId]["protocol"]["http"] = {
                        "url": "http://{IP}%s/%s" % (":%d" % (port) if port != portStandard else "", msId)
                    }
                    # port = self.param.advertiser.httpServer.portHttps
                    # portStandard = self.param.advertiser.httpServer.portHttpsStandard
                    # ret[msId]["protocol"]["https"] = {
                    #     "url": "https://{IP}%s/%s" % (":%d" % (port) if port != portStandard else "", msId)
                    # }
                    continue
                if proto == "ftp":
                    port = self.param.advertiser.ftpServer.port
                    portStandard = self.param.advertiser.ftpServer.portStandard
                    ret[msId]["protocol"]["ftp"] = {
                        "url": "ftp://{IP}%s/%s" % (":%d" % (port) if port != portStandard else "", msId)
                    }
                    continue
                if proto == "rsync":
                    port = self.param.advertiser.rsyncServer.port
                    portStandard = self.param.advertiser.rsyncServer.portStandard
                    ret[msId]["protocol"]["rsync"] = {
                        "url": "rsync://{IP}%s/%s" % (":%d" % (port) if port != portStandard else "", msId)
                    }
                    continue
                if proto == "git-http":
                    assert False
        return ret


# class HttpServer2:

#     def __init__(self, param):
#         self.param = param

#     @property
#     def port(self):
#         return self._port

#     @property
#     def running(self):
#         return False

#     def start(self):
#         assert self.soupServer is None
#         self.soupServer = SoupServer()
#         self.soupServer.listen_all()
#         self.soupServer.add_handler(None, server_callback, None, None)

#         self.jinaEnv = jinja2.Environment(loader=jinja2.FileSystemLoader(self.param.shareDir),
#                                           autoescape=select_autoescape(['html', 'xml']))

#     def stop(self):
#         assert self._proc is not None

#     def _callback(self):
#         pass

#     def _generateHomePage(self):
#         template = self.jinaEnv.get_template('index.html')

#         env = None
#         template = jinja2.Template('Hello {{ name }}!')
#         template.render(name='John Doe')
