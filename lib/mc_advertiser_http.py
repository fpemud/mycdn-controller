#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import jinja2
import pathlib
import logging
import asyncio
import functools
import aiohttp.web
import aiohttp_jinja2
from mc_util import AsyncIteratorExecuter


class McHttpServer:

    def __init__(self, param, mainloop, ip, port, logDir):
        assert 0 < port < 65536

        self.param = param
        self._mainloop = mainloop
        self._ip = ip
        self._port = port
        self._logDir = logDir

        self._app = aiohttp.web.Application(loop=self._mainloop)
        aiohttp_jinja2.setup(self._app, loader=jinja2.FileSystemLoader('/usr/share/mirrors'))

        self._runner = None

    @property
    def port(self):
        assert self._runner is not None
        return self._port

    @property
    def running(self):
        return self._runner is None

    def addFileDir(self, name, realPath):
        self._app.router.add_static("/" + name + "/", realPath, name=name, show_index=True, follow_symlinks=True)

    def start(self):
        self._mainloop.create_task(self._start())

    def stop(self):
        self._mainloop.run_until_complete(self._stop())

    async def _start(self):
        self._runner = aiohttp.web.AppRunner(self._app)
        await self._runner.setup()
        site = aiohttp.web.TCPSite(self._runner, self._ip, self._port)
        await site.start()
        logging.info("Advertising server (HTTP) started, listening on port %d." % (self._port))

    async def _stop(self):
        await self._runner.cleanup()

    @aiohttp_jinja2.template('index.jinja2')
    def _handler(self, request):
        ret = {
            "static": {
                "title": "mirror site",
                "name": "镜像名",
                "update_time": "上次更新时间",
                "help": "使用帮助",
            }
        }

        ret["mirror_site_list"] = []
        for msId, msObj in self.param.mirrorSiteDict.items():
            msData = {
                "id": msObj.id,
                "is_initialized": self.param.updater.isMirrorSiteInitialized(msObj.id),
                "update_status": None,
                "last_update_time": None,
                "help": {
                    "title": None,
                    "filename": None,
                }
            }
            ret["mirror_site_list"].append(msData)

        return ret
































class HttpServer2:

    def __init__(self, param):
        self.param = param

    @property
    def port(self):
        return self._port

    @property
    def running(self):
        return False

    def start(self):
        assert self.soupServer is None
        self.soupServer = SoupServer()
        self.soupServer.listen_all()
        self.soupServer.add_handler(None, server_callback, None, None)

        self.jinaEnv = jinja2.Environment(loader=jinja2.FileSystemLoader(self.param.shareDir),
                                          autoescape=select_autoescape(['html', 'xml']))

    def stop(self):
        assert self._proc is not None

    def _callback(self):
        pass

    def _generateHomePage(self):
        template = self.jinaEnv.get_template('index.html')

        env = None
        template = jinja2.Template('Hello {{ name }}!')
        template.render(name='John Doe')
