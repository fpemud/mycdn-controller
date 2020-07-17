#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import logging
import jinja2
import aiohttp.web
import aiohttp_jinja2
from mc_util import McUtil


class McHttpServer:

    def __init__(self, serverName, mainloop, ip, port, logDir):
        assert port == "random" or 0 < port < 65536

        self._serverName = serverName
        self._mainloop = mainloop
        self._ip = ip
        self._port = port
        self._logDir = logDir

        self._userSet = set()
        self._app = aiohttp.web.Application(loop=self._mainloop)

        self._bStart = False
        self._runner = None

    @property
    def port(self):
        assert self._runner is not None
        return self._port

    def useBy(self, user):
        assert not self._bStart
        self._userSet.add(user)

    def start(self):
        assert not self._bStart
        self._bStart = True
        try:
            if len(self._userSet) > 0:
                self._mainloop.run_until_complete(self._start())
                logging.info("%s started, listening on port %d." % (self._serverName, self._port))
        except Exception:
            self._bStart = False
            raise

    def stop(self):
        assert self._bStart
        if self._runner is not None:
            self._mainloop.run_until_complete(self._stop())
        self._bStart = False

    def isStarted(self):
        return self._bStart

    def isRunning(self):
        assert self._bStart
        return self._runner is not None

    def addRoute(self, method, path, handler):
        assert self._runner is not None
        with _UnfrozenApp(self._app):
            self._app.router.add_route(method, path, handler)

    def addFileDir(self, name, realPath):
        assert self._runner is not None
        with _UnfrozenApp(self._app):
            self._app.router.add_static("/m/" + name + "/", realPath, name=name, show_index=True, follow_symlinks=True)

    async def _start(self):
        if self._port == "random":
            self._port = McUtil.getFreeSocketPort("tcp")
        aiohttp_jinja2.setup(self._app, loader=jinja2.FileSystemLoader('/usr/share/mirrors'))       # FIXME, we should use VUE alike, not jinja
        self._runner = aiohttp.web.AppRunner(self._app)
        await self._runner.setup()
        site = aiohttp.web.TCPSite(self._runner, self._ip, self._port)
        await site.start()

    async def _stop(self):
        await self._runner.cleanup()
        self._runner = None


class _UnfrozenApp:

    def __init__(self, app):
        self._app = app

    def __enter__(self):
        self._tmp = self._app.router._frozen
        self._app.router._frozen = False

    def __exit__(self, type, value, traceback):
        self._app.router._frozen = self._tmp
