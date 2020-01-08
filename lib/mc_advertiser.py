#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import subprocess
from mc_util import McUtil
from mc_util import HttpFileServer
from mc_util import FtpServer
from mc_util import RsyncServer
from mc_param import McConst


class McAdvertiser:

    def __init__(self, param):
        self.param = param

        self.httpDirDict = dict()       # dict<mirror-id,data-dir>
        self.ftpDirDict = dict()        # dict<mirror-id,data-dir>
        self.rsyncDirDict = dict()      # dict<mirror-id,data-dir>
        for ms in self.param.mirrorSiteList:
            for proto in ms.advertiseProtocolList:
                if proto == "http":
                    self.httpDirDict[ms.id] = ms.dataDir
                elif proto == "ftp":
                    self.ftpDirDict[ms.id] = ms.dataDir
                elif proto == "rsync":
                    self.rsyncDirDict[ms.id] = ms.dataDir
                else:
                    assert False

        self.httpServer = None
        if len(self.httpDirDict) > 0:
            if self.param.httpPort == "random":
                self.param.httpPort = McUtil.getFreeSocketPort("tcp")
            self.httpServer = AioHttpFileServer(self.param.listenIp, self.param.httpPort, list(self.httpDirDict.values()), McConst.logDir)
            self.param.mainloop.call_soon(self.httpServer.start())

        self.ftpServer = None
        if len(self.ftpDirDict) > 0:
            if self.param.ftpPort == "random":
                self.param.ftpPort = McUtil.getFreeSocketPort("tcp")
            self.ftpServer = AioFtpServer(self.param.listenIp, self.param.ftpPort, list(self.ftpDirDict.values()), McConst.logDir)
            self.param.mainloop.call_soon(self.ftpServer.start())

        self.rsyncServer = None
        if len(self.rsyncDirDict) > 0:
            if self.param.rsyncPort == "random":
                self.param.rsyncPort = McUtil.getFreeSocketPort("tcp")
            self.rsyncServer = RsyncServer(self.param.listenIp, self.param.rsyncPort, list(self.rsyncDirDict.values()), McConst.tmpDir, McConst.logDir)
            self.param.mainloop.call_soon(self.rsyncServer.start())

    def dispose(self):
        if self.httpServer is not None:
            self.param.mainloop.run_until_complete(self.httpServer.stop())
            self.httpServer = None
        if self.ftpServer is not None:
            self.param.mainloop.run_until_complete(self.ftpServer.stop())
            self.ftpServer = None
        if self.rsyncServer is not None:
            self.param.mainloop.run_until_complete(self.rsyncServer.stop())
            self.rsyncServer = None


class _HttpServer:

    def __init__(self, mainloop, ip, port, logDir):
        assert 0 < port < 65536

        self._ip = ip
        self._port = port
        self._dirDict = dict()
        self._logDir = logDir

        self._app = web.Application(loop=mainloop)
        self._runner = None

    @property
    def port(self):
        return self._port

    @property
    def running(self):
        return self._runner is None

    def addFileDir(self, name, realPath):
        self._dirDict[name] = realPath
        self._app.router.add_static("/" + name + "/", realPath, name=name, show_index=True, follow_symlinks=True)

    async def start(self):
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._ip, self._port)
        await site.start()

    async def stop(self):
        await self._runner.cleanup()


class _FtpServer(aioftp.AbstractPathIO):

    def __init__(self, mainloop, ip, port, logDir):
        assert 0 < port < 65536

        self._mainloop = mainloop
        self._ip = ip
        self._port = port
        self._dirDict = dict()
        self._logDir = logDir

        self._server = aioftp.Server(path_io_factory=self)
        self._bStart = False

    @property
    def port(self):
        return self._port

    @property
    def running(self):
        return self._bStart

    def addFileDir(self, name, realPath):
        self._dirDict[name] = realPath

    async def start(self):
        await self._server.start(self._ip, self._port)
        self._bStart = True

    async def stop(self):
        await self._server.cleanup()
        self._bStart = False


class _FtpServerPathIO:

    def __init__(self, *, timeout=None, loop=None, root=None):
        super().__init__(timeout=timeout, loop=loop)
        self.root = root

    def __repr__(self):
        return repr(self.root)

    def get_node(self, path):
        node = None
        nodes = [self.root]
        for part in path.parts:
            if not isinstance(nodes, list):
                return

            for node in nodes:
                if node.name == part:
                    nodes = node.content
                    break
            else:
                return

        return node

    async def exists(self, path):
        return self.get_node(path) is not None

    async def is_dir(self, path):
        node = self.get_node(path)
        return not (node is None or node.kind != ItemKind.room)

    async def is_file(self, path):
        node = self.get_node(path)
        return not (node is None or node.kind == ItemKind.room)

    async def mkdir(self, path, *, parents=False):
        if self.get_node(path):
            raise FileExistsError
        elif not parents:
            parent = self.get_node(path.parent)
            if parent is None:
                raise FileNotFoundError
            elif not parent.kind == ItemKind.room:
                raise FileExistsError
            node = Room(path.name)
            parent.add_child(node)
        else:
            nodes = [self.root]
            parent = self.root
            for part in path.parts:
                if isinstance(nodes, list):
                    for node in nodes:
                        if node.name == part:
                            nodes = node.content
                            parent = node
                            break
                    else:
                        new_node = Room(name=part)
                        parent.add_child(new_node)
                        nodes = new_node.content
                        parent = new_node
                else:
                    raise FileExistsError

    async def rmdir(self, path):
        node = self.get_node(path)
        if node is None:
            raise FileNotFoundError
        elif node.kind != ItemKind.room:
            raise NotADirectoryError
        elif node.content:
            raise OSError("Directory not empty")
        else:
            node.remove()

    async def unlink(self, path):
        node = self.get_node(path)
        if node is None:
            raise FileNotFoundError
        elif node.kind == ItemKind.room:
            raise IsADirectoryError
        else:
            node.remove()

    async def list(self, path):
        node = self.get_node(path)
        if node is None or node.kind != ItemKind.room:
            return ()
        else:
            names = map(operator.attrgetter("name"), node.content)
            paths = map(lambda name: path / name, names)
            return tuple(paths)

    async def stat(self, path):
        node = self.get_node(path)
        if node is None:
            raise FileNotFoundError
        else:
            size = len(node.content)
            return self.Stats(
                size,
                0,
                0,
                1,
                0o100777,
            )

    async def open(self, path, mode="rb", *args, **kwargs):
        if mode == "rb":
            node = self.get_node(path)
            if node is None:
                raise FileNotFoundError
            data = node.content.encode('utf-8')
            file_like = io.BytesIO(data)
        elif mode in ("wb", "ab"):
            node = self.get_node(path)
            parent = self.get_node(path.parent)
            if parent is None or parent.kind != ItemKind.room:
                raise FileNotFoundError

            if node is None:
                file_like = (io.BytesIO(), parent, path.name)
            elif node.kind != ItemKind.regular:
                raise IsADirectoryError
            else:
                previous_content = node.content
                node.remove()
                if mode == "wb":
                    file_like = (io.BytesIO(), parent, path.name)
                else:
                    file_like = (io.BytesIO(previous_content.encode('utf-8')), parent, path.name)
        else:
            raise ValueError(str.format("invalid mode: {}", mode))

        return file_like

    async def write(self, file, data):
        if isinstance(file, tuple):
            (stream, parent, name) = file
            stream.write(data)
            # file.mtime = int(time.time())

    async def read(self, file, count=None):
        return file.read(count)

    async def close(self, file):
        if isinstance(file, tuple):
            # we're writing to a file, so commit the whole thing to the item tree
            (stream, parent, name) = file

            data = stream.getvalue().decode()
            parent.add_child(GameItem(name, content=data))
        else:
            pass

    async def rename(self, source, destination):
        if source != destination:
            sparent = self.get_node(source.parent)
            dparent = self.get_node(destination.parent)
            snode = self.get_node(source)
            if None in (snode, dparent):
                raise FileNotFoundError

            for i, node in enumerate(sparent.content):
                if node.name == source.name:
                    node.remove()

            snode.name = destination.name
            for i, node in enumerate(dparent.content):
                if node.name == destination.name:
                    dparent.content[i] = snode
                    break
            else:
                dparent.add_child(snode)
