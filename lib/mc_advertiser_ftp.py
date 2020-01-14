#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import aioftp
import pathlib
import logging
import asyncio
import functools
from mc_util import AsyncIteratorExecuter


class McFtpServer:

    def __init__(self, mainloop, ip, port, logDir):
        assert 0 < port < 65536

        self._mainloop = mainloop
        self._ip = ip
        self._port = port
        self._dirDict = dict()
        self._logDir = logDir

        self._server = None
        self._bStart = False

    @property
    def port(self):
        assert self._bStart
        return self._port

    @property
    def running(self):
        return self._bStart

    def addFileDir(self, name, realPath):
        self._dirDict[name] = realPath

    def start(self):
        assert not self._bStart
        self._mainloop.create_task(self._start())

    def stop(self):
        self._mainloop.run_until_complete(self._stop())

    async def _start(self):
        self._server = aioftp.Server(path_io_factory=functools.partial(_FtpServerPathIO, parent=self))
        await self._server.start(self._ip, self._port)
        self._bStart = True
        logging.info("Advertising server (FTP) started, listening on port %d." % (self._port))

    async def _stop(self):
        # it seems aioftp.Server.close() has syntax error
        # await self._server.close()
        self._bStart = False


def _ftp_server_universal_exception(func):
    """
    Decorator. Reraising any exception (with exceptions) with universal exception :py:class:`aioftp.PathIOError`
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (NotImplementedError, aioftp.errors.PathIOError) as e:
            print(str(e))
            raise
        except Exception as e:
            print(str(e))
            raise aioftp.errors.PathIOError(reason=sys.exc_info())

    return wrapper


def _async_ftp_server_universal_exception(coro):
    """
    Decorator. Reraising any exception (with exceptions) with universal exception :py:class:`aioftp.PathIOError`
    """
    @functools.wraps(coro)
    async def wrapper(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except (asyncio.CancelledError, NotImplementedError, StopAsyncIteration, aioftp.errors.PathIOError) as e:
            print(str(e))
            raise
        except Exception as e:
            print(str(e))
            raise aioftp.errors.PathIOError(reason=sys.exc_info())

    return wrapper


def _async_ftp_server_defend_file_methods(coro):
    """
    Decorator. Raises exception when file methods called with wrapped by :py:class:`aioftp.AsyncPathIOContext` file object.
    """
    @functools.wraps(coro)
    async def wrapper(self, file, *args, **kwargs):
        if isinstance(file, aioftp.AsyncPathIOContext):
            raise ValueError("Native path io file methods can not be used with wrapped file object")
        return await coro(self, file, *args, **kwargs)
    return wrapper


class _FtpServerPathIO(aioftp.AbstractPathIO):

    def __init__(self, *kargs, parent=None, **kwargs):
        super().__init__(kargs, kwargs)
        self._dirDict = parent._dirDict

    @_async_ftp_server_universal_exception
    async def exists(self, path):
        if self._isVirtualDir(path):
            return True
        else:
            path = self._convertPath(path)
            return path.exists()

    @_async_ftp_server_universal_exception
    async def is_dir(self, path):
        if self._isVirtualDir(path):
            return True
        else:
            path = self._convertPath(path)
            return path.is_dir()

    @_async_ftp_server_universal_exception
    async def is_file(self, path):
        if self._isVirtualDir(path):
            return False
        else:
            path = self._convertPath(path)
            return path.is_file()

    @_async_ftp_server_universal_exception
    async def mkdir(self, path, *, parents=False, exist_ok=False):
        # realonly ftp server
        assert False

    @_async_ftp_server_universal_exception
    async def rmdir(self, path):
        # realonly ftp server
        assert False

    @_async_ftp_server_universal_exception
    async def unlink(self, path):
        # realonly ftp server
        assert False

    def list(self, path):
        if path.as_posix() == ".":
            ret = sorted(self._dirDict.keys())
            ret = (pathlib.Path(x) for x in ret)
            return AsyncIteratorExecuter(ret)
        else:
            path = self._convertPath(path)
            return AsyncIteratorExecuter(_ftp_server_universal_exception(path.glob)("*"))

    @_async_ftp_server_universal_exception
    async def stat(self, path):
        if path.as_posix() == ".":
            return pathlib.Path("/").stat()         # FIXME
        else:
            path = self._convertPath(path)
            return path.stat()

    @_async_ftp_server_universal_exception
    async def _open(self, path, *args, **kwargs):
        path = self._convertPath(path)
        return path.open(*args, **kwargs)

    @_async_ftp_server_universal_exception
    @_async_ftp_server_defend_file_methods
    async def seek(self, file, *args, **kwargs):
        return file.seek(*args, **kwargs)

    @_async_ftp_server_universal_exception
    @_async_ftp_server_defend_file_methods
    async def write(self, file, *args, **kwargs):
        # realonly ftp server
        assert False

    @_async_ftp_server_universal_exception
    @_async_ftp_server_defend_file_methods
    async def read(self, file, *args, **kwargs):
        return file.read(*args, **kwargs)

    @_async_ftp_server_universal_exception
    @_async_ftp_server_defend_file_methods
    async def close(self, file):
        return file.close()

    @_async_ftp_server_universal_exception
    async def rename(self, source, destination):
        # realonly ftp server
        assert False

    def _isVirtualDir(self, path):
        s = os.path.normpath(path.as_posix())       # we need a noramlization process fully decoupled with the filesystem, pathlib.Path.resolve() does not meet this requirement
        if s == ".":
            return True
        if s in self._dirDict:
            return True
        return False

    def _convertPath(self, path):
        s = os.path.normpath(path.as_posix())
        dirParts = s.split("/")
        prefix = dirParts[0]
        dirParts = dirParts[1:]
        if prefix not in self._dirDict:
            raise FileNotFoundError("No such file or directory: '%s'" % (s))
        return pathlib.Path(self._dirDict[prefix], *dirParts)

    def _showName(self, path):
        if path == ".":
            return "/"
        else:
            return path
