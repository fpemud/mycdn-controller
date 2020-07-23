#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import aioftp
import pathlib
import logging
import logging.handlers
import asyncio
import functools
from mc_util import McUtil
from mc_util import AsyncIteratorExecuter
from mc_param import McConst


class McFtpServer:

    def __init__(self, serverName, mainloop, ip, port, logDir):
        assert port == "random" or 0 < port < 65536

        self._serverName = serverName
        self._mainloop = mainloop
        self._ip = ip
        self._port = port
        self._logDir = logDir

        self._userSet = set()
        self._dirDict = dict()

        self._bStart = False
        self._log = None
        self._server = None

    @property
    def port(self):
        assert self._server is not None
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
        if self._server is not None:
            self._mainloop.run_until_complete(self._stop())
        self._bStart = False

    def isStarted(self):
        return self._bStart

    def isRunning(self):
        assert self._bStart
        return self._server is not None

    def addFileDir(self, name, realPath):
        assert self._server is not None
        self._dirDict[name] = realPath

    async def _start(self):
        try:
            if self._port == "random":
                self._port = McUtil.getFreeSocketPort("tcp")
            if True:
                self._log = logging.getLogger("aioftp")
                self._log.propagate = False
                self._log.addHandler(logging.handlers.RotatingFileHandler(os.path.join(self._logDir, 'ftpd.log'),
                                                                          maxBytes=McConst.updaterLogFileSize,
                                                                          backupCount=McConst.updaterLogFileCount))
            if True:
                self._server = aioftp.Server(path_io_factory=functools.partial(_FtpServerPathIO, parent=self))
                await self._server.start(self._ip, self._port)
        except Exception:
            await self._stop()
            raise

    async def _stop(self):
        if self._server is not None:
            await self._server.close()
            self._server = None
        if self._log is not None:
            for h in self._log.handlers:
                self._log.removeHandler(h)
            self._log = None


def _ftp_server_universal_exception(func):
    """
    Decorator. Reraising any exception (with exceptions) with universal exception :py:class:`aioftp.PathIOError`
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (NotImplementedError, aioftp.errors.PathIOError) as e:
            raise
        except Exception as e:
            raise aioftp.errors.PathIOError(reason=sys.exc_info())

    return wrapper


def _async_ftp_server_universal_exception(coro):
    """
    Decorator. Reraising any exception (with exceptions) with universal exception :py:class:`aioftp.errors.PathIOError`
    """
    @functools.wraps(coro)
    async def wrapper(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except (asyncio.CancelledError, NotImplementedError, StopAsyncIteration, aioftp.errors.PathIOError) as e:
            raise
        except Exception as e:
            raise aioftp.errors.PathIOError(reason=sys.exc_info())

    return wrapper


def _async_ftp_server_defend_file_methods(coro):
    """
    Decorator. Raises exception when file methods called with wrapped by :py:class:`aioftp.pathio.AsyncPathIOContext` file object.
    """
    @functools.wraps(coro)
    async def wrapper(self, file, *args, **kwargs):
        if isinstance(file, aioftp.pathio.AsyncPathIOContext):
            raise ValueError("Native path io file methods can not be used with wrapped file object")
        return await coro(self, file, *args, **kwargs)
    return wrapper


class _FtpServerPathIO(aioftp.AbstractPathIO):

    def __init__(self, *kargs, parent=None, **kwargs):
        super().__init__(*kargs, **kwargs)
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
            newPath = self._convertPath(path)
            return AsyncIteratorExecuter(_ftp_server_universal_exception(self._listDir)(path, newPath))

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

    def _listDir(self, path, newPath):
        tl = [x.as_posix() for x in newPath.glob("*")]
        tl = [x.replace(newPath.as_posix(), path.as_posix()) for x in tl]
        tl = [pathlib.Path(x) for x in tl]
        return iter(tl)
