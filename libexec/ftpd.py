#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import json
import signal
import pyftpdlib.servers
import pyftpdlib.handlers
import pyftpdlib.authorizers
import pyftpdlib.filesystems


class VirtualFS(pyftpdlib.filesystems.AbstractedFS):

    """
    virtual-root-directory
      |---- virtual-site-directory -> /var/cache/mirrors/SITE/storage-file
              |---- ...
      |---- virtual-site-directory -> /var/cache/mirrors/SITE/storage-file
              |---- ...
      |---- ...
    """

    # --- Wrapper methods around open() and tempfile.mkstemp

    def open(self, filename, mode):
        return super().open(self._convertPath(filename), mode)

    def mkstemp(self, suffix='', prefix='', dir=None, mode='wb'):
        raise NotImplementedError

    # --- Wrapper methods around os.* calls

    def chdir(self, path):
        if self._isVirtualRootDir(path):
            os.chdir("/")                           # FIXME
        else:
            os.chdir(self._convertPath(path))
        self.cwd = self.fs2ftp(path)

    def mkdir(self, path):
        raise NotImplementedError

    def listdir(self, path):
        if self._isVirtualRootDir(path):
            return self._listVirtualRootDir()
        else:
            return super().listdir(self._convertPath(path))

    def listdirinfo(self, path):
        raise NotImplementedError

    def rmdir(self, path):
        raise NotImplementedError

    def remove(self, path):
        raise NotImplementedError

    def rename(self, src, dst):
        raise NotImplementedError

    def chmod(self, path, mode):
        raise NotImplementedError

    def stat(self, path):
        if self._isVirtualRootDir(path):
            return os.stat("/")                     # FIXME
        else:
            return super().stat(self._convertPath(path))

    def utime(self, path, timeval):
        raise NotImplementedError

    def lstat(self, path):
        if self._isVirtualRootDir(path):
            return os.lstat("/")                    # FIXME
        else:
            return super().lstat(self._convertPath(path))

    def readlink(self, path):
        raise NotImplementedError

    # --- Wrapper methods around os.path.* calls

    def isfile(self, path):
        if self._isVirtualRootDir(path) or self._isVirtualSiteDir(path):
            return False
        else:
            return super().isfile(self._convertPath(path))

    def islink(self, path):
        if self._isVirtualRootDir(path) or self._isVirtualSiteDir(path):
            return False
        else:
            return super().islink(self._convertPath(path))

    def isdir(self, path):
        if self._isVirtualRootDir(path) or self._isVirtualSiteDir(path):
            return True
        else:
            return super().isdir(self._convertPath(path))

    def getsize(self, path):
        if self._isVirtualRootDir(path):
            return os.path.getsize("/")                         # FIXME
        elif self._isVirtualSiteDir(path):
            return super().getsize(self._convertPath(path))     # FIXME
        else:
            return super().getsize(self._convertPath(path))

    def getmtime(self, path):
        if self._isVirtualRootDir(path):
            return os.path.getmtime("/")                        # FIXME
        elif self._isVirtualSiteDir(path):
            return super().getmtime(self._convertPath(path))    # FIXME
        else:
            return super().getmtime(self._convertPath(path))

    def realpath(self, path):
        if self._isVirtualRootDir(path):
            return os.path.realpath("/")                        # FIXME
        elif self._isVirtualSiteDir(path):
            return super().realpath(self._convertPath(path))    # FIXME
        else:
            return super().realpath(self._convertPath(path))

    def lexists(self, path):
        if self._isVirtualRootDir(path) or self._isVirtualSiteDir(path):
            return True
        else:
            return super().lexists(self._convertPath(path))

    def get_user_by_uid(self, uid):
        return "owner"

    def get_group_by_gid(self, gid):
        return "group"

    def _isVirtualRootDir(self, path):
        assert os.path.isabs(path)
        return path == "/"

    def _isVirtualSiteDir(self, path):
        # "/xyz" are virtual site directories
        assert os.path.isabs(path) and not self._isVirtualRootDir(path)
        return path.count("/") == 1

    def _listVirtualRootDir(self):
        global cfg
        return sorted(cfg["dirmap"].keys())

    def _convertPath(self, path):
        global cfg
        assert os.path.isabs(path) and not self._isVirtualRootDir(path)
        dirParts = path[1:].split("/")
        prefix, dirParts = dirParts[0], dirParts[1:]
        if prefix not in cfg["dirmap"]:
            raise FileNotFoundError("No such file or directory: '%s'" % (path))
        return os.path.join(cfg["dirmap"][prefix], *dirParts)


def refreshCfgFromCfgFile():
    global cfgFile
    global cfg

    with open(cfgFile, "r") as f:
        buf = f.read()
        if buf != "":
            dataObj = json.loads(buf)
            if "ip" not in dataObj:
                raise Exception("no \"ip\" in config file")
            if "port" not in dataObj:
                raise Exception("no \"port\" in config file")
            if "dirmap" not in dataObj:
                raise Exception("no \"dirmap\" in config file")
            for key, value in dataObj["dirmap"].items():
                if not os.path.isabs(value):
                    raise Exception("value of \"%s\" in \"dirmap\" is not absolute path")
            cfg = dataObj
        else:
            cfg = dict()


def runServer():
    global cfg

    authorizer = pyftpdlib.authorizers.DummyAuthorizer()
    authorizer.add_anonymous("/")

    handler = pyftpdlib.handlers.FTPHandler
    handler.authorizer = authorizer
    handler.abstracted_fs = VirtualFS

    server = pyftpdlib.servers.FTPServer((cfg["ip"], cfg["port"]), handler)
    server.serve_forever()


def sigHandlerHUP(signum, frame):
    refreshCfgFromCfgFile()


if __name__ == "__main__":
    cfgFile = sys.argv[1]
    cfg = dict()
    refreshCfgFromCfgFile()
    signal.signal(signal.SIGHUP, sigHandlerHUP)
    runServer()
