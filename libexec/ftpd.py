#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import json
import signal
import logging
import logging.handlers
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

    # --- Pathname / conversion utilities

    def validpath(self, path):
        if self._isVirtualRootDir(path):
            return True
        else:
            path = self._path2rpath(path)
            path = os.path.realpath(path)
            return self._rpathInRange(path)

    # --- Wrapper methods around open() and tempfile.mkstemp

    def open(self, filename, mode):
        return super().open(self._path2rpath(filename), mode)

    def mkstemp(self, suffix='', prefix='', dir=None, mode='wb'):
        raise NotImplementedError()

    # --- Wrapper methods around os.* calls

    def chdir(self, path):
        if self._isVirtualRootDir(path):
            os.chdir("/")                           # FIXME
        else:
            os.chdir(self._path2rpath(path))
        self.cwd = self.fs2ftp(path)

    def mkdir(self, path):
        raise NotImplementedError()

    def listdir(self, path):
        if self._isVirtualRootDir(path):
            return self._listVirtualRootDir()
        else:
            return super().listdir(self._path2rpath(path))

    def listdirinfo(self, path):
        raise NotImplementedError()

    def rmdir(self, path):
        raise NotImplementedError()

    def remove(self, path):
        raise NotImplementedError()

    def rename(self, src, dst):
        raise NotImplementedError()

    def chmod(self, path, mode):
        raise NotImplementedError()

    def stat(self, path):
        if self._isVirtualRootDir(path):
            return os.stat("/")                     # FIXME
        else:
            return super().stat(self._path2rpath(path))

    def utime(self, path, timeval):
        raise NotImplementedError()

    def lstat(self, path):
        if self._isVirtualRootDir(path):
            return os.lstat("/")                    # FIXME
        else:
            return super().lstat(self._path2rpath(path))

    def readlink(self, path):
        raise NotImplementedError()

    # --- Wrapper methods around os.path.* calls

    def isfile(self, path):
        if self._isVirtualRootDir(path) or self._isVirtualSiteDir(path):
            return False
        else:
            return super().isfile(self._path2rpath(path))

    def islink(self, path):
        if self._isVirtualRootDir(path) or self._isVirtualSiteDir(path):
            return False
        else:
            return super().islink(self._path2rpath(path))

    def isdir(self, path):
        if self._isVirtualRootDir(path) or self._isVirtualSiteDir(path):
            return True
        else:
            return super().isdir(self._path2rpath(path))

    def getsize(self, path):
        if self._isVirtualRootDir(path):
            return os.path.getsize("/")                         # FIXME
        elif self._isVirtualSiteDir(path):
            return super().getsize(self._path2rpath(path))     # FIXME
        else:
            return super().getsize(self._path2rpath(path))

    def getmtime(self, path):
        if self._isVirtualRootDir(path):
            return os.path.getmtime("/")                        # FIXME
        elif self._isVirtualSiteDir(path):
            return super().getmtime(self._path2rpath(path))    # FIXME
        else:
            return super().getmtime(self._path2rpath(path))

    def realpath(self, path):
        if self._isVirtualRootDir(path) or self._isVirtualSiteDir(path):
            return path
        else:
            path = self._path2rpath(path)
            path = os.path.realpath(path)
            path = self._rpath2path(path)
            return path

    def lexists(self, path):
        if self._isVirtualRootDir(path) or self._isVirtualSiteDir(path):
            return True
        else:
            return super().lexists(self._path2rpath(path))

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

    def _path2rpath(self, path):
        global cfg
        assert os.path.isabs(path) and not self._isVirtualRootDir(path)
        for prefix, realPath in cfg["dirmap"].items():
            if path == "/" + prefix:
                return realPath
            if path.startswith("/" + prefix + "/"):
                return path.replace("/" + prefix, realPath, 1)
        raise FileNotFoundError("No such file or directory: '%s'" % (path))

    def _rpath2path(self, rpath):
        global cfg
        for prefix, realPath in cfg["dirmap"].items():
            if rpath == realPath:
                return "/" + prefix
            if rpath.startswith(realPath + "/"):
                return rpath.replace(realPath, "/" + prefix, 1)
        assert False

    def _rpathInRange(self, rpath):
        global cfg
        for prefix, realPath in cfg["dirmap"].items():
            if rpath == realPath:
                return True
            if rpath.startswith(realPath + "/"):
                return True
        return False


def refreshCfgFromCfgFile():
    global cfgFile
    global cfg

    with open(cfgFile, "r") as f:
        buf = f.read()
        if buf == "":
            raise Exception("no content in config file")
        dataObj = json.loads(buf)

        if "logFile" not in dataObj:
            raise Exception("no \"logFile\" in config file")
        if "logMaxBytes" not in dataObj:
            raise Exception("no \"logMaxBytes\" in config file")
        if "logBackupCount" not in dataObj:
            raise Exception("no \"logBackupCount\" in config file")
        if "ip" not in dataObj:
            raise Exception("no \"ip\" in config file")
        if "port" not in dataObj:
            raise Exception("no \"port\" in config file")
        if "dirmap" not in dataObj:
            raise Exception("no \"dirmap\" in config file")
        for key, value in dataObj["dirmap"].items():
            if not os.path.isabs(value) or value.endswith("/"):
                raise Exception("value of \"%s\" in \"dirmap\" is invalid" % (key))
        if True:
            tl = list(dataObj["dirmap"].values())
            for i in range(0, len(tl)):
                for j in range(0, len(tl)):
                    if i != j and (tl[i] == tl[j] or tl[i].startswith(tl[j] + "/") or tl[j].startswith(tl[i] + "/")):
                        raise Exception("values in \"dirmap\" are overlay")

        if "logFile" not in cfg:
            cfg["logFile"] = dataObj["logFile"]                     # cfg["logFile"] is not changable
        if "logMaxBytes" not in cfg:
            cfg["logMaxBytes"] = dataObj["logMaxBytes"]             # cfg["logMaxBytes"] is not changable
        if "logBackupCount" not in cfg:
            cfg["logBackupCount"] = dataObj["logBackupCount"]       # cfg["logBackupCount"] is not changable
        if "ip" not in cfg:
            cfg["ip"] = dataObj["ip"]                               # cfg["ip"] is not changable
        if "port" not in cfg:
            cfg["port"] = dataObj["port"]                           # cfg["port"] is not changable
        cfg["dirmap"] = dataObj["dirmap"]


def runServer():
    global cfg

    log = logging.getLogger("pyftpdlib")
    log.propagate = False
    log.setLevel(logging.INFO)
    log.addHandler(logging.handlers.RotatingFileHandler(cfg["logFile"], cfg["logMaxBytes"], cfg["logBackupCount"]))

    authorizer = pyftpdlib.authorizers.DummyAuthorizer()
    authorizer.add_anonymous("/")

    handler = pyftpdlib.handlers.FTPHandler
    handler.authorizer = authorizer
    handler.abstracted_fs = VirtualFS

    server = pyftpdlib.servers.FTPServer((cfg["ip"], cfg["port"]), handler)
    server.serve_forever()


def sigHandler(signum, frame):
    refreshCfgFromCfgFile()


if __name__ == "__main__":
    cfgFile = sys.argv[1]
    cfg = dict()
    refreshCfgFromCfgFile()
    signal.signal(signal.SIGUSR1, sigHandler)
    runServer()
