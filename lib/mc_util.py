#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import imp
import dbus
import math
import json
import fcntl
import struct
import shutil
import random
import socket
import logging
import traceback
import ipaddress
import subprocess
import multiprocessing
from datetime import datetime
from collections import OrderedDict
from croniter import croniter
from gi.repository import GLib
from OpenSSL import crypto
from dbus.mainloop.glib import DBusGMainLoop


class McUtil:

    @staticmethod
    def loadObject(filename, classname, *args):
        try:
            f = open(filename)
            m = imp.load_module(filename[:-3], f, filename, ('.py', 'r', imp.PY_SOURCE))
            objClass = getattr(m, classname)
            return objClass(*args)
        except Exception:
            raise Exception("syntax error")

    @staticmethod
    def is_method(obj, method_name):
        if not hasattr(obj, method_name):
            return False
        return callable(getattr(obj, method_name))

    @staticmethod
    def objpath(obj, level=1):
        t = obj.__module__.split(".")
        t = t[level * -1:]
        return ".".join(t)

    @staticmethod
    def splitToTuple(s, delimiter):
        t = s.split(delimiter)
        assert len(t) == 2
        return (t[0], t[1])

    @staticmethod
    def joinLists(lists):
        ret = []
        for l in lists:
            ret += l
        return ret

    @staticmethod
    def loadCertAndKey(certFile, keyFile):
        cert = None
        with open(certFile, "rt") as f:
            buf = f.read()
            cert = crypto.load_certificate(crypto.FILETYPE_PEM, buf)

        key = None
        with open(keyFile, "rt") as f:
            buf = f.read()
            key = crypto.load_privatekey(crypto.FILETYPE_PEM, buf)

        return (cert, key)

    @staticmethod
    def genCertAndKey(caCert, caKey, cn, keysize):
        k = crypto.PKey()
        k.generate_key(crypto.TYPE_RSA, keysize)

        cert = crypto.X509()
        cert.get_subject().CN = cn
        cert.set_serial_number(random.randint(0, 65535))
        cert.gmtime_adj_notBefore(100 * 365 * 24 * 60 * 60 * -1)
        cert.gmtime_adj_notAfter(100 * 365 * 24 * 60 * 60)
        cert.set_issuer(caCert.get_subject())
        cert.set_pubkey(k)
        cert.sign(caKey, 'sha1')

        return (cert, k)

    @staticmethod
    def dumpCertAndKey(cert, key, certFile, keyFile):
        with open(certFile, "wb") as f:
            buf = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
            f.write(buf)
            os.fchmod(f.fileno(), 0o644)

        with open(keyFile, "wb") as f:
            buf = crypto.dump_privatekey(crypto.FILETYPE_PEM, key)
            f.write(buf)
            os.fchmod(f.fileno(), 0o600)

    @staticmethod
    def is_int(s):
        try:
            int(s)
            return True
        except Exception:
            return False

    @staticmethod
    def forceDelete(filename):
        if os.path.islink(filename):
            os.remove(filename)
        elif os.path.isfile(filename):
            os.remove(filename)
        elif os.path.isdir(filename):
            shutil.rmtree(filename)

    @staticmethod
    def mkDirAndClear(dirname):
        McUtil.forceDelete(dirname)
        os.mkdir(dirname)

    @staticmethod
    def ensureDir(dirname):
        if not os.path.exists(dirname):
            os.makedirs(dirname)

    @staticmethod
    def getFileList(dirName, level, typeList):
        """typeList is a string, value range is "d,f,l,a"
           returns basename"""

        ret = []
        for fbasename in os.listdir(dirName):
            fname = os.path.join(dirName, fbasename)

            if os.path.isdir(fname) and level - 1 > 0:
                for i in McUtil.getFileList(fname, level - 1, typeList):
                    ret.append(os.path.join(fbasename, i))
                continue

            appended = False
            if not appended and ("a" in typeList or "d" in typeList) and os.path.isdir(fname):        # directory
                ret.append(fbasename)
            if not appended and ("a" in typeList or "f" in typeList) and os.path.isfile(fname):        # file
                ret.append(fbasename)
            if not appended and ("a" in typeList or "l" in typeList) and os.path.islink(fname):        # soft-link
                ret.append(fbasename)

        return ret

    @staticmethod
    def getInterfaceIfIndex(ifname):
        SIOCGIFINDEX = 0x8933           # check your /usr/include/linux/sockios.h file for the appropriate value here
        IFNAMSIZ = 16                   # from /usr/include/net/if.h
        ifname = ifname[:IFNAMSIZ - 1]  # truncate supplied ifname
        ioctlbuf = ifname + ('\x00' * (IFNAMSIZ - len(ifname))) + ('\x00' * IFNAMSIZ)
        skt = socket.socket()
        try:
            ret = fcntl.ioctl(skt.fileno(), SIOCGIFINDEX, ioctlbuf)
            ifname, ifindex = struct.unpack_from('16sL', ret)
            return ifindex
        finally:
            skt.close()

    @staticmethod
    def ip2ipar(ip):
        AF_INET = 2
        # AF_INET6 = 10
        el = ip.split(".")
        assert len(el) == 4
        return (AF_INET, [bytes([int(x)]) for x in el])

    @staticmethod
    def getLineWithoutBlankAndComment(line):
        if line.find("#") >= 0:
            line = line[:line.find("#")]
        line = line.strip()
        return line if line != "" else None

    @staticmethod
    def printInfo(msgStr):
        print(McUtil.fmt("*", "GOOD") + " " + msgStr)

    @staticmethod
    def printInfoNoNewLine(msgStr):
        print(McUtil.fmt("*", "GOOD") + " " + msgStr, end="", flush=True)

    @staticmethod
    def fmt(msgStr, fmtStr):
        FMT_GOOD = "\x1B[32;01m"
        FMT_WARN = "\x1B[33;01m"
        FMT_BAD = "\x1B[31;01m"
        FMT_NORMAL = "\x1B[0m"
        FMT_BOLD = "\x1B[0;01m"
        FMT_UNDER = "\x1B[4m"

        for fo in fmtStr.split("+"):
            if fo == "GOOD":
                return FMT_GOOD + msgStr + FMT_NORMAL
            elif fo == "WARN":
                return FMT_WARN + msgStr + FMT_NORMAL
            elif fo == "BAD":
                return FMT_BAD + msgStr + FMT_NORMAL
            elif fo == "BOLD":
                return FMT_BOLD + msgStr + FMT_NORMAL
            elif fo == "UNDER":
                return FMT_UNDER + msgStr + FMT_NORMAL
            else:
                assert False

    @staticmethod
    def getReservedIpv4NetworkList():
        return [
            ipaddress.IPv4Network("0.0.0.0/8"),
            ipaddress.IPv4Network("10.0.0.0/8"),
            ipaddress.IPv4Network("100.64.0.0/10"),
            ipaddress.IPv4Network("127.0.0.0/8"),
            ipaddress.IPv4Network("169.254.0.0/16"),
            ipaddress.IPv4Network("172.16.0.0/12"),
            ipaddress.IPv4Network("192.0.0.0/24"),
            ipaddress.IPv4Network("192.0.2.0/24"),
            ipaddress.IPv4Network("192.88.99.0/24"),
            ipaddress.IPv4Network("192.168.0.0/16"),
            ipaddress.IPv4Network("198.18.0.0/15"),
            ipaddress.IPv4Network("198.51.100.0/24"),
            ipaddress.IPv4Network("203.0.113.0/24"),
            ipaddress.IPv4Network("224.0.0.0/4"),
            ipaddress.IPv4Network("240.0.0.0/4"),
            ipaddress.IPv4Network("255.255.255.255/32"),
        ]

    @staticmethod
    def substractIpv4Network(ipv4Network, ipv4NetworkList):
        netlist = [ipv4Network]
        for n in ipv4NetworkList:
            tlist = []
            for n2 in netlist:
                if not n2.overlaps(n):
                    tlist.append(n2)                                # no need to substract
                    continue
                try:
                    tlist += list(n2.address_exclude(n))            # successful to substract
                except Exception:
                    pass                                            # substract to none
            netlist = tlist
        return netlist

    @staticmethod
    def getFreeSocketPort(portType):
        if portType == "tcp":
            stlist = [socket.SOCK_STREAM]
        elif portType == "udp":
            stlist = [socket.SOCK_DGRAM]
        elif portType == "tcp+udp":
            stlist = [socket.SOCK_STREAM, socket.SOCK_DGRAM]
        else:
            assert False

        for port in range(10000, 65536):
            bFound = True
            for sType in stlist:
                s = socket.socket(socket.AF_INET, sType)
                try:
                    s.bind((('', port)))
                except socket.error:
                    bFound = False
                finally:
                    s.close()
            if bFound:
                return port

        raise Exception("no valid port")

    @staticmethod
    def testSocketPort(portType, port):
        if portType == "tcp":
            stlist = [socket.SOCK_STREAM]
        elif portType == "udp":
            stlist = [socket.SOCK_DGRAM]
        elif portType == "tcp+udp":
            stlist = [socket.SOCK_STREAM, socket.SOCK_DGRAM]
        else:
            assert False

        for sType in stlist:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.bind((('', port)))
            except socket.error:
                return False
            finally:
                s.close()
        return True

    @staticmethod
    def touchFile(filename):
        assert not os.path.exists(filename)
        f = open(filename, 'w')
        f.close()


class StdoutRedirector:

    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()


class DynObject:
    # an object that can contain abitrary dynamically created properties and methods
    pass


class GLibIdleInvoker:

    def __init__(self):
        pass

    def dispose(self):
        # there's should be a source list tracking all the idle sources
        # the source list should be released when disposing
        pass

    def add(self, func, *args):
        GLib.idle_add(self._idleCallback, func, *args)

    def _idleCallback(self, func, *args):
        func(*args)
        return False


class GLibCronScheduler:

    def __init__(self):
        self.jobDict = OrderedDict()       # dict<id,(iter,callback)>
        self.jobPauseDict = dict()         # dict<id,datetime>

        self.nextDatetime = None
        self.nextJobList = None

        self.timeoutHandler = None

    def dispose(self):
        if self.timeoutHandler is not None:
            GLib.source_remove(self.timeoutHandler)
            self.timeoutHandler = None
        self.nextJobList = None
        self.nextDatetime = None
        self.jobDict = OrderedDict()

    def addJob(self, jobId, cronExpr, jobCallback):
        assert jobId not in self.jobDict

        now = datetime.now()

        # add job
        iter = self._createCronIter(cronExpr, now)
        self.jobDict[jobId] = (iter, jobCallback)

        # add job or recalcluate timeout if it is first job
        if self.nextDatetime is not None:
            now = min(now, self.nextDatetime)
            if self._getNextDatetime(now, iter) < self.nextDatetime:
                self._clearTimeout()
                self._calcTimeout(now)
            elif self._getNextDatetime(now, iter) == self.nextDatetime:
                self.nextJobList.append(jobId)
        else:
            self._calcTimeout(now)

    def removeJob(self, jobId):
        assert jobId in self.jobDict

        # remove job
        del self.jobDict[jobId]

        # recalculate timeout if neccessary
        now = datetime.now()
        if self.nextDatetime is not None:
            if jobId in self.nextJobList:
                self.nextJobList.remove(jobId)
                if len(self.nextJobList) == 0:
                    self._clearTimeout()
                    self._calcTimeout(now)
        else:
            assert False

    def pauseJob(self, jobId, datetime):
        assert jobId in self.jobDict
        self.jobPauseDict[jobId] = datetime

    def _calcTimeout(self, now):
        assert self.nextDatetime is None

        for jobId, v in self.jobDict.items():
            iter = v[0]
            if self.nextDatetime is None or self._getNextDatetime(now, iter) < self.nextDatetime:
                self.nextDatetime = self._getNextDatetime(now, iter)
                self.nextJobList = [jobId]
                continue
            if self._getNextDatetime(now, iter) == self.nextDatetime:
                self.nextJobList.append(jobId)
                continue

        if self.nextDatetime is not None:
            interval = math.ceil((self.nextDatetime - now).total_seconds())
            assert interval > 0
            self.timeoutHandler = GLib.timeout_add_seconds(interval, self._jobCallback)

    def _clearTimeout(self):
        assert self.nextDatetime is not None

        GLib.source_remove(self.timeoutHandler)
        self.timeoutHandler = None
        self.nextJobList = None
        self.nextDatetime = None

    def _jobCallback(self):
        for jobId in self.nextJobList:
            if jobId not in self.jobPauseDict:
                self.jobDict[jobId][1](self.nextDatetime)
            else:
                if self.jobPauseDict[jobId] <= self.nextDatetime:
                    del self.jobPauseDict[jobId]
                    self.jobDict[jobId][1](self.nextDatetime)
        self._clearTimeout()
        self._calcTimeout(datetime.now())           # self._calcTimeout(self.nextDatetime) is stricter but less robust
        return False

    def _createCronIter(self, cronExpr, curDatetime):
        iter = croniter(cronExpr, curDatetime, datetime)
        iter.get_next()
        return iter

    def _getNextDatetime(self, curDatetime, croniterIter):
        while croniterIter.get_current() < curDatetime:
            croniterIter.get_next()
        return croniterIter.get_current()


class UnixDomainSocketApiServer:

    def __init__(self, serverFile, clientInitFunc, notifyFunc):
        self.clientInitFunc = clientInitFunc
        self.notifyFunc = notifyFunc

        self.serverSock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.serverSock.bind(serverFile)
        self.serverSock.listen(5)
        self.serverSourceId = GLib.io_add_watch(self.serverSock, GLib.IO_IN, self.onServerAccept)

        self.clientInfoDict = dict()

    def dispose(self):
        for sock, obj in self.clientInfoDict.items():
            GLib.source_remove(obj.inWatch)
            sock.close()
        GLib.source_remove(self.serverSourceId)
        self.serverSock.close()

    def onServerAccept(self, source, cb_condition):
        # event callback, no exception is allowed

        new_sock, addr = source.accept()

        try:
            data = self.clientInitFunc(new_sock)
        except Exception:
            # absorb exception raised by upper layer function
            traceback.print_exc()
            new_sock.close()
            return True

        obj = DynObject()
        obj.inWatch = GLib.io_add_watch(new_sock, GLib.IO_IN | GLib.IO_PRI | GLib.IO_ERR | GLib.IO_HUP | GLib.IO_NVAL, self.onRecv)
        obj.recvBuf = b''
        obj.clientData = data
        self.clientInfoDict[new_sock] = obj
        return True

    def onRecv(self, source, cb_condition):
        try:
            obj = self.clientInfoDict[source]

            # receive and parse
            obj.recvBuf += source.recv(4096)
            while True:
                i = obj.recvBuf.find(b'\n')
                if i < 0:
                    break
                jsonObj = json.loads(obj.recvBuf[:i].decode("utf-8"))
                obj.recvBuf = obj.recvBuf[i + 1:]
                try:
                    self.notifyFunc(obj.clientData, jsonObj)
                except Exception:
                    # absorb exception raised by upper layer function, FIXME
                    raise

            # remote closed
            if (cb_condition & GLib.IO_HUP):
                raise Exception("remove closed")

            return True
        except Exception as e:
            print("excp IO_IN, %d" % (cb_condition & GLib.IO_IN))               # FIXME
            print("excp IO_PRI, %d" % (cb_condition & GLib.IO_PRI))
            print("excp IO_ERR, %d" % (cb_condition & GLib.IO_ERR))
            print("excp IO_HUP, %d" % (cb_condition & GLib.IO_HUP))
            print("excp IO_NVAL, %d" % (cb_condition & GLib.IO_NVAL))
            logging.debug("UnixSocketApiServer.onRecv: Failed, %s, %s", e.__class__, e)
            source.close()
            del self.clientInfoDict[source]
            return False


class AsyncIteratorExecuter:

    def __init__(self, iter):
        self.iter = iter

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


class HttpFileServer:

    def __init__(self, ip, port, dirList, logDir):
        assert 0 < port < 65536
        self._ip = ip
        self._port = port
        self._dirlist = dirList
        self._logfile = os.path.join(logDir, "bozohttpd.log")
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    @property
    def running(self):
        return self._proc is not None

    def start(self):
        assert self._proc is None
        homedir = os.path.dirname(self._dirlist[0])
        cmd = "/usr/bin/bozohttpd -b -f -H -I %d -s -X %s 2>%s" % (self._port, homedir, self._logfile)
        self._proc = subprocess.Popen(cmd, shell=True, universal_newlines=True)

    def stop(self):
        assert self._proc is not None
        self._proc.terminate()
        self._proc.wait()
        self._proc = None


class FtpServer:

    def __init__(self, ip, port, dirList, logDir):
        assert 0 < port < 65536
        self._ip = ip
        self._port = port
        self._dirlist = dirList
        self._logfile = os.path.join(logDir, "pyftpd.log")
        self._proc = None

    @property
    def port(self):
        assert self._proc is not None
        return self._port

    @property
    def running(self):
        return self._proc is not None

    def start(self):
        assert self._proc is None
        homedir = os.path.dirname(self._dirlist[0])
        self._proc = multiprocessing.Process(target=FtpServer._runFtpDaemon, args=(self._ip, self._port, homedir, self._logfile, ))
        self._proc.start()

    def stop(self):
        assert self._proc is not None
        self._proc.terminate()
        self._proc.join()
        self._proc = None

    @staticmethod
    def _runFtpDaemon(ip, port, homedir, logfile):
        with open(logfile, "a") as f:
            sys.stdout = f              # redirect stdout into logfile
            sys.stderr = f              # redirect stderr into logfile
            from pyftpdlib.authorizers import DummyAuthorizer
            from pyftpdlib.handlers import FTPHandler
            from pyftpdlib.servers import FTPServer
            handler = FTPHandler
            handler.authorizer = DummyAuthorizer()
            handler.authorizer.add_anonymous(homedir)
            server = FTPServer((ip, port), handler)
            server.serve_forever()


class AvahiServiceRegister:

    """
    Exampe:
        obj = AvahiServiceRegister()
        obj.add_service(socket.gethostname(), "_http", 80)
        obj.start()
        obj.stop()
    """

    def __init__(self):
        self.retryInterval = 30
        self.serviceList = []

    def add_service(self, service_name, service_type, port):
        assert isinstance(service_name, str)
        assert service_type.endswith("._tcp") or service_type.endswith("._udp")
        assert isinstance(port, int)
        self.serviceList.append((service_name, service_type, port))

    def start(self):
        DBusGMainLoop(set_as_default=True)

        self._server = None
        self._retryCreateServerTimer = None
        self._entryGroup = None
        self._retryRegisterServiceTimer = None
        self._ownerChangeHandler = None

        if dbus.SystemBus().name_has_owner("org.freedesktop.Avahi"):
            self._createServer()
        self._ownerChangeHandler = dbus.SystemBus().add_signal_receiver(self.onNameOwnerChanged, "NameOwnerChanged", None, None)

    def stop(self):
        if self._ownerChangeHandler is not None:
            dbus.SystemBus().remove_signal_receiver(self._ownerChangeHandler)
            self._ownerChangeHandler = None
        self._unregisterService()
        self._releaseServer()

    def onNameOwnerChanged(self, name, old, new):
        if name == "org.freedesktop.Avahi":
            if new != "" and old == "":
                if self._server is None:
                    self._createServer()
                else:
                    # this may happen on some rare case
                    pass
            elif new == "" and old != "":
                self._unregisterService()
                self._releaseServer()
            else:
                assert False

    def _createServer(self):
        assert self._server is None and self._retryCreateServerTimer is None
        assert self._entryGroup is None
        try:
            self._server = dbus.Interface(dbus.SystemBus().get_object("org.freedesktop.Avahi", "/"), "org.freedesktop.Avahi.Server")
            if self._server.GetState() == 2:    # avahi.SERVER_RUNNING
                self._registerService()
            self._server.connect_to_signal("StateChanged", self.onSeverStateChanged)
        except Exception:
            logging.error("Avahi create server failed, retry in %d seconds" % (self.retryInterval), exc_info=True)
            self._releaseServer()
            self._retryCreateServer()

    def _releaseServer(self):
        assert self._entryGroup is None
        if self._retryCreateServerTimer is not None:
            GLib.source_remove(self._retryCreateServerTimer)
            self._retryCreateServerTimer = None
        self._server = None

    def onSeverStateChanged(self, state, error):
        if state == 2:      # avahi.SERVER_RUNNING
            self._unregisterService()
            self._registerService()
        else:
            self._unregisterService()

    def _registerService(self):
        assert self._entryGroup is None and self._retryRegisterServiceTimer is None
        try:
            self._entryGroup = dbus.Interface(dbus.SystemBus().get_object("org.freedesktop.Avahi", self._server.EntryGroupNew()),
                                              "org.freedesktop.Avahi.EntryGroup")
            for serviceName, serviceType, port in self.serviceList:
                self._entryGroup.AddService(-1,                 # interface = avahi.IF_UNSPEC
                                            0,                  # protocol = avahi.PROTO_UNSPEC
                                            dbus.UInt32(0),     # flags
                                            serviceName,        # name
                                            serviceType,        # type
                                            "",                 # domain
                                            "",                 # host
                                            dbus.UInt16(port),  # port
                                            "")                 # txt
            self._entryGroup.Commit()
            self._entryGroup.connect_to_signal("StateChanged", self.onEntryGroupStateChanged)
        except Exception:
            logging.error("Avahi register service failed, retry in %d seconds" % (self.retryInterval), exc_info=True)
            self._unregisterService()
            self._retryRegisterService()

    def _unregisterService(self):
        if self._retryRegisterServiceTimer is not None:
            GLib.source_remove(self._retryRegisterServiceTimer)
            self._retryRegisterServiceTimer = None
        if self._entryGroup is not None:
            try:
                if self._entryGroup.GetState() != 4:        # avahi.ENTRY_GROUP_FAILURE
                    self._entryGroup.Reset()
                    self._entryGroup.Free()
                    # .Free() has mem leaks?
                    self._entryGroup._obj._bus = None
                    self._entryGroup._obj = None
            except dbus.exceptions.DBusException:
                pass
            finally:
                self._entryGroup = None

    def onEntryGroupStateChanged(self, state, error):
        if state in [0, 1, 2]:  # avahi.ENTRY_GROUP_UNCOMMITED, avahi.ENTRY_GROUP_REGISTERING, avahi.ENTRY_GROUP_ESTABLISHED
            pass
        elif state == 3:        # avahi.ENTRY_GROUP_COLLISION
            self._unregisterService()
            self._retryRegisterService()
        elif state == 4:        # avahi.ENTRY_GROUP_FAILURE
            assert False
        else:
            assert False

    def _retryCreateServer(self):
        assert self._retryCreateServerTimer is None
        self._retryCreateServerTimer = GLib.timeout_add_seconds(self.retryInterval, self.__timeoutCreateServer)

    def __timeoutCreateServer(self):
        self._retryCreateServerTimer = None
        self._createServer()                    # no exception in self._createServer()
        return False

    def _retryRegisterService(self):
        assert self._retryRegisterServiceTimer is None
        self._retryRegisterServiceTimer = GLib.timeout_add_seconds(self.retryInterval, self.__timeoutRegisterService)

    def __timeoutRegisterService(self):
        self._retryRegisterServiceTimer = None
        self._registerService()                 # no exception in self._registerService()
        return False


class RotatingFile:
    """
    By default, the file grows indefinitely. You can specify particular
    values of maxBytes and backupCount to allow the file to rollover at
    a predetermined size.
    Rollover occurs whenever the current file is nearly maxBytes in
    length. File length never reachs maxBytes unless the current line of
    data exceeds maxBytes.
    If backupCount is >= 1, the system will successively create
    new files with the same pathname as the base file, but with extensions
    ".1", ".2" etc. appended to it. For example, with a backupCount of 5
    and a base file name of "app.log", you would get "app.log",
    "app.log.1", "app.log.2", ... through to "app.log.5". The file being
    written to is always "app.log" - when it gets filled up, it is closed
    and renamed to "app.log.1", and if files "app.log.1", "app.log.2" etc.
    exist, then they are renamed to "app.log.2", "app.log.3" etc.
    respectively.
    """

    def __init__(self, filename, maxBytes, backupCount):
        assert maxBytes > 0 and backupCount > 0
        self.baseFilename = filename
        self.maxBytes = maxBytes
        self.backupCount = backupCount
        self.f = open(self.baseFilename, "ab", buffering=0)

    def write(self, s):
        assert self.f is not None
        while s != b'':
            # process one line at a time
            try:
                i = s.index(b'\n')
                sCur = s[:i+1]
                s = s[i+1:]
            except ValueError:
                sCur = s
                s = b''

            # do roll over
            if self.f.tell() > 0 and self.f.tell() + len(sCur) >= self.maxBytes:
                self._doRollover()

            # write line
            self.f.write(sCur)

    def close(self):
        assert self.f is not None
        self.f.close()
        self.f = None

    def _doRollover(self):
        self.f.close()
        self.f = None

        for i in range(self.backupCount - 1, 0, -1):
            sfn = "%s.%d" % (self.baseFilename, i)
            dfn = "%s.%d" % (self.baseFilename, i + 1)
            if os.path.exists(sfn):
                if os.path.exists(dfn):
                    os.remove(dfn)
                os.rename(sfn, dfn)

        dfn = self.baseFilename + ".1"
        if os.path.exists(dfn):
            os.remove(dfn)
        os.rename(self.baseFilename, dfn)

        self.f = open(self.baseFilename, "a")
