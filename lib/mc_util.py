#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import sys
import socket
import fcntl
import struct
import shutil
import random
import cronitor
import ipaddress
import subprocess
import multiprocessing
from datetime import datetime
from collections import OrderedDict
from gi.repository import GLib
from OpenSSL import crypto


class McUtil:

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
        except:
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
                except:
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


class CronScheduler:

    def __init__(self):
        self.jobDict = OrderedDict()       # dict<id,(iter,callback)>
        self.nextDatetime = None
        self.nextJobCallbackList = None
        self.timeoutHandler = None

    def dispose(self):
        if self.timeoutHandler is not None:
            GLib.source_remove(self.timeoutHandler)
            self.timeoutHandler = None
        self.nextJobCallbackList = None
        self.nextDatetime = None
        self.jobDict = OrderedDict()

    def addJob(self, jobId, cronExpr, jobCallback):
        assert jobId not in self.jobDict
        self.jobDict[jobId] = (cronitor(cronExpr), jobCallback)
        self._refreshTimeout()

    def removeJob(self, jobId):
        del self.jobDict[jobId]
        self._refreshTimeout()

    def _refreshTimeout(self):
        nextDatetime = None
        nextJobCallbackList = []
        for iter, cb in self.jobDict.values():
            if nextDatetime is None:
                nextDatetime = iter.get_next(datetime)
                nextJobCallbackList = [cb]
                continue
            if iter.get_next(datetime) == nextDatetime:
                nextJobCallbackList.append(cb)
                continue
            if iter.get_next(datetime) < nextDatetime:
                nextDatetime = iter.get_next(datetime)
                nextJobCallbackList = [cb]
                continue

        if nextDatetime is None:
            if self.nextDatetime is not None:
                self._clearTimeout()
        else:
            if self.nextDatetime is None:
                self._setTimeout(nextDatetime, nextJobCallbackList)
            elif nextDatetime == self.nextDatetime:
                self.nextJobCallbackList = nextJobCallbackList
            else:
                self._clearTimeout()
                self._setTimeout(nextDatetime, nextJobCallbackList)

    def _clearTimeout(self):
        assert self.nextDatetime is not None
        GLib.source_remove(self.timeoutHandler)
        self.timeoutHandler = None
        self.nextJobCallbackList = None
        self.nextDatetime = None

    def _setTimeout(self, nextDatetime, nextJobCallbackList):
        assert self.nextDatetime is None
        self.nextDatetime = nextDatetime
        self.nextJobCallbackList = nextJobCallbackList
        self.timeoutHandler = GLib.timeout_add((self.nextDatetime - datetime.now()).total_seconds(),
                                               self._jobCallback)

    def _jobCallback(self):
        for jobCallback in self.nextJobCallbackList:
            jobCallback(self.nextDatetime)
        self._refreshTimeout()
        return False


class HttpFileServer:

    def __init__(self, ip, port, dirList, logFile):
        assert 0 < port < 65536

        self._ip = ip
        self._port = port
        self._dirlist = dirList
        self._logfile = logfile
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
        self._proc = subprocess.Popen(cmd, shell=True, universial_newlines=True)

    def stop(self):
        assert self._proc is not None
        self._proc.terminate()
        self._proc.wait()
        self._proc = None


class FtpServer:

    def __init__(self, ip, port, dirList, logFile):
        assert 0 < port < 65536

        self._ip = ip
        self._port = port
        self._dirlist = dirList
        self._logfile = logfile
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
        from pyftpdlib.authorizers import DummyAuthorizer
        from pyftpdlib.handlers import FTPHandler
        from pyftpdlib.servers import FTPServer
        with open(logfile, "a") as f:
            sys.stdout = f              # redirect stdout into logfile
            sys.stderr = f              # redirect stderr into logfile
            handler = FTPHandler
            handler.authorizer = DummyAuthorizer()
            handler.authorizer.add_anonymous(homedir)
            server = FTPServer((ip, port), handler)
            server.serve_forever()
