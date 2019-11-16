#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import json
import time
import socket
import struct
import logging
import threading
from collections import deque
from gi.repository import GLib


class McApiServer:

    def __init__(self, param):
        self.param = param

        self.serverSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSock.bind((self.param.listenIp, self.param.apiPort))
        self.serverSock.listen(5)
        self.serverSock.setblocking(0)
        self.serverSourceId = GLib.io_add_watch(self.serverSock, GLib.IO_IN | _flagError, self._onServerAccept)

        self.clientDict = dict()
        self.clientDictLock = threading.Lock()

    def dispose(self):
        GLib.source_remove(self.serverSourceId)
        self.serverSock.close()

        with self.clientDictLock:
            for client in self.clientDict.values():
                client.stop()
        while len(self.clientDict) > 0:
            time.sleep(1.0)

    def _onServerAccept(self, source, cb_condition):
        assert not (cb_condition & _flagError)

        try:
            new_sock, addr = source.accept()
            with self.clientDictLock:
                client = ApiClientObj(self, new_sock, addr[0])
                self.clientDict[addr] = client
                client.start()
            return True
        except socket.error as e:
            logging.debug("ApiServer._onServerAccept: Failed, %s, %s", e.__class__, e)
            return True


class ApiClientObj(threading.Thread):

    def __init__(self, pObj, sock, addr):
        threading.Thread.__init__(self)
        self.param = pObj.param
        self.pObj = pObj
        self.sock = sock
        self.addr = addr
        self.stopFlag = False
        self.sendDataQueue = deque()

        self.start()

    def sendData(self, jsonObj):
        self.sendDataQueue.append(jsonObj)

    def stop(self):
        self.stopFlag = True

    def run(self):
        while True:
            action = False
            if self.stopFlag:
                break

            # receive
            obj = self.__receiveOneObject()
            if obj is not None:
                action = True

            # send
            try:
                jsonObj = self.sendDataQueue.pop()
                self.__sendOneObject(jsonObj)
                action = True
            except IndexError:
                pass

            if not action:
                time.sleep(1.0)

        self.sock.close()
        with self.daemon.clientDictLock:
            self.daemon.clientDict[self.addr]

    def __receiveOneObject(self):
        try:
            self.sock.settimeout(1.0)
            buf = self.sock.recv(4)
            if len(buf) == 0:
                raise Exception("socket closed by peer")

            dataLen = struct.unpack("!I", buf)
            self.sock.settimeout(None)
            buf = self.sock.recv(dataLen).decode("utf-8")
            if len(buf) == 0:
                raise Exception("socket closed by peer")

            return json.loads(buf)
        except socket.timeout:
            return None

    def __sendOneObject(self, obj):
        buf = json.dumps(obj).encode("utf-8")
        self.sock.send(struct.pack("!I", len(buf)))
        self.sock.send(buf)


_flagError = GLib.IO_PRI | GLib.IO_ERR | GLib.IO_HUP | GLib.IO_NVAL
