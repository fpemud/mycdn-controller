#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

# plugin.py - mirrors plugin client library
#
# Copyright (c) 2005-2020 Fpemud <fpemud@sina.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
mirrors.plugin

@author: Fpemud
@license: GPLv3 License
@contact: fpemud@sina.com
"""

import json
import socket

__author__ = "fpemud@sina.com (Fpemud)"
__version__ = "0.0.1"


class ApiClient:

    def __init__(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/run/mirrors/api.socket")

    def close(self):
        self.sock.close()
        del self.sock

    def progress_changed(self, progress):
        self.sock.send(json.dumps({
            "message": "progress",
            "data": {
                "progress": progress,
            },
        }).encode("utf-8"))
        self.sock.send(b'\n')

    def error_occured(self, exc_info):
        self.sock.send(json.dumps({
            "message": "error",
            "data": {
                "exc_info": "abc",
            },
        }).encode("utf-8"))
        self.sock.send(b'\n')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
