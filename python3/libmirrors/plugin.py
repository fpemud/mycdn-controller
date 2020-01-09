#!/usr/bin/env python3

# libmirrors.plugin - Framework for implementing mirrors plugins
#
# Copyright (c) 2005-2019 Fpemud <fpemud@sina.com>
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
libmirrors.plugin

@author: Fpemud
@license: GPLv3 License
@contact: fpemud@sina.com
"""

import json
import socket

__author__ = "fpemud@sina.com (Fpemud)"
__version__ = "0.0.1"


_mirror_site_id = None
_operation_type = None
_country = None
_location = None
_data_dir = None
_tmp_dir = None
_log_dir = None
_sock = None


OPERATION_TYPE_INIT = 1
OPERATION_TYPE_UPDATE = 2


def plugin_init(argv):
    global _mirror_site_id
    global _operation_type
    global _country
    global _location
    global _data_dir
    global _log_dir
    global _sock

    try:
        _mirror_site_id = argv[1]
        if argv[2] == "init":
            _operation_type = OPERATION_TYPE_INIT
        elif argv[2] == "update":
            _operation_type = OPERATION_TYPE_UPDATE
        else:
            assert False
        _country = argv[6]
        _location = argv[7]
        _data_dir = argv[3]
        _tmp_dir = argv[4]
        _log_dir = argv[5]

        _sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        _sock.connect("/run/mirrors/api.socket")
    except:
        _sock = None
        _log_dir = None
        _tmp_dir = None
        _data_dir = None
        _location = None
        _country = None
        _operation_type = None
        _mirror_site_id = None
        raise


def get_mirror_site_id():
    global _mirror_site_id
    return _mirror_site_id


def get_operation_type():
    global _operation_type
    return _operation_type


def get_country():
    global _country
    return _country


def get_location():
    global _location
    return _location


def get_data_dir():
    global _data_dir
    return _data_dir


def get_log_dir():
    global _log_dir
    return _log_dir


def progress_changed(progress):
    global _sock
    data = json.dumps({
        "message": "progress",
        "data": {
            "progress": progress,
        },
    })
    _sock.send(data.encoding("utf-8"))


def error_occured(exc_info):
    global _sock
    data = json.dumps({
        "message": "error_occured",
        "data": {
            "exc_info": "abc",
        },
    })
    _sock.send(data.encoding("utf-8"))


def error_occured_and_hold_for(seconds, exc_info):
    global _sock
    data = json.dumps({
        "message": "error_occured_and_hold_for",
        "data": {
            "seconds": seconds,
            "exc_info": "abc",
        },
    })
    _sock.send(data.encoding("utf-8"))
