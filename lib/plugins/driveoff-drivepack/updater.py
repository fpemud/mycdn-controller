#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import io
import sys
import gzip
import time
import json
import magic
import random
import certifi
import socket
import subprocess
import lxml.html
import urllib.request


MAX_PAGE = 10
PROGRESS_STAGE_1 = 20
PROGRESS_STAGE_2 = 79


def main():
    sock = _Util.connect()
    try:
        dataDir = sys.argv[1]
        linkDict = dict()
        fnSet = set()

        # fetch web pages
        # retrived from "http://driveroff.net/category/dp", it's in russian, do use google webpage translator
        print("Start fetching file list.")
        for i in range(1, MAX_PAGE):
            found = False
            url = "http://www.gigabase.com/folder/cbcv8AZeKsHjAkenvVrjPQBB?page=%d" % (i)
            root = _Util.getWebPageElementTree(url)
            for elem in root.xpath(".//a"):
                if elem.text is None:
                    continue
                if not elem.text.startswith("DP_"):
                    continue
                linkDict[elem.text] = elem.attrib["href"]
                found = True
            if not found:
                break
            _Util.progress_changed(sock, PROGRESS_STAGE_1 * i // MAX_PAGE)
        print("File list fetched, total %d files." % (len(linkDict)))
        _Util.progress_changed(sock, PROGRESS_STAGE_1)

        # download driver pack file one by one
        i = 1
        total = len(linkDict)
        for filename, url in _Util.randomSorted(linkDict.items()):
            fullfn = os.path.join(libmirrors.get_data_dir(), filename)
            if not os.path.exists(fullfn) or _Util.shellCallWithRetCode("/usr/bin/7z t %s" % (fullfn))[0] != 0:
                print("Download file \"%s\"." % (filename))

                # get the real download url, gigabase sucks
                downloadUrl = None
                if True:
                    for elem in _Util.getWebPageElementTree(url).xpath(".//a"):
                        if elem.text == "Download file":
                            downloadUrl = elem.attrib["href"]
                            break
                    assert downloadUrl is not None

                # download
                tmpfn = fullfn + ".tmp"
                logFile = os.path.join(libmirrors.get_log_dir(), "wget.log")
                while True:
                    _Util.shellCall("/usr/bin/wget -O \"%s\" \"%s\" >\"%s\" 2>&1" % (tmpfn, downloadUrl, logFile))
                    # gigabase may show downloading page twice, re-get the real download url
                    if magic.detect_from_filename(tmpfn).mime_type == "text/html":
                        with open(tmpfn, "r") as f:
                            found = False
                            for elem in lxml.html.parse(f).xpath(".//a"):
                                if elem.text == "Download file":
                                    downloadUrl = elem.attrib["href"]
                                    found = True
                                    break
                            if found:
                                time.sleep(5.0)
                                continue
                    break
                os.rename(tmpfn, fullfn)
            else:
                print("File \"%s\" exists." % (filename))

            fnSet.add(filename)
            _Util.progress_changed(sock, PROGRESS_STAGE_1 + PROGRESS_STAGE_2 * i // total)
            i += 1

        # clear old files in cache
        for fn in (set(os.listdir(libmirrors.get_data_dir())) - fnSet):
            print("Remove old file \"%s\"." % (fn))
            fullfn = os.path.join(libmirrors.get_data_dir(), fn)
            os.unlink(fullfn)

        # recheck files
        # it seems sometimes wget download only partial files but there's no error
        for fn in fnSet:
            fullfn = os.path.join(libmirrors.get_data_dir(), fn)
            if _Util.shellCallWithRetCode("/usr/bin/7z t %s" % (fullfn))[0] != 0:
                raise Exception("file %s is not valid, strange?!" % (fn))

        # report full progress
        _Util.progress_changed(sock, 100)
    except:
        _Util.error_occured(sock, sys.exc_info())
        raise
    finally:
        sock.close()


class _Util:

    @staticmethod
    def connect():
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect("/run/mirrors/api.socket")
        return sock

    @staticmethod
    def progress_changed(sock, progress):
        sock.send(json.dumps({
            "message": "progress",
            "data": {
                "progress": progress,
            },
        }).encoding("utf-8"))

    @staticmethod
    def error_occured(sock, exc_info):
        sock.send(json.dumps({
            "message": "error_occured",
            "data": {
                "exc_info": "abc",
            },
        }).encoding("utf-8"))

    @staticmethod
    def randomSorted(tlist):
        return sorted(tlist, key=lambda x: random.random())

    @staticmethod
    def getWebPageElementTree(url):
        for i in range(0, 3):
            try:
                resp = urllib.request.urlopen(url, timeout=60, cafile=certifi.where())
                if resp.info().get('Content-Encoding') is None:
                    fakef = resp
                elif resp.info().get('Content-Encoding') == 'gzip':
                    fakef = io.BytesIO(resp.read())
                    fakef = gzip.GzipFile(fileobj=fakef)
                else:
                    assert False
                return lxml.html.parse(fakef)
            except urllib.error.URLError as e:
                if isinstance(e.reason, TimeoutError):
                    pass                                # retry 3 times
                else:
                    raise

    @staticmethod
    def shellCall(cmd):
        # call command with shell to execute backstage job
        # scenarios are the same as FmUtil.cmdCall

        ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             shell=True, universal_newlines=True)
        if ret.returncode > 128:
            # for scenario 1, caller's signal handler has the oppotunity to get executed during sleep
            time.sleep(1.0)
        if ret.returncode != 0:
            ret.check_returncode()
        return ret.stdout.rstrip()

    @staticmethod
    def shellCallWithRetCode(cmd):
        ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             shell=True, universal_newlines=True)
        if ret.returncode > 128:
            time.sleep(1.0)
        return (ret.returncode, ret.stdout.rstrip())


###############################################################################

if __name__ == "__main__":
    main()
