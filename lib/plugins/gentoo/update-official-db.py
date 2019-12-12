#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
import json
import certifi
import lxml.html
import urllib.request
from collections import OrderedDict


def convertDict(srcDict):
    ret = dict()
    for url, prop in srcDict.items():
        if prop["protocol"] != "rsync":
            continue
        url = convertUrl(url)
        if url is None:
            continue
        ret[url] = prop
    return ret


def convertUrl(srcUrl):
    url = srcUrl.rstrip("/")
    if not url.endswith("/gentoo"):
        return None
    url += "-portage"
    return url


if __name__ == "__main__":
    selfDir = os.path.dirname(os.path.realpath(__file__))

    url = "https://www.gentoo.org/downloads/mirrors"
    resp = urllib.request.urlopen(url, timeout=60, cafile=certifi.where())
    root = lxml.html.parse(resp)

    # we don't use region information currently
    regionDict = dict()
    if False:
        elemTitle = root.xpath(".//h2[text()='Countries covered by Gentoo source mirrors']")[0]
        elemTable = elemTitle.getnext()
        for elemTr in elemTable.xpath(".//tr")[1:]:
            elemTh = elemTr.xpath(".//th")[0]
            print(elemTh.text)
            for elemA in elemTr.xpath(".//a"):
                print(elemA.text)

    # parse all mirrors
    mirrorDict = OrderedDict()
    for elemTitle in root.xpath(".//h3"):
        elemTable = elemTitle.getnext()
        curName = None
        for elemTr in elemTable.xpath(".//tr")[1:]:
            elemTdList = elemTr.xpath(".//td")
            if len(elemTdList) == 4:
                curName = elemTdList[0].text
                proto = elemTdList[1].xpath(".//span")[0].text
                url = elemTdList[3].xpath(".//code")[0].text
            elif len(elemTdList) == 3:
                proto = elemTdList[0].xpath(".//span")[0].text
                url = elemTdList[2].xpath(".//code")[0].text
            else:
                continue
            mirrorDict[url] = {
                "name": curName,
                "country": elemTitle.get("id"),
                "protocol": proto,
            }

    # write to database file
    with open(os.path.join(selfDir, "db-official.json"), "w") as f:
        f.write(json.dumps(mirrorDict, indent=4))

    # write to portage database file
    with open(os.path.join(selfDir, "db-portage-official.json"), "w") as f:
        f.write(json.dumps(convertDict(mirrorDict), indent=4))
