#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import os
# import gi
import sys
import json
import signal
# gi.require_version('Gst', '1.0')
# gi.require_version('GstRtspServer', '1.0')
from gi.repository import GLib
from gi.repository import Gst
from gi.repository import GstRtspServer


class MyRtspMediaFactory(GstRtspServer.RTSPMediaFactory):

    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self.set_shared(True)

    def do_create_element(self, url):
        # set mp4 file path to filesrc's location property
        src_demux = "filesrc location=%s ! qtdemux name=demux" % (self.filename)
        h264_transcode = "demux.video_0"
        # uncomment following line if video transcoding is necessary
        # h264_transcode = "demux.video_0 ! decodebin ! queue ! x264enc"
        pipeline = "{0} {1} ! queue ! rtph264pay name=pay0 config-interval=1 pt=96".format(src_demux, h264_transcode)
        return Gst.parse_launch(pipeline)


def refreshCfgFromCfgFile():
    global cfgFile
    global cfg

    with open(cfgFile, "r") as f:
        buf = f.read()
        if buf == "":
            raise Exception("no content in config file")
        dataObj = json.loads(buf)

        if "ip" not in dataObj:
            raise Exception("no \"ip\" in config file")
        if "port" not in dataObj:
            raise Exception("no \"port\" in config file")
        if "media-file-map" not in dataObj:
            raise Exception("no \"media-file-map\" in config file")
        for key, value in dataObj["media-file-map"].items():
            if not os.path.isabs(value) or value.endswith("/"):
                raise Exception("value of \"%s\" in \"media-file-map\" is invalid" % (key))

        if "ip" not in cfg:
            cfg["ip"] = dataObj["ip"]                               # cfg["ip"] is not changable
        if "port" not in cfg:
            cfg["port"] = dataObj["port"]                           # cfg["port"] is not changable
        if "media-file-map" in cfg:
            cfg["old-media-file-map"] = cfg["media-file-map"]
        cfg["media-file-map"] = dataObj["media-file-map"]


def refreshMediaFileMapFromCfg():
    global cfg
    global rtspServer

    mountPoints = rtspServer.get_mount_points()

    # remove
    for key in cfg["old-media-file-map"]:
        if key not in cfg["media-file-map"]:
            mountPoints.remove_factory(key)

    # add or change
    for key, filename in cfg["media-file-map"].items():
        if key in cfg["old-media-file-map"]:
            if filename != cfg["old-media-file-map"][key]:
                mountPoints.remove_factory(key)
                mountPoints.add_factory(key, MyRtspMediaFactory(filename))
        else:
            mountPoints.add_factory(key, MyRtspMediaFactory(filename))


def runServer():
    global cfg
    global rstpServer

    Gst.init(None)
    rtspServer = GstRtspServer.RTSPServer()
    rstpServer.set_address(cfg["ip"])
    rstpServer.set_service(cfg["port"])
    rtspServer.attach(None)
    refreshMediaFileMapFromCfg()
    GLib.MainLoop().run()


def sigHandler(signum, frame):
    refreshCfgFromCfgFile()
    refreshMediaFileMapFromCfg()


if __name__ == '__main__':
    cfgFile = sys.argv[1]
    cfg = dict()
    rstpServer = None
    refreshCfgFromCfgFile()
    signal.signal(signal.SIGUSR1, sigHandler)
    runServer()
