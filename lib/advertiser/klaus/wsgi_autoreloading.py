#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-
# This file is based on https://github.com/jonashaag/klaus/klaus/contrib/wsgi_autoreloading.py

from __future__ import print_function
import glob
import time
import threading

from klaus import make_app


# Shared state between poller and application wrapper
class S:
    #: the real WSGI app
    inner_app = None
    should_reload = True


def poll_for_changes(interval, dir):
    """
    Polls `dir` for changes every `interval` seconds and sets `should_reload`
    accordingly.
    """
    glob_pattern = dir + "/*"
    old_contents = glob.glob(glob_pattern)
    while 1:
        time.sleep(interval)
        if S.should_reload:
            # klaus application has not seen our change yet
            continue
        new_contents = glob.glob(glob_pattern)
        if new_contents != old_contents:
            # Directory contents changed => should_reload
            old_contents = new_contents
            S.should_reload = True


def make_autoreloading_app(repos_root, *args, **kwargs):
    def app(environ, start_response):
        if S.should_reload:
            # Refresh inner application with new repo list
            print("Reloading repository list...")
            namespaceDict = dict()
            for fullfn in glob.glob(repos_root + "/*"):
                if os.path.exists(os.path.join(fullfn, ".git")):
                    if None not in namespaceDict:
                        namespaceDict[None] = []
                    namespaceDict[None].append(fullfn)
                else:
                    fn = os.path.basename(fullfn)
                    if fn not in namespaceDict:
                        namespaceDict[fn] = glob.glob(fullfn + "/*")
            S.inner_app = make_app(namespaceDict, *args, **kwargs)
            S.should_reload = False
        return S.inner_app(environ, start_response)

    # Background thread that polls the directory for changes
    poller_thread = threading.Thread(target=(lambda: poll_for_changes(10, repos_root)))
    poller_thread.daemon = True
    poller_thread.start()

    return app
