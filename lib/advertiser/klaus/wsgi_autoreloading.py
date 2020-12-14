#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import glob
import time
import threading
from klaus import make_app


# Shared state between poller and application wrapper
class _S:
    #: the real WSGI app
    inner_app = None
    should_reload = True
    namespace_dict = None


def _get_namespace_dict(repos_root):
    namespace_dict = dict()
    for fullfn in glob.glob(repos_root + "/*"):
        if os.path.exists(os.path.join(fullfn, ".git")):
            if None not in namespace_dict:
                namespace_dict[None] = []
            namespace_dict[None].append(fullfn)
        else:
            fn = os.path.basename(fullfn)
            if fn not in namespace_dict:
                namespace_dict[fn] = glob.glob(fullfn + "/*")


def _poll_for_changes(interval, dir):
    """
    Polls `dir` for changes every `interval` seconds and sets `should_reload`
    accordingly.
    """
    while True:
        time.sleep(interval)
        if _S.should_reload:
            # klaus application has not seen our change yet
            continue
        new_contents = _get_namespace_dict(dir)
        if new_contents != _S.namespace_dict:
            # Directory contents changed => should_reload
            _S.namespace_dict = new_contents
            _S.should_reload = True


def make_autoreloading_app(repos_root, *args, **kwargs):
    # Create namespace_dict
    _S.namespace_dict = _get_namespace_dict(repos_root)

    # Define web request handler
    def app(environ, start_response):
        if _S.should_reload:
            # Refresh inner application with new repo list
            print("Reloading repository list...")
            _S.inner_app = make_app(_S.namespace_dict, *args, **kwargs)
            _S.should_reload = False
        return _S.inner_app(environ, start_response)

    # Start a background thread that polls the directory for changes
    poller_thread = threading.Thread(target=(lambda: _poll_for_changes(10, repos_root)))
    poller_thread.daemon = True
    poller_thread.start()

    return app
