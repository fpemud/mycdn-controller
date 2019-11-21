#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import sys


class McMirrorSiteUpdaterApiProcess:

    def __init__(self):
        self.dataDir = sys.argv[1]
        self.logDir = sys.argv[2]

    def get_country(self):
        # FIXME
        return "CN"

    def get_location(self):
        # FIXME
        return None

    def get_data_dir(self):
        return self.dataDir

    def get_log_dir(self):
        return self.logDir

    def notify_progress(self, progress, finished):
        assert False


if __name__ == "__main__":
    assert False
