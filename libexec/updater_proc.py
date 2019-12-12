#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-

import sys
sys.path.append('/usr/lib64/mirrors')
from mc_util import DynObject

def main():






def _createInitApi(self):
    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: self.mirrorSite.dataDir
    api.get_log_dir = lambda: self.param.logDir
    api.progress_changed = self.initProgressCallback
    return api

def _createUpdateApi(self):
    api = DynObject()
    api.get_country = lambda: "CN"
    api.get_location = lambda: None
    api.get_data_dir = lambda: self.mirrorSite.dataDir
    api.get_log_dir = lambda: self.param.logDir
    api.progress_changed = self.initProgressCallback
    return api


if __name__ == "__main__":
    assert False
