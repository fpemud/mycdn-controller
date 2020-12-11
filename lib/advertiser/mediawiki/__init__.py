#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-


class Advertiser:

    @staticmethod
    def get_properties():
        return {
            "storage-dependencies": ["file", "mariadb"],
        }

    def __init__(self, param):
        pass

    def dispose(self):
        pass

    def get_access_info(self, mirror_site_id):
        return {
            "url": "",
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        pass
