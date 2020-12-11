#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-


class Storage:

    @staticmethod
    def get_properties():
        return {
            "with-integrated-advertiser": True,
        }

    def __init__(self, param):
        pass

    def dispose(self):
        pass

    def get_param(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "port": -1,
            "database": mirror_site_id,
        }

    def get_access_info(self, mirror_site_id):
        assert mirror_site_id in self._mirrorSiteDict
        return {
            "url": "mongodb://{IP}:%d/%s" % (-1, mirror_site_id),
            "description": "",
        }

    def advertise_mirror_site(self, mirror_site_id):
        pass
