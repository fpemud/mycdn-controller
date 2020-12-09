#!/usr/bin/python3
# -*- coding: utf-8; tab-width: 4; indent-tabs-mode: t -*-


class Storage:

    @staticmethod
    def get_proprites():
        return {
            "with-integrated-advertiser": True,
        }

    def __init__(self, param):
        pass

    def dispose(self):
        pass

    def get_param(self, mirror_site_id):
        return {
            "port": -1,
            "database": mirror_site_id,
        }

    def advertise_mirror_site(self, mirror_site_id):
        pass
