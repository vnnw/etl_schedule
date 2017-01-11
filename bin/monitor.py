#!/usr/bin/python
# -*- coding:utf-8 -*-

import json
import urllib2

from configutil import ConfigUtil
from dboption import DBOption


class Monitor(object):

    def __init__(self):
        self.dboption = DBOption()
        self.config = ConfigUtil()

    def monitor_all(self, job_name):
        main_phones = self.dboption.get_main_man()
        phones = set()
        for main_man in main_phones:
            phones.add(main_man["user_phone"])
        data = {
            "mobile": ",".join(phones),
            "template": "super_template",
            "data": {
                "content": "etl_schedule job:" + job_name + " 运行失败,需要修复"
            }
        }
        host = self.config.get("sms.host")
        request = urllib2.Request(url="http://" + host + "/api/v1/sms/send/template", data=json.dumps(data))
        request.add_header('Content-Type', 'application/json')
        response = urllib2.urlopen(request)
        print(response.read())

    def monitor(self, job_name):
        etl_job = self.dboption.get_job_info(job_name)
        main_man = etl_job["main_man"]
        main_phone = self.dboption.get_main_man_user(main_man)
        data = {
            "mobile": main_phone["user_phone"],
            "template": "super_template",
            "data": {
                "content": "etl_schedule job:" + job_name + " 运行失败,需要修复"
            }
        }
        host = self.config.get("sms.host")
        request = urllib2.Request(url="http://" + host + "/api/v1/sms/send/template", data=json.dumps(data))
        request.add_header('Content-Type', 'application/json')
        response = urllib2.urlopen(request)
        print(response.read())

# test
if __name__ == '__main__':
    monitor = Monitor()
    monitor.monitor_all("STG_T_TEST_1")
