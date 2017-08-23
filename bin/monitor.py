#!/usr/bin/python
# -*- coding:utf-8 -*-

import json
import urllib2

from configutil import ConfigUtil
from dboption import DBOption
from smsutil import SMSUtil

class Monitor(object):

    def __init__(self):
        self.dboption = DBOption()
        self.config = ConfigUtil()
        self.smsUtil = SMSUtil()

    def monitor_all(self, job_name):
        main_phones = self.dboption.get_main_man()
        phones = set()
        for main_man in main_phones:
            phones.add(main_man["user_phone"])
        content = "etl_schedule job:" + job_name + " 运行失败,需要修复"
        response = self.smsUtil.send(",".join(phones), content)
        print("sms response:" + str(response))

    def monitor(self, job_name):
        etl_job = self.dboption.get_job_info(job_name)
        if etl_job is None:
            print("job 信息不存," + str(job_name))
        else:
            main_man = etl_job["main_man"]
            monitor_user = self.dboption.get_main_man_user(main_man)
            if monitor_user is None:
                print("收件人信息不存在," + str(main_man))
            else:
                phone = monitor_user["user_phone"]
                content = "etl_schedule job:" + job_name + " 运行失败,需要修复"
                response = self.smsUtil.send(phone, content)
                print("sms response:" + str(response))

# test
if __name__ == '__main__':
    monitor = Monitor()
    monitor.monitor("STG_T_TEST_1")
