#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys
from dateutil import DateUtil
from dbutil import DBUtil
import json
import MySQLdb
import urllib2
from dboption import DBOption

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)


class ETLMonitor(object):
    def __init__(self):
        self.dbUtil = DBUtil()
        self.dboption = DBOption()

    # today 共运行完成 xx job
    def run(self):
        today = DateUtil.get_now_fmt(None)
        connection = self.dbUtil.get_connection()
        sql = "select count(*) as job_count from t_etl_job where job_status = 'Done' and last_run_date = %s"
        cursor = connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(sql, (today,))
        row = cursor.fetchone()
        print row['job_count']
        main_phone = self.dboption.get_main_man_user("yxl")
        phone = main_phone['user_phone']
        data = {
            "mobile": ",".join([phone]),
            "template": "super_template",
            "data": {
                "content": today + " 运行完成任务数:" + str(row['job_count'])
            }
        }
        host = self.config.get("sms.host")
        request = urllib2.Request(url="http://" + host + "/api/v1/sms/send/template", data=json.dumps(data))
        request.add_header('Content-Type', 'application/json')
        response = urllib2.urlopen(request)
        print(response.read())


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    monitor = ETLMonitor()
    monitor.run()
