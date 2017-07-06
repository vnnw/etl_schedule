#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys
from configutil import ConfigUtil
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
        self.config = ConfigUtil()
        self.dbUtil = DBUtil()
        self.dboption = DBOption()

    # today 共运行完成 xx job
    def run(self):
        today = DateUtil.get_now_fmt(None)
        msg = []
        connection = self.dbUtil.get_connection()
        cursor = connection.cursor(MySQLdb.cursors.DictCursor)
        total_sql = "select count(*) as job_count from t_etl_job where date_format(pending_time,'%Y-%m-%d') = %s"
        cursor.execute(total_sql, (today,))
        row = cursor.fetchone()
        msg.append("总的任务数:" + str(row['job_count']))
        sql = "select job_status,count(*) as job_count from t_etl_job where last_start_time >= %s group by job_status"
        cursor.execute(sql, (today,))
        rows = cursor.fetchall()
        for row in rows:
            msg.append(str(row['job_status']) + ":" + str(row['job_count']))
        connection.close()
        main_phones = self.dboption.get_main_man_by_role("admin")
        phones = set()
        for main_phone in main_phones:
            phones.add(main_phone['user_phone'])
        if not phones or len(phones) == 0:
            print("没有配置短信发送phone")
            return
        data = {
            "mobile": ",".join(phones),
            "template": "super_template",
            "data": {
                "content": today + " 运行日志信息:\n" + str(",\n".join(msg))
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
