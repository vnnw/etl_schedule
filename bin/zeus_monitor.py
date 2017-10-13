#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys
import MySQLdb
from configutil import ConfigUtil
from smsutil import SMSUtil
from dateutil import DateUtil

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)


configUtil = ConfigUtil()
smsUtil = SMSUtil()

def get_zeus_connection():
        host = configUtil.get("mysql.host")
        username = configUtil.get("mysql.username")
        password = configUtil.get("mysql.password")
        port = int(configUtil.get("mysql.port"))
        db = "db_zeus"
        connection = MySQLdb.connect(host, username, password,db , port, use_unicode=True, charset='utf8')
        return connection

def run():
    date_str = DateUtil.get_now_fmt("%Y%m%d")
    query_date_str = date_str + "0" * 10
    zeus_connection = get_zeus_connection()
    query_sql = """select status,count(id) as action_count from zeus_action where id >= %s group by status """
    cursor = zeus_connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query_sql, (query_date_str,))
    rows = cursor.fetchall()
    null_count = 0
    success_count = 0
    running_count = 0
    failed_count = 0
    other_count = 0
    for row in rows:
        status = row["status"]
        if status is None:
            null_count = row["action_count"]
        elif status == "failed":
            failed_count = row["action_count"]
        elif status == "running":
            running_count = row["action_count"]
        elif status == "success":
            success_count = row["action_count"]
        else:
            print("other:" + str(status))
            other_count = row["action_count"]
    total_count = null_count + success_count + running_count + failed_count + other_count
    msg = ["总的任务数:" + str(total_count),
           "未运行任务数:" + str(null_count),
           "运行中任务数:" + str(running_count),
           "运行完成任务数:" + str(success_count),
           "运行失败任务数:" + str(failed_count)]
    content = date_str + " 运行日志信息:\n" + str(",\n".join(msg)) + "\n"
    query_user_sql = """select phone from zeus_user where is_effective = 1 and uid !='admin' """
    cursor.execute(query_user_sql, ())
    rows = cursor.fetchall()
    phones = set()
    for row in rows:
        phones.add(row["phone"])
    response = smsUtil.send(",".join(phones), content)
    print(response)

if __name__ == '__main__':
    run()