# -*- coding:utf-8 -*-

import os
import sys
from configutil import ConfigUtil
from dateutil import DateUtil
from dbutil import DBUtil
from smsutil import SMSUtil
import json
import MySQLdb
import urllib2
from dboption import DBOption
import datetime
import traceback

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)


class ETLMonitor(object):
    def __init__(self):
        self.config = ConfigUtil()
        self.dbUtil = DBUtil()
        self.dboption = DBOption()
        self.smsUtil = SMSUtil()

    def get_dependency(self, cursor, job_name, dep_jobs):
        dep_sql = "select job_name,dependency_job from t_etl_job_dependency where job_name = %s"
        cursor.execute(dep_sql, (job_name,))
        deps = cursor.fetchall()
        for dep_job in deps:
            dep_jobs.add(dep_job["dependency_job"])
            self.get_dependency(cursor, dep_job["dependency_job"], dep_jobs)
        return dep_jobs

    def run_check(self, cursor, today, msg):
        job_sql = "select job_name,last_start_time,last_end_time from t_etl_job where 1=1 "

        cursor.execute(job_sql + " and last_run_date=%s", (today,))
        jobs = cursor.fetchall()

        count = 0
        failed = 0
        error = 0
        failed_job = []
        for job in jobs:
            job_name = job["job_name"]
            job_start_time = datetime.datetime.strptime(job["last_start_time"], "%Y-%m-%d %H:%M:%S")
            dep_jobs = set()
            self.get_dependency(cursor, job_name, dep_jobs)
            for dep_job in dep_jobs:
                cursor.execute(job_sql + " and job_name = %s", (dep_job,))
                dep_job_detail = cursor.fetchone()
                # print dep_job_detail
                try:
                    dep_job_end_time = datetime.datetime.strptime(dep_job_detail["last_end_time"], "%Y-%m-%d %H:%M:%S")
                    duration = (job_start_time - dep_job_end_time).total_seconds()
                    if duration <= 0:
                        failed += 1
                        print job_name, job_start_time, dep_job, dep_job_end_time, str(duration)
                        failed_job.append(job_name)
                except Exception as e:
                    print traceback.format_exc()
                    print "job->", job
                    print "dep_job->", dep_job_detail
                    error += 1
            count += 1
        print "check:" + str(count) + " jobs failed:" + str(failed) + " exception:" + str(error)
        if len(failed_job) > 0 or failed > 0:
            msg.append("调度异常数:" + str(failed))
            msg.append("调度异常任务:" + str(",".join(failed_job)))

    def run_count(self, cursor, today, msg):
        total_sql = 'select count(*) as job_count from t_etl_job where pending_time >= %s '
        cursor.execute(total_sql, (today,))
        row = cursor.fetchone()
        msg.append("总的任务数:" + str(row['job_count']))
        sql = "select job_status,count(*) as job_count from t_etl_job where last_start_time >= %s group by job_status"
        cursor.execute(sql, (today,))
        rows = cursor.fetchall()
        for row in rows:
            msg.append(str(row['job_status']) + ":" + str(row['job_count']))

    # today 共运行完成 xx job
    def run(self):
        today = DateUtil.get_now_fmt(None)
        msg = []
        connection = self.dbUtil.get_connection()
        cursor = connection.cursor(MySQLdb.cursors.DictCursor)

        self.run_count(cursor, today, msg)

        self.run_check(cursor, today, msg)

        connection.close()

        main_phones = self.dboption.get_main_man_by_role("admin")
        phones = set()
        for main_phone in main_phones:
            phones.add(main_phone['user_phone'])
        if not phones or len(phones) == 0:
            print("没有配置短信发送phone")
            return
        content = today + " 运行日志信息:\n" + str(",\n".join(msg))
        response = self.smsUtil.send(",".join(phones), content)
        print(response)


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    monitor = ETLMonitor()
    monitor.run()
