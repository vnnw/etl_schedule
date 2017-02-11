#!/usr/bin/python
# -*- coding:utf-8 -*-

from dboption import DBOption
import sys
import getopt
import os
from configutil import ConfigUtil


class ETLUtil(object):
    def __init__(self):
        self.dboption = DBOption()
        self.config = ConfigUtil()
        self.time_format = "时间触发格式: Job名称,time,触发天(每天0),触发小时,触发分钟,触发周期(day|month),负责人,脚本位置"
        self.depdency_format = "依赖触发格式:Job名称,dependency,依赖Job(多个用空格分隔),触发Job,负责人,脚本位置"

    def check_trigger_job(self, day, interval):
        if int(day) == 0 and interval == 'month':
            print("day =0 表示每天触发, 触发周期应该为day")
            return False
        if int(day) != 0 and interval == 'day':
            print("触发周期为day 表示每天,需要设置触发天为 0")
            return False
        return True

    def parse_line(self, line):
        if line is None or len(line) == 0:
            print("读取的记录为空")
        else:
            line = line.strip()
            print line
            line_array = line.split(",")
            if len(line_array) != 6 and len(line_array) != 8:
                print("配置格式错误")
                print(self.time_format)
                print(self.depdency_format)
                raise Exception("配置格式错误")
            else:
                job_name = line_array[0].upper()
                trigger_type = line_array[1]
                man = line_array[len(line_array) - 2]
                script = line_array[len(line_array) - 1].strip()
                script_path = self.config.get("job.script.path") + "/script/" + script
                self.check_script_path(script_path)
                if len(line_array) == 8:
                    day = line_array[2]
                    hour = line_array[3]
                    minute = line_array[4]
                    interval = line_array[5]
                    valid = self.check_trigger_job(day, interval)
                    if not valid:
                        raise Exception("Job 配置错误")
                    code = self.dboption.save_time_trigger_job(job_name, trigger_type,
                                                               day, hour, minute, interval, man, script)
                    if code == 1:
                        print("添加时间触发Job 成功")
                    else:
                        print("添加时间触发Job 失败")

                if len(line_array) == 6:
                    dep_jobs = line_array[2].strip().upper()
                    stream = line_array[3].strip().upper()
                    for dep_job in dep_jobs.split(" "):
                        job = self.dboption.get_job_info(dep_job)
                        if job is None:
                            raise Exception("Job :" + dep_job + " 不存在")
                    stream_job = self.dboption.get_job_info(stream)
                    if stream_job is None:
                        raise Exception("Job:" + stream + " 不存在")
                    code = self.dboption.save_depdency_trigger_job(job_name, trigger_type, dep_jobs, stream, man,
                                                                   script)
                    if code == 1:
                        print("添加依赖触发Job 成功")

    def check_script_path(self, path):
        exists = os.path.exists(path)
        if not exists:
            raise Exception("脚本:" + path + " 不存在")

    def remove_etl_job(self, job_name):
        print("删除Job:" + job_name)
        code = self.dboption.remove_etl_job(job_name)
        if code == 1:
            print("删除Job:" + job_name)

    def query_etl_job(self, job_name):
        print("查询Job:" + job_name)
        etl_job = self.dboption.get_job_info(job_name)
        if etl_job is None:
            print("没有找Job:" + job_name)
        else:
            print("------job-----")
            print("job:" + str(etl_job))
            trigger = etl_job["job_trigger"]
            if trigger == "time":
                etl_job_trigger = self.dboption.get_etl_job_trigger(job_name)
                print("-------trigger-----")
                print(str(etl_job_trigger))
            dep_jobs = self.dboption.get_dependency_job(job_name)
            print("------deps-----")
            if dep_jobs is not None and len(dep_jobs) != 0:
                for dep_job in dep_jobs:
                    print(str(dep_job['dependency_job']))
            print("-----nexts-----")
            next_jobs = self.dboption.get_stream_job(job_name)
            if next_jobs is not None and len(next_jobs) != 0:
                for next_job in next_jobs:
                    print(str(next_job['stream_job']))
            print("-----before----")
            before = self.dboption.get_before_job(job_name)
            if before is not None:
                print(str(before['job_name']))

    def read_file(self, path):
        file_handler = open(path, 'r')
        for line in file_handler.readlines():
            if line is not None and len(line) > 0 and not line.startswith("#"):
                self.parse_line(line)


if __name__ == '__main__':
    etl = ETLUtil()

    # args_array = "-p /Users/yxl/Desktop/job.txt".split(" ")
    # args_array = "-q stg_t_test_1".split(" ")
    # opts,args = getopt.getopt(args_array, "p:q:", ["help", "path=","query="])

    opts, args = getopt.getopt(sys.argv[1:], "p:q:", ["help", "path=", "query="])
    for option, value in opts:
        if option in ["--path", "-p"]:
            etl.read_file(value.strip())
        elif option in ["--query", "-q"]:
            etl.query_etl_job(value.upper())
        else:
            print("nothing to do")
            sys.exit(0)
