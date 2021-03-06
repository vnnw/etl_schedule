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

    def remove_etl_job(self, job_name, should_check=True):
        job_info = self.dboption.get_job_info(job_name)
        if job_info is None:
            print(job_name + " 不存在,无法删除")
            return
        if should_check:
            # 判断是否被依赖
            dependened_jobs = self.dboption.get_depended_job(job_name)
            depended = False
            if dependened_jobs is not None:
                for dependened_job in dependened_jobs:
                    print(dependened_job["job_name"] + " 依赖 " + job_name)
                    depended = True
            if depended:
                print(job_name + " 被依赖无法删除")
                return
        stream_jobs = self.dboption.get_stream_job(job_name)
        stream_job_set = set()
        for stream_job in stream_jobs:
            stream_job_set.add(stream_job["job_name"])
        print("删除 job_name:" + job_name + " 不会删除触发执行的job:[" + ",".join(stream_job_set, ) + "]!!!")
        # 删除时间触发
        self.dboption.remove_etl_job_trigger(job_name)
        # 删除 触发
        self.dboption.remove_etl_stream_job(job_name)
        # 删除 依赖
        self.dboption.remove_etl_dependency_job(job_name)
        # 删除 配置
        self.dboption.remove_etl_job(job_name)

    def parse_line(self, line):
        if line is None or len(line.strip()) == 0:
            # print("读取的记录为空")
            pass
        else:
            line = line.strip()
            print line
            line_array = line.split(",")
            job_name = line_array[0].upper()
            job_info = self.dboption.get_job_info(job_name)
            job_trigger_info = self.dboption.get_etl_job_trigger(job_name)
            if job_info or job_trigger_info:
                # raise Exception("Job:" + job_name + " 已经存在")
                print("Job:" + job_name + " 已经存在,需要删除后重新创建!")
                self.remove_etl_job(job_name, should_check=False)
            trigger_type = line_array[1]
            man = line_array[len(line_array) - 2]
            script = line_array[len(line_array) - 1].strip()
            script_path = self.config.get("job.script.path") + "/script/" + script
            self.check_script_path(script_path)
            if trigger_type and trigger_type in ("time", "dependency"):
                if trigger_type == "time":
                    day = line_array[2]
                    hour = line_array[3]
                    minute = line_array[4]
                    interval = line_array[5]
                    valid = self.check_trigger_job(day, interval)
                    if not valid:
                        raise Exception("Job 配置错误")

                    deps = line_array[6]
                    add_job_dep_sets = set()
                    if deps != man:
                        print "时间触发需要添加依赖:", deps
                        for dep_job in deps.split(" "):
                            job = self.dboption.get_job_info(dep_job)
                            if job is None:
                                print "依赖的 Job:" + str(dep_job) + " 不存在"
                                raise Exception("依赖Job :" + dep_job + " 不存在")
                            add_job_dep_sets.add(dep_job)
                        print("需要配置依赖:" + ",".join(add_job_dep_sets))

                    code = self.dboption.save_time_trigger_job(job_name, trigger_type,
                                                               day, hour, minute,
                                                               interval, man, script, add_job_dep_sets)

                    if code == 1:
                        print("添加时间触发Job 成功")
                    else:
                        print("添加时间触发Job 失败")

                elif trigger_type == "dependency":
                    dep_jobs = line_array[2].strip().upper()
                    stream = line_array[3].strip().upper()
                    # 依赖job,已经依赖的
                    add_job_dep_sets = set()
                    for dep_job in dep_jobs.split(" "):
                        job = self.dboption.get_job_info(dep_job)
                        if job is None:
                            print "依赖的 Job:" + str(dep_job) + " 不存在"
                            raise Exception("依赖Job :" + dep_job + " 不存在")
                        add_job_dep_sets.add(dep_job)
                    print("需要配置依赖:" + ",".join(add_job_dep_sets))
                    stream_job = self.dboption.get_job_info(stream)
                    if stream_job is None:
                        raise Exception("Job:" + stream + " 不存在")
                    code = self.dboption.save_depdency_trigger_job(job_name, trigger_type, add_job_dep_sets, stream,
                                                                   man,
                                                                   script,)
                    if code == 1:
                        print("添加依赖触发Job 成功")
                    else:
                        print("添加依赖触发 Job 失败")
            else:
                raise Exception("配置 job 触发方式 :" + str(trigger_type))
                print(self.time_format)
                print(self.depdency_format)
                raise Exception("配置格式错误")

    def check_script_path(self, path):
        exists = os.path.exists(path)
        if not exists:
            raise Exception("脚本:" + path + " 不存在")

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

    def rename_job(self,etl_job_name):
        etl_job_array = etl_job_name.strip().split(" ")
        if etl_job_array is None or len(etl_job_array) != 3:
            raise Exception("修改job 名称格式: 原名称 新名称 脚本相对路径")
        from_job = etl_job_array[0].strip()
        to_job = etl_job_array[1].strip()
        print("修改的名称:" + from_job + " -> " + to_job)
        from_job_info = self.dboption.get_job_info(from_job)
        if not from_job_info:
            raise Exception("原 Job 名称 " + from_job + " 不存在")
        to_job_info = self.dboption.get_job_info(to_job)
        if to_job_info:
            raise Exception("新 Job 名称 " + from_job + " 已存在")

        yaml_file = etl_job_array[2].strip().lower()
        script_path = self.config.get("job.script.path") + "/script/" + yaml_file
        self.check_script_path(script_path)

        # trigger
        update_trigger = "update t_etl_job_trigger set job_name = %s where job_name = %s"
        self.dboption.execute_sql(update_trigger, (to_job, from_job))

        # stream
        update_stream_1 = "update t_etl_job_stream set job_name = %s where job_name = %s"
        self.dboption.execute_sql(update_stream_1, (to_job, from_job))
        update_stream_2 = "update t_etl_job_stream set stream_job = %s where stream_job = %s"
        self.dboption.execute_sql(update_stream_2, (to_job, from_job))

        # dependency
        update_dependency_1 = "update t_etl_job_dependency set job_name = %s where job_name = %s"
        self.dboption.execute_sql(update_dependency_1, (to_job, from_job))
        update_dependency_2 = "update t_etl_job_dependency set dependency_job = %s where dependency_job = %s"
        self.dboption.execute_sql(update_dependency_2, (to_job, from_job))

        # job
        update_job = "update t_etl_job set job_name = %s , job_script=%s where job_name = %s"
        self.dboption.execute_sql(update_job, (to_job, yaml_file, from_job))

        self.query_etl_job(to_job)

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    etl = ETLUtil()

    # args_array = "-p /Users/yxl/Desktop/job.txt".split(" ")
    # args_array = "-q stg_t_test_1".split(" ")
    # opts,args = getopt.getopt(args_array, "p:q:", ["help", "path=","query="])

    opts, args = getopt.getopt(sys.argv[1:], "p:q:d:r:", ["help", "path=", "query=", "delete=", "rename"])
    for option, value in opts:
        if option in ["--path", "-p"]:
            etl.read_file(value.strip())
        elif option in ["--query", "-q"]:
            etl.query_etl_job(value.upper())
        elif option in ["--delete", "-d"]:
            etl.remove_etl_job(value.upper())
        elif option in ["--rename", "-r"]:
            etl.rename_job(value.upper())
        else:
            print("nothing to do")
            sys.exit(0)
