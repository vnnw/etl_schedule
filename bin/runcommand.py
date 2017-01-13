#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys

import subprocess
from configutil import ConfigUtil
from dboption import DBOption
from yamlparser import YamlParser
import yaml
import traceback

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)
from export.hiveutil import HiveUtil

'''
用来运行具体的脚本
'''


class RunCommand(object):
    def __init__(self):
        self.config = ConfigUtil()
        self.dboption = DBOption()

    def get_python_bin(self):
        python_path = self.config.get("python.home")
        if python_path is None or len(python_path) == 0:
             raise Exception("can't find python.home")
        python_bin = python_path + "/bin/python"
        return python_bin

    # run command
    def run_command(self, job_name):
        try:

            etl_job = self.dboption.get_job_info(job_name)

            job_script = etl_job["job_script"]

            job_script = self.config.get("job.script.path") + "/script/" + job_script

            print(" 需要运行的Job名称:" + job_name + " 脚本位置:" + job_script)

            if not os.path.exists(job_script):
                raise Exception("can't find run script:" + job_script)

            extend = os.path.splitext(job_script)[1]

            if extend == ".py":
                child = subprocess.Popen([self.get_python_bin(), job_script, "-job", job_name],
                                         stdout=None,
                                         stderr=subprocess.STDOUT,
                                         shell=False)
                if child is None:
                    raise Exception("创建子进程运行脚本:" + job_script + "异常")
                    print("创建子进程:" + str(child.pid))
                    code = child.wait()
                    return code

            elif extend == ".sh":
                child = subprocess.Popen([job_script, "-job", job_name],
                                         stdout=None,
                                         stderr=subprocess.STDOUT,
                                         shell=True)
                if child is None:
                    raise Exception("创建子进程运行脚本:" + job_script + "异常")
                    print("创建子进程:" + str(child.pid))
                    code = child.wait()
                    return code

            elif extend == ".yml":
                return self.run_yaml(job_script)

            else:
                raise Exception("当前只支持 python , shell , yaml 脚本")
        except Exception, e:
            print("Executor 上 job 运行失败:" + job_name)
            print(traceback.format_exc())
            return 1

    def run_single_command(self, command):
        print "start run command: " + " ".join(command)
        child = subprocess.Popen(command,
                                 stdout=None,
                                 stderr=subprocess.STDOUT,
                                 shell=False)
        code = child.wait()
        return code

    def run_yaml(self,yaml_file):
        yaml_sql_path = self.config.get("job.script.path") + "/sql"
        yaml_parser = YamlParser()
        yaml_file = open(yaml_file, 'r')
        yaml_dict = yaml.safe_load(yaml_file)
        steps = yaml_dict['steps']
        if steps and len(steps) > 0:
            for step in steps:
                step_type = step['type']
                if step_type == 'hive':
                    (vars, sqls, sql_paths) = yaml_parser.parse_hive(step)
                    if sqls or sql_paths:
                        hive_util = HiveUtil()
                        hive_util.add_vars(vars)
                        hive_util.add_sqls(sqls)
                        hive_util.add_sql_paths(yaml_sql_path, sql_paths)
                        code = hive_util.run_sql_client()
                        if code != 0:
                            return 1
                if step_type == 'export':
                    command_list = yaml_parser.parse_export(self.get_python_bin(), project_path, step)
                    if command_list and len(command_list) > 0:
                        for command in command_list:
                            code = self.run_single_command(command)
                            if code != 0:
                                return 1
            return 0
        else:
            return 0


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    job = sys.argv[2]
    if job is None or len(job) == 0:
        print("无法获取 job 名称")
        exit(-1)
    runCommand = RunCommand()
    code = runCommand.run_command(job)
    if code == 0:
        sys.exit(0)
    else:
        sys.exit(1)
