#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import traceback
import sys
from configutil import ConfigUtil
from runcommand import RunCommand

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)


class RunYaml(object):
    def __init__(self):
        self.config = ConfigUtil()
        pass

    def run_command(self, path):
        try:
            print("脚本位置:" + path)

            if not os.path.exists(path):
                raise Exception("can't find run script:" + path)

            extend = os.path.splitext(path)[1]
            if extend == ".yml":
                runCommand = RunCommand()
                return runCommand.run_yaml(path)
            else:
                raise Exception("当前只支持 yml 脚本")
        except Exception, e:
            print("Executor 上 job 运行失败:" + path)
            print(traceback.format_exc())
            return 1


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    path = sys.argv[1]
    if path is None or len(path) == 0:
        print("无法获取path")
        exit(-1)
    runCommand = RunYaml()
    code = runCommand.run_command(path)
    if code == 0:
        print "脚本运行完成"
        sys.exit(0)
    else:
        print "脚本运行失败"
        sys.exit(1)
