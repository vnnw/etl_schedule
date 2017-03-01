#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import traceback
import sys
from configutil import ConfigUtil
from runcommand import RunCommand
from optparse import OptionParser

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)
from dateutil import DateUtil


class RunYaml(object):
    def __init__(self):
        self.config = ConfigUtil()
        pass

    def option_parser(self):
        usage = "usage: %prog [options] arg1 arg2"

        parser = OptionParser(usage=usage)

        parser.add_option("-s", "--start", dest="start", action="store", type="string",
                          help="start date yyyy-MM-dd")
        parser.add_option("-e", "--end", dest="end", action="store", type="string",
                          help="end date yyyy-MM-dd")
        parser.add_option("-p", "--path", dest="path", action="store", type="string",
                          help="yaml file path")

        return parser

    def run_command(self, path, p_day):
        try:
            print("脚本位置:" + path)

            if not os.path.exists(path):
                raise Exception("can't find run script:" + path)

            extend = os.path.splitext(path)[1]
            if extend == ".yml":
                run_command = RunCommand()
                code = run_command.run_yaml(path, p_day)
                return code
            else:
                raise Exception("当前只支持 yml 脚本")
        except Exception, e:
            print("Executor 上 job 运行失败:" + path)
            print(traceback.format_exc())
            return 1


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    run_yaml = RunYaml()
    optParser = run_yaml.option_parser()
    options, args = optParser.parse_args(sys.argv[1:])

    if options.path is None:
        print("require yaml file")
        optParser.print_help()
        sys.exit(-1)

    start = options.start
    end = options.end
    if start is None:
        start = DateUtil.parse_date(DateUtil.get_now_fmt())
    else:
        start = DateUtil.parse_date(start)
    if end is None:
        end = DateUtil.parse_date(DateUtil.get_now_fmt())
    else:
        end = DateUtil.parse_date(end)

    run_code = []
    for p_day in DateUtil.get_list_day(start, end):
        print "运行时设置的日期:", p_day
        code = run_yaml.run_command(options.path, p_day)
        run_code.append(str(code))
    code_str = ",".join(run_code)
    print "运行结果:", code_str
