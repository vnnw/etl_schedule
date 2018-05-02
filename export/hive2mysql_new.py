# -*- coding:utf-8 -*-
# !/usr/bin/env python

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import subprocess
from bin.configutil import ConfigUtil

config_util = ConfigUtil()
DATA_SPLIT = "|"


def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-s", "--sql", dest="mysql_sql", action="store", type="string", help="mysql sql")
    parser.add_option("-i", "--hive", dest="hive_table", action="store", type="string", help="hive table")
    parser.add_option("-t", "--to", dest="mysql_db", action="store", help="mysql database.table")
    parser.add_option("-q", "--query", dest="hive_hql", action="store", help="hive query hql")
    parser.add_option("-c", "--columns", dest="mysql_columns", action="store",
                      help="mysql table columns split by comma")
    parser.add_option("-m", "--mode", dest="mode", action="store", help="overwrite or append")

    return parser


def run(options, args):
    shell = config_util.get("export.hive.mysql")
    command_list = list()
    command_list.append(shell)

    hive_table_arg = options.hive_table
    hive_database = hive_table_arg.split(".")[0]
    hive_table = hive_table_arg.split(".")[1]

    mysql_table_arg = options.mysql_db
    mysql_database = mysql_table_arg.split(".")[0]
    mysql_table = mysql_table_arg.split(".")[1]

    command_list.append(hive_table_arg)
    command_list.append(mysql_table)
    command_list.append(options.mysql_columns)
    command_list.append(options.hive_hql)
    command_list.append(options.mysql_sql)

    command_str = " ".join(command_list)

    print command_str
    sys.stdout.flush()

    child = subprocess.Popen(command_list,
                             stdout=None,
                             stderr=subprocess.STDOUT,
                             shell=False)
    code = child.wait()
    return code


def add_prefix(prefix='', source='', suffix=''):
    return prefix + source + suffix


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    fakeArgs = ["-q", "select * from t_hello", "-i", "dim_beeper.t_hello", "-c", "id,name", "-s",
                "select * from t_test", '-t', "db_stg.driver_quiz_score"]

    optParser = get_option_parser()

    options, args = optParser.parse_args(sys.argv[1:])

    #options, args = optParser.parse_args(fakeArgs)

    print options

    if options.hive_table is None:
        print("require hive table")
        optParser.print_help()
        sys.exit(1)
    if options.mysql_db is None:
        print("require mysql db")
        optParser.print_help()
        sys.exit(1)
    if options.mysql_columns is None:
        print("require mysql columns")
        optParser.print_help()
        sys.exit(1)
    code = run(options, args)
    sys.exit(code)
