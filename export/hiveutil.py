#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import random
import traceback
from bin.configutil import ConfigUtil
from connection import Connection


class HiveUtil:
    def __init__(self):
        self.config = ConfigUtil()
        self.sqls = []
        self.vars = []

    def add_sql(self, sql):
        self.sqls.append(sql)

    def add_sqls(self, sqls):
        for sql in sqls:
            self.sqls.append(sql)

    def add_var(self, var):
        self.vars.append(var)

    def add_vars(self, vars):
        for var in vars:
            self.add_var(var)

    def add_sql_paths(self, base_path, sql_paths):
        for sql_path in sql_paths:
            self.add_sql_path(base_path, sql_path)

    def add_sql_path(self, base_path, sql_path):
        if base_path and len(base_path) > 0:
            sql_path = base_path + "/" + sql_path
        sql_handler = open(sql_path, 'r')
        sqls = ""
        for line in sql_handler.readlines():
            sqls += line
        self.add_sqls((sqls,))
        sql_handler.flush()
        sql_handler.close()

    def run_sql_count(self, sql):
        try:
            connection = Connection.get_hive_connection(self.config)
            cursor = connection.cursor()
            print "start run hql:" + str(sql)
            sql = sql.replace(";", "")
            cursor.execute(sql)
            count = 0
            for i in cursor.fetch():
                count = i[0]
            return count
        except Exception, e:
            traceback.format_exc()
            return 0
        finally:
            connection.close()

    def run_sql(self, sql):
        try:
            connection = Connection.get_hive_connection(self.config)
            cursor = connection.cursor()
            print "start run hql:" + str(sql)
            sql = sql.replace(";", "")
            cursor.execute(sql)
            return 0
        except Exception, e:
            traceback.format_exc()
            return -1
        finally:
            connection.close()

    def run_sql_connection(self):
        try:
            connection = Connection.get_hive_connection(self.config)
            cursor = connection.cursor()
            for sql in self.sqls:
                print "start run hql:" + str(sql)
                sql = sql.replace(";", "")
                cursor.execute(sql)
            return 0
        except Exception, e:
            traceback.format_exc()
            return -1
        finally:
            connection.close()

    def run_sql_hive(self, sql_path):
        print("使用Hive 运行 SQL 文件")
        sys.stdout.flush()
        hive_home = self.config.get("hive.path")
        if hive_home is None:
            raise Exception("HIVE_HOME 环境变量没有设置")
        command_bin = hive_home + "/bin/hive"
        code = os.system(command_bin + " -f " + sql_path)
        if code != 0:
            print "run hql by hive error exit"
            return 1
        return code

    def run_sql_spark(self, sql_path):
        print("使用Spark 运行 SQL 文件")
        spark_home = self.config.get("spark.path")
        if spark_home is None:
            raise Exception("SPARK_HOME 环境变量没有设置")
        command_list = list()
        spark_sql_opt = self.config.get("spark.sql.opt")
        command_list.append(spark_sql_opt)
        command_list.append(sql_path)
        command_bin = " ".join(command_list)
        print("command:" + str(command_bin))
        sys.stdout.flush()
        code = os.system(command_bin)
        if code != 0:
            print "run hql by spark error exit"
            return 1

        return code

    def run_sql_client(self):
        tmp_dir = self.config.get("tmp.path") + "/hqls"
        try:
            for sql in self.sqls:
                start = datetime.datetime.now()
                print "start run hql:" + str(sql)
                mills = datetime.datetime.now().microsecond
                rand = random.randint(1, 100)
                if not os.path.exists(tmp_dir):
                    os.makedirs(tmp_dir)
                tmp_path = tmp_dir + "/" + str(mills) + "-" + str(rand) + ".hql"
                print "tmp path " + str(tmp_path)
                tmp_file = open(tmp_path, "w")
                for var in self.vars:
                    tmp_file.write(var + '\n')
                tmp_file.flush()
                tmp_file.write(sql)
                tmp_file.flush()
                tmp_file.close()
                sys.stdout.flush()
                if self.config.getBooleanOrElse("spark.sql.run", False):
                    code = self.run_sql_spark(tmp_path)
                    if code != 0:  # spark run sql error
                        code = self.run_sql_hive(tmp_path)
                        if code != 0:  # hive run sql error
                            return code
                else:
                    code = self.run_sql_hive(tmp_path)
                    if code != 0:  # hive run sql error
                        return code
                end = datetime.datetime.now()
                duration = (end-start).seconds
                print "sql run spent seconds:" + str(duration)
                sys.stdout.flush()
            return 0  # all sql run success
        except Exception, e:
            print(e)
            sys.stdout.flush()
            return 1
