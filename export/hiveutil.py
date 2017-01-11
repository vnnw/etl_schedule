#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import random
import pyhs2
from bin.configutil import ConfigUtil

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

    def add_sql_paths(self,base_path,sql_paths):
        for sql_path in sql_paths:
            self.add_sql_path(base_path, sql_path)

    def add_sql_path(self,base_path,sql_path):
        if base_path and len(base_path) > 0:
            sql_path = base_path + "/" + sql_path
        sql_handler = open(sql_path,'r')
        sqls = ""
        for line in sql_handler.readlines():
            sqls += line
        self.add_sqls(sqls.split(";"))
        sql_handler.flush()
        sql_handler.close()

    def get_connection(self):
        host = self.config.get("hive.host")
        port = self.config.get("hive.port")
        connection = pyhs2.connect(host=host,
                                   port=int(port),
                                   authMechanism="PLAIN",
                                   user="hadoop",
                                   password="hadoop")
        return connection

    def run_sql_count(self, sql):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            print "start run hql:" + str(sql)
            sql = sql.replace(";", "")
            cursor.execute(sql)
            count = 0
            for i in cursor.fetch():
                count = i[0]
            connection.close()
            return count
        except Exception, e:
            print(e)
            connection.close()
            return 0

    def run_sql(self, sql):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            print "start run hql:" + str(sql)
            sql = sql.replace(";", "")
            cursor.execute(sql)
            connection.close()
            return 0
        except Exception, e:
            print(e)
            connection.close()
            return -1

    def run_sql_connection(self):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            for sql in self.sqls:
                print "start run hql:" + str(sql)
                sql = sql.replace(";", "")
                cursor.execute(sql)
            connection.close()
            return 0
        except Exception, e:
            print(e)
            connection.close()
            return -1

    def run_sql_client(self):
        hiveHome = self.config.get("hive.path")
        if hiveHome is None:
            raise Exception("HIVE_HOME 环境变量没有设置")
        command_bin = hiveHome + "/bin/hive"
        tmpdir = self.config.get("tmp.path") + "/hqls"
        try:
            for sql in self.sqls:
                print "start run hql:" + str(sql)
                sys.stdout.flush()
                mills = datetime.datetime.now().microsecond
                rand = random.randint(1, 100)
                if not os.path.exists(tmpdir):
                    os.makedirs(tmpdir)
                tmppath = tmpdir + "/" + str(mills) + "-" + str(rand) + ".hql"
                print "tmp path " + str(tmppath)
                sys.stdout.flush()
                tmp_file = open(tmppath, "w")
                for var in self.vars:
                    tmp_file.write(var + '\n')
                tmp_file.flush()
                tmp_file.write(sql)
                tmp_file.flush()
                tmp_file.close()
                code = os.system(command_bin + " -f " + tmppath)
                #os.remove(tmppath)
                if code != 0:
                    print "run hql error exit"
                    sys.stdout.flush()
                    return -1
            return 0
        except Exception, e:
            print(e)
            sys.stdout.flush()
            return -1

    def check_run_code(self, code=-1):
        if code == 0:
            sys.exit(0)
        else:
            sys.exit(-1)
