#!/usr/bin/python
# -*- coding:utf-8 -*-


from logutil import Logger
from configutil import ConfigUtil

import time
import MySQLdb

from DBUtils.PooledDB import PooledDB


class DBUtil(object):
    def __init__(self):
        self.config = ConfigUtil()
        self.host = self.config.get("mysql.host")
        self.port = self.config.get("mysql.port")
        self.db = self.config.get("mysql.db")
        self.username = self.config.get("mysql.username")
        self.password = self.config.get("mysql.password")
        self.logger = Logger("db").getlog()

        self.pool = PooledDB(creator=MySQLdb,
                             mincached=1,
                             maxcached=20,
                             host=self.host,
                             port=int(self.port),
                             user=self.username,
                             passwd=self.password,
                             db=self.db,
                             use_unicode=True,
                             charset="utf8")

    def get_connection(self):
        try:
            mysql_con = self.pool.connection()
            return mysql_con
        except Exception, e:
            self.logger.error(e)
            for i in range(3):
                try:
                    time.sleep(5)
                    mysql_con = self.pool.connection()
                    return mysql_con
                except Exception, e:
                    self.logger.error(e)
                    self.logger.error("数据库连接异常执行" + str(i + 1) + "次连接")
            return None

    def close_connection(self, connection):
        try:
            connection.close()
        except Exception, e:
            self.logger.error(e)
            self.logger.error("mysql connection 关闭异常")
