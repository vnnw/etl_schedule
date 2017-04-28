#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymongo
import pyhs2
import MySQLdb


class Connection:

    @staticmethod
    def get_mongo_connection(config_util, mongo_db):
        host = config_util.get("mongo." + mongo_db + ".host")
        port = config_util.get("mongo." + mongo_db + ".port")
        connection = pymongo.MongoClient(host, int(port))
        return connection

    @staticmethod
    def get_hive_connection(config_util, hive_db):
        host = config_util.get("hive.host")
        port = config_util.get("hive.port")
        username = config_util.get("hive.username")
        password = config_util.get("hive.password")
        connection = pyhs2.connect(host=host,
                                   port=int(port),
                                   authMechanism="PLAIN",
                                   user=username,
                                   password=password,
                                   database=hive_db)
        return connection

    @staticmethod
    def get_mysql_config(config_util,mysql_db):
        prefix = "mysql" + "." + mysql_db
        db_config = {}
        db_config["username"] = config_util.get(prefix + ".username")
        db_config["password"] = config_util.get(prefix + ".password")
        db_config["host"] = config_util.get(prefix + ".host")
        db_config["port"] = config_util.get(prefix + ".port")
        print "MySQL 配置信息:" + str(mysql_db) + str(db_config)
        return db_config

    @staticmethod
    def get_mysql_connection(config_util,mysql_db):
        mysql_config = Connection.get_mysql_config(config_util,mysql_db)
        host = mysql_config["host"]
        username = mysql_config["username"]
        password = mysql_config["password"]
        port = int(mysql_config["port"])
        connection = MySQLdb.connect(host, username, password, mysql_db, port, use_unicode=True, charset='utf8')
        return connection

