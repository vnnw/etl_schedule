#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymongo
import pyhs2
import MySQLdb
from odps import ODPS
from odps import options

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
        password = config_util.getOrElse("hive.password", '')
        connection = pyhs2.connect(host=host,
                                   port=int(port),
                                   authMechanism="PLAIN",
                                   user=username,
                                   password=password,
                                   database=hive_db,
                                   timeout=60000)
        return connection

    @staticmethod
    def get_mysql_config(config_util,mysql_db):
        prefix = "mysql" + "." + mysql_db
        db_config = {}
        db_config["username"] = config_util.get(prefix + ".username")
        db_config["password"] = config_util.get(prefix + ".password")
        db_config["host"] = config_util.get(prefix + ".host")
        db_config["port"] = config_util.get(prefix + ".port")
        return db_config

    @staticmethod
    def get_mysql_connection(config_util,mysql_db):
        mysql_config = Connection.get_mysql_config(config_util,mysql_db)
        host = mysql_config["host"]
        username = mysql_config["username"]
        password = mysql_config["password"]
        port = int(mysql_config["port"])
        print "创建 MySQL 连接 host:{host}, username:{username}, password:{password}, db:{mysql_db}"\
            .format(host=host, username=username, password=password, mysql_db=mysql_db)
        connection = MySQLdb.connect(host, username, password, mysql_db, port, use_unicode=True, charset='utf8')
        return connection

    @staticmethod
    def get_mysql_monitor_dict(config_util):
        db = config_util.get("mysql.db")
        db_config = Connection.get_mysql_config(config_util,db)
        url = "jdbc:mysql://" + db_config["host"] + ":" + str(db_config["port"]) + "/" + db
        static_dict = {
            "url": url,
            "username": db_config["username"],
            "password": db_config["password"],
            "table": "t_datax_monitor"
        }
        return static_dict

    @staticmethod
    def get_odps_connection(config_util,odps_db):
        access_id = config_util.get("odps_accessId")
        access_key = config_util.get("odps_accessKey")
        endpoint = config_util.get("odps_endpoint")
        odps = ODPS(access_id, access_key, odps_db, endpoint=endpoint)
        return odps
