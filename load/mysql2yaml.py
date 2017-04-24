# -*- coding:utf-8 -*-

import os
import sys
import MySQLdb
import yaml

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bin.configutil import ConfigUtil

config_util = ConfigUtil()


def get_mysql_config(mysql_db):
    prefix = "mysql" + "." + mysql_db
    db_config = {}
    db_config["username"] = config_util.get(prefix + ".username")
    db_config["password"] = config_util.get(prefix + ".password")
    db_config["host"] = config_util.get(prefix + ".host")
    db_config["port"] = config_util.get(prefix + ".port")
    return db_config


'''
 获取 mysql 连接
'''


def get_mysql_connection(mysql_db):
    mysql_config = get_mysql_config(mysql_db)
    host = mysql_config["host"]
    username = mysql_config["username"]
    password = mysql_config["password"]
    port = int(mysql_config["port"])
    connection = MySQLdb.connect(host, username, password, mysql_db, port, use_unicode=True, charset='utf8')
    return connection


yaml_str = """
steps:
  - type : export
    ops:
      - mysql2hive:
          mysql_db: beeper_tf.trans_task
          hive_db: ods.ods_trans_task
          include_columns:
          exclude_columns:
"""


def yaml2dict(yaml_file):
    return yaml.load(open(yaml_file, 'r'))


def run(mysql_db):
    connection = get_mysql_connection(mysql_db)
    tables = get_tables(connection)
    for table in tables:
        columns = get_table_columns(connection, table)
        print "-" * 20
        print table
        include_column = []
        create_column = []
        table_name = "ods_mysql.ods_" + db + "__" + table
        for column in columns:
            (name, typestring, comment) = column
            ctype = typestring.split("(")[0]
            include_column.append(name)
            create_column.append(
                    "    `" + str(name) + "` " + str(change_type(ctype)).strip() + " comment \"" + str(comment).strip() + "\"")
        create_column_str = ",\n".join(create_column)
        create_sql_str = "create external table if not exists " + table_name + " ( \n" + create_column_str + " )"
        create_sql_str += "\ncomment \"xxxx\""
        create_sql_str += "\npartitioned by(p_day string)"
        create_sql_str += "\nstored as orc ;"
        print ",".join(include_column)
        print(create_sql_str)


def get_table_columns(connection, table):
    sql = "show full columns from " + table
    rows = run_sql_dict(connection, sql, ())
    columns = list()
    for row in rows:
        columns.append((row.get('Field'), row.get('Type'), row.get('Comment')))
    return columns


def run_sql_dict(connection, sql, params):
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def change_type(ctype):
    ctype = ctype.lower()
    if ctype in ("varchar", "char"):
        ctype = "string"
    if ctype in ("datetime",):
        ctype = "timestamp"
    if ctype == "text":
        ctype = "string"
    if ctype == "time":
        ctype = "string"
    if ctype == "longtext":
        ctype = "string"
    if ctype in ("long", "mediumint", "tinyint"):
        ctype = "bigint"
    if ctype in ("smallint"):
        ctype = "int"
    if ctype == "decimal":
        ctype = "double"
    if ctype == "date":  # 转换类型
        ctype = "string"
    return ctype


def get_tables(connection):
    sql = "show tables"
    rows = run_sql_dict(connection, sql, ())
    tables = set()
    for row in rows:
        tables.add(row.values()[0])
    return tables


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    db = "beeper_trans_task"
    run(db)
