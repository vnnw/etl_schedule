#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import json
import MySQLdb
import subprocess
import pyhs2
from bin.configutil import ConfigUtil

config_util = ConfigUtil()


def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-f", "--from", dest="mysql_db", action="store", type="string", help="mysql database.table")
    parser.add_option("-t", "--to", dest="hive_db", action="store", help="hive database.table")
    parser.add_option("-c", "--columns", dest="columns", action="store",
                      help="mysql table columns split by comma")
    parser.add_option("-w", "--where", dest="where", action="store", help="mysql query condition")
    parser.add_option("-p", "--partition", dest="partition", action="store",
                      help="hive partition key=value")
    parser.add_option("-e", "--exclude-columns", dest="exclude_columns", action="store",
                      help="mysql table exclude columns split by comma")

    return parser


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


def read_base_json():
    json_str = """{
        "job": {
            "content": [
                {
                    "reader": {
                        "name": "mysqlreader"
                    },
                    "writer": {
                        "name": "hdfswriter"
                    }
                }
            ],
            "setting": {
                "speed": {
                    "channel": "2"
                }
            }
        }
    } """.strip()
    json_data = json.loads(json_str)
    return json_data


'''
获取MySQL表字段
'''


# List[(columnName,columnType,comment)]
def get_mysql_table_columns(columns, exclude_columns, mysql_db, mysql_table):
    connection = get_mysql_connection(mysql_db)
    command = """show full columns from """ + mysql_table
    cursor = connection.cursor()
    cursor.execute(command)
    result = cursor.fetchall()
    exclude_columns_set = set()
    if exclude_columns is not None:
        exclude_columns_array = exclude_columns.split(",")
        for exclude_column in exclude_columns_array:
            exclude_columns_set.add(exclude_column.strip())

    column_list = []
    table_columns = []
    table_column_dict = {}
    for r in result:
        (field, ctype, c2, c3, c4, c5, c6, c7, comment) = r
        if field not in exclude_columns_set:
            table_columns.append(field)
            table_column_dict[field] = {"Field": field, "Type": ctype, "Comment": comment}
    if columns is None:
        for column_name in table_columns:
            column = table_column_dict[column_name]
            column_list.append((column['Field'], column['Type'], column['Comment']))
    else:
        column_array = columns.split(",")
        for column_name in column_array:
            column_name = column_name.strip()
            if column_name not in table_columns:
                print("column:" + column_name + "not in table:" + table_columns)
                sys.exit(-1)
            else:
                column = table_column_dict[column_name]
                column_list.append((column['Field'], column['Type'], column['Comment']))
    connection.close()
    return column_list


def get_hive_connection(db):
    host = config_util.get("hive.host")
    port = config_util.get("hive.port")
    username = config_util.get("hive.username")
    password = config_util.get("hive.password")
    connection = pyhs2.connect(host=host,
                               port=int(port),
                               authMechanism="PLAIN",
                               user=username,
                               password=password,
                               database=db)
    return connection


'''
创建hive 表
'''


def create_hive_table(hive_db, hive_table, column_list, partition):
    connection = get_hive_connection(hive_db)
    cursor = connection.cursor()
    cursor.execute("use " + hive_db)
    cursor.execute("show tables")
    result = cursor.fetchall()
    tables = set()
    for table in result:
        tables.add(table[0])

    if partition is None and hive_table in tables:  # 如果有partition 不能删除表,应该增加partition
        cursor.execute("drop table " + hive_table)
        tables.remove(hive_table)

    partition_key = None
    partition_value = None
    if partition is not None:
        partition_array = partition.split("=")
        partition_key = partition_array[0].strip()
        partition_value = partition_array[1].strip()

    if hive_table in tables:
        if partition_key is not None:  # 先删除再重建防止partition里面有数据
            cursor.execute(
                    "alter table " + hive_table + " drop partition(" + partition_key + "='" + partition_value + "')")
            cursor.execute(
                "alter table " + hive_table + " add partition(" + partition_key + "='" + partition_value + "')")
    else:
        create_column = []
        for column in column_list:
            (name, typestring, comment) = column
            create_column.append(
                    "`" + str(name) + "` " + str(typestring).strip() + " comment \"" + str(comment).strip() + "\"")
        create_column_str = " , ".join(create_column)
        create_sql_str = "create table " + hive_table + " ( " + create_column_str + " )"
        if partition_key is not None:
            create_sql_str += " partitioned by(" + partition_key + " string)"
        #create_sql_str += " comment xxxx"
        create_sql_str += " STORED AS ORC"
        print(create_sql_str)
        cursor.execute(create_sql_str)
        if partition_key is not None:  # 添加新的分区
            cursor.execute(
                "alter table " + hive_table + " add partition(" + partition_key + "='" + partition_value + "')")
    connection.close()


'''
 MySQL -> Hive 字段类型对应
'''


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
    if ctype == "decimal":
        ctype = "double"
    if ctype == "date": # 转换类型
        ctype = "string"
    return ctype


def parse_mysql_db(mysql_db_table):
    mysql_db_table_array = mysql_db_table.split(".")
    mysql_db = mysql_db_table_array[0]
    mysql_table = mysql_db_table_array[1]
    return (mysql_db, mysql_table)


def parse_hive_db(hive_db_table):
    hive_db_table_array = hive_db_table.split(".")
    hive_db = hive_db_table_array[0]
    hive_table = hive_db_table_array[1]
    return (hive_db, hive_table)


'''
处理 MySQL 的列,拼接成SQL
'''


def process_mysql(options):
    columns = options.columns  # 包含
    exclude_columns = options.exclude_columns  # 排除

    where = options.where
    if where is not None:  # where 条件
        where = where.strip()

    (mysql_db, mysql_table) = parse_mysql_db(options.mysql_db)
    column_list = get_mysql_table_columns(columns, exclude_columns, mysql_db, mysql_table)
    column_name_list = []
    column_name_type_list = []
    format_column_list = []
    for column in column_list:
        (name, ctype, comment) = column
        column_name_list.append("`" + name + "`")
        if "(" in ctype:
            ctype = ctype[:ctype.index("(")]
        ctype = change_type(ctype)
        column_name_type_list.append({"name": name, "type": ctype})  # datax json 的格式
        format_column_list.append((name, ctype, comment))  # 用来创建hive表的字段
    query_sql = "select " + ", ".join(column_name_list) + " from  " + mysql_table + " where 1=1 "
    if where is not None and len(where) > 0:
        query_sql += " and " + where
    print "query_sql:", str(query_sql)
    return (format_column_list, column_name_type_list, query_sql)


def build_json_file(options, args):
    json_data = read_base_json()

    partition = options.partition
    if partition is not None:
        partition = partition.strip()

    (hive_db, hive_table) = parse_hive_db(options.hive_db)

    (mysql_db, mysql_table) = parse_mysql_db(options.mysql_db)

    mysql_config_dict = get_mysql_config(mysql_db)

    (format_column_list, column_name_type_list, query_sql) = process_mysql(options)

    create_hive_table(hive_db, hive_table, format_column_list, partition)

    readerParameterDict = {
        "connection": [{
            "querySql": [query_sql],
            "jdbcUrl": [
                "jdbc:mysql://" + mysql_config_dict["host"] + ":" + str(mysql_config_dict["port"]) + "/" + mysql_db]
        }],
        "username": mysql_config_dict["username"],
        "password": mysql_config_dict["password"]
    }
    json_data["job"]["content"][0]["reader"]["parameter"] = readerParameterDict

    hive_table_path = "/user/hive/warehouse/" + hive_db + ".db/" + hive_table
    if partition is not None:
        hive_table_path = hive_table_path + "/" + partition

    writer_parameter_dict = {
        "column": column_name_type_list,
        "defaultFS": config_util.get("hdfs.uri"),
        "fieldDelimiter": '\u0001',
        "fileName": mysql_table,
        "fileType": "orc",
        "path": hive_table_path,
        "writeMode": "append"
    }

    json_data["job"]["content"][0]["writer"]["parameter"] = writer_parameter_dict

    datax_json_base_path = config_util.get("datax.json.path")
    if not os.path.exists(datax_json_base_path):
        os.makedirs(datax_json_base_path)
    datax_json_path = datax_json_base_path + "/" + mysql_db + "_" + mysql_table + "_" + hive_db + "_" + hive_table + ".json"
    datax_json_file_handler = open(datax_json_path, "w")
    json_str = json.dumps(json_data, indent=4, sort_keys=False, ensure_ascii=False)
    json_str = json_str.replace("\\\\", "\\")
    # print jsonStr
    datax_json_file_handler.write(json_str)
    datax_json_file_handler.close()
    return datax_json_path


def run_datax(json_file):
    datax_command = config_util.get("datax.path") + " " + json_file
    print datax_command
    child_process = subprocess.Popen("python " + datax_command, shell=True)
    (stdout, stderr) = child_process.communicate()
    return child_process.returncode


'''
检查导入的数据是否完整,总的记录条数差距 %10
'''


def run_check(options):
    (format_column_list, column_name_type_list, query_sql) = process_mysql(options)
    count_mysql = "select count(*) as mcount from (" + query_sql + ") t1"
    print "count_mysql_sql:" + count_mysql
    (mysql_db, mysql_table) = parse_mysql_db(options.mysql_db)
    mysql_connection = get_mysql_connection(mysql_db)
    mysql_cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)
    mysql_cursor.execute(count_mysql)
    r1 = mysql_cursor.fetchone()
    mysql_count = r1['mcount']
    mysql_connection.close()
    (hive_db, hive_table) = parse_hive_db(options.hive_db)
    hive_connection = get_hive_connection(hive_db)
    count_hive = "select count(*) as hcount from " + options.hive_db
    partition = options.partition
    if partition is not None and len(partition) > 0:
         partition_key = None
         partition_value = None
         if partition is not None:
            partition_array = partition.split("=")
            partition_key = partition_array[0].strip()
            partition_value = partition_array[1].strip()
         count_hive = count_hive + " where " + partition_key + "'" + partition_value + "'"
    print "count_hive_sql:" + count_hive
    hive_cursor = hive_connection.cursor()
    hive_cursor.execute(count_hive)
    r2 = hive_cursor.fetchone()
    hive_count = r2[0]
    hive_connection.close()
    diff_count = abs(hive_count - mysql_count)
    print "mysql_count:" + str(mysql_count)
    print "hive_count:" + str(hive_count)
    if diff_count == 0:
        return 0
    threshold = diff_count * 100 / hive_count
    if threshold > 10:
        print "导出的数据总数有差异 mongodb:" + options.mysql_db + ":" + str(mysql_count) \
                + " hive:" + options.hive_db + ":" + str(hive_count) + " 差值:" + str(diff_count) \
                + " threshold:" + str(threshold)
        return 1
    else:
        print "导出的数据总数 mongodb:" + options.mysql_db + ":" + str(mysql_count) \
                + " hive:" + options.hive_db + ":" + str(hive_count) + " 差值:" + str(diff_count) \
                + " threshold:" + str(threshold)
        return 0


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # fakeArgs = ["-f","test.t_test",'-t',"db_stg.driver_quiz_score"]

    optParser = get_option_parser()

    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.mysql_db is None or options.hive_db is None:
        optParser.print_help()
        sys.exit(1)
    else:
        if options.mysql_db is None:
            print("require mysql database.table")
            sys.exit(1)
        if options.hive_db is None:
            print("require hive database.table")
            sys.exit(1)
        jsonFile = build_json_file(options, args)
        code = run_datax(jsonFile)
        if code != 0:
            print("datax load failed")
            sys.exit(1)
        else:
            complete = run_check(options)
            sys.exit(complete)
