#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import json
import subprocess
import pyhs2
from odps import ODPS
from bin.configutil import ConfigUtil

config_util = ConfigUtil()


def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-f", "--from", dest="odps_db", action="store", type="string", help="odps database.table")
    parser.add_option("-t", "--to", dest="hive_db", action="store", help="hive database.table")
    parser.add_option("-c", "--columns", dest="columns", action="store",
                      help="odps table columns split by comma")
    parser.add_option("-p", "--partition", dest="partition", action="store",
                      help="hive partition key=value")
    parser.add_option("-e", "--exclude-columns", dest="exclude_columns", action="store",
                      help="odps table exclude columns split by comma")

    return parser


'''
 获取 odps 连接
'''


def get_odps_connection(odps_db):
    access_id = config_util.get("odps_accessId")
    access_key = config_util.get("odps_accessKey")
    endpoint = config_util.get("odps_endpoint")
    odps = ODPS(access_id, access_key, odps_db, endpoint=endpoint)
    return odps


def read_base_json():
    json_str = """{
    "job": {
        "setting": {
            "speed": {
                "channel": 2
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "odpsreader"
                },
                "writer": {
                        "name": "hdfswriter"
                    }
            }
        ]
    }
    }""".strip()
    json_data = json.loads(json_str)
    return json_data


'''
获取ODPS表字段
'''


# List[(columnName,columnType,comment)]
def get_odps_table_columns(columns, exclude_columns, odps_db, odps_table):
    odps = get_odps_connection(odps_db)
    t = odps.get_table(odps_table)

    exclude_columns_set = set()
    if exclude_columns is not None:
        exclude_columns_array = exclude_columns.split(",")
        for exclude_column in exclude_columns_array:
            exclude_columns_set.add(exclude_column.strip())

    column_list = []
    table_columns = []
    table_column_dict = {}
    for column in t.schema.columns:
        column_name = str(column.name)
        column_type = str(column.type).lower()
        column_comment = str(column.comment)

        if column_name not in exclude_columns_set:
            table_columns.append(column_name)
            table_column_dict[column_name] = {"Field": column_name, "Type": column_type, "Comment": column_comment}

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

    return column_list


def get_hive_connection(db):
    host = config_util.get("hive.host")
    port = config_util.get("hive.port")
    connection = pyhs2.connect(host=host,
                               port=int(port),
                               authMechanism="PLAIN",
                               user="hadoop",
                               password="hadoop",
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
        create_sql_str += " STORED AS ORC"
        print(create_sql_str)
        cursor.execute(create_sql_str)
        if partition_key is not None:  # 添加新的分区
            cursor.execute(
                    "alter table " + hive_table + " add partition(" + partition_key + "='" + partition_value + "')")
    connection.close()


'''
 ODPS -> Hive 字段类型对应
'''


def change_type(ctype):
    ctype = ctype.lower()
    if ctype in ("varchar", "char"):
        ctype = "string"
    if ctype in "datetime":
        ctype = "timestamp"
    if ctype == "text":
        ctype = "string"
    if ctype == "time":
        ctype = "string"
    if ctype == "longtext":
        ctype = "string"
    if ctype == "long":
        ctype = "bigint"
    if ctype == "decimal":
        ctype = "double"
    return ctype


def parse_odps_db(odps_db_table):
    odps_db_table_array = odps_db_table.split(".")
    odps_db = odps_db_table_array[0]
    odps_table = odps_db_table_array[1]
    return (odps_db, odps_table)


def parse_hive_db(hive_db_table):
    hive_db_table_array = hive_db_table.split(".")
    hive_db = hive_db_table_array[0]
    hive_table = hive_db_table_array[1]
    return (hive_db, hive_table)


def build_json_file(options, args):
    columns = options.columns  # 包含
    exclude_columns = options.exclude_columns  # 排除

    json_data = read_base_json()

    partition = options.partition
    if partition is not None:
        partition = partition.strip()

    (hive_db, hive_table) = parse_hive_db(options.hive_db)

    (odps_db, odps_table) = parse_odps_db(options.odps_db)

    odps_columns = get_odps_table_columns(columns, exclude_columns, odps_db, odps_table)

    format_column_list = []  # 用来创建hive表的字段
    column_name_type_list = []  # hdfs 写入字段
    for column in odps_columns:
        (name, ctype, comment) = column
        format_column_list.append((name, ctype, comment))
        column_name_type_list.append({"name": name, "type": change_type(ctype)})

    create_hive_table(hive_db, hive_table, format_column_list, partition)

    access_id = config_util.get("odps_accessId")
    access_key = config_util.get("odps_accessKey")
    odps_dict = {
        "accessId": access_id,
        "accessKey": access_key,
        "project": odps_db,
        "table": odps_table,
        "partition": [partition],
        "column": [],
        "splitMode": "record",
        "odpsServer": "http://service.odps.aliyun.com/api"
    }

    json_data["job"]["content"][0]["reader"]["parameter"] = odps_dict

    hive_table_path = "/user/hive/warehouse/" + hive_db + ".db/" + hive_table
    if partition is not None:
        hive_table_path = hive_table_path + "/" + partition

    writer_parameter_dict = {
        "column": column_name_type_list,
        "defaultFS": config_util.get("hdfs.uri"),
        "fieldDelimiter": '\u0001',
        "fileName": odps_table,
        "fileType": "orc",
        "path": hive_table_path,
        "writeMode": "append"
    }

    json_data["job"]["content"][0]["writer"]["parameter"] = writer_parameter_dict

    datax_json_base_path = config_util.get("datax.json.path")
    if not os.path.exists(datax_json_base_path):
        os.makedirs(datax_json_base_path)
    datax_json_path = datax_json_base_path + "/" + odps_db + "_" + odps_table + "_" + hive_db + "_" + hive_table + ".json"
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

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # fakeArgs = ["-f","test.t_test",'-t',"db_stg.driver_quiz_score"]

    optParser = get_option_parser()

    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.odps_db is None or options.hive_db is None:
        optParser.print_help()
        sys.exit(1)
    else:
        if options.odps_db is None:
            print("require mysql database.table")
            sys.exit(1)
        if options.hive_db is None:
            print("require odps database.table")
            sys.exit(1)
        jsonFile = build_json_file(options, args)
        code = run_datax(jsonFile)
        if code != 0:
            print("datax load failed")
            sys.exit(1)
        else:
            sys.exit(0)
