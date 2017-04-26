#!/usr/bin/env python
# -*- coding:utf-8 -*-


import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import json
import subprocess
import yaml
import pymongo
import pyhs2
import datetime
from hivetype import HiveType
from bin.configutil import ConfigUtil
from connection import Connection

config_util = ConfigUtil()


def get_today(tz=None):
    today = datetime.datetime.now()
    today = today.replace(hour=0, minute=0, second=0)
    if tz == "utc":
        today = today - datetime.timedelta(hours=8)
    return today


def get_yesterday(tz=None):
    today = datetime.datetime.now()
    today = today.replace(hour=0, minute=0, second=0)
    if tz == "utc":
        today = today - datetime.timedelta(hours=8)
    return today - datetime.timedelta(days=1)


def get_interval_day(interval, tz=None):
    today = datetime.datetime.now()
    today = today.replace(hour=0, minute=0, second=0)
    if tz == "utc":
        today = today - datetime.timedelta(hours=8)
    return today - datetime.timedelta(days=interval)


def replace_query_utc(obj):
    for param_key, param_value in obj.items():
        if param_value == '${yesterday}':
            obj[param_key] = get_yesterday("utc")
        if param_value == '${today}':
            obj[param_key] = get_today("utc")
    return obj


def json2dict_utc(json_str):
    json_dict = json.loads(json_str, object_hook=replace_query_utc)
    return json_dict


def replace_query(obj):
    for param_key, param_value in obj.items():
        if param_value == '${yesterday}':
            obj[param_key] = {"$date": int(get_yesterday().strftime("%s")) * 1000}
        if param_value == '${today}':
            obj[param_key] = {"$date": int(get_today().strftime("%s")) * 1000}
    return obj


def json2dict(json_str):
    json_dict = json.loads(json_str, object_hook=replace_query)
    return json_dict


def get_datetime_from_timestamp(timestamp):
    return datetime.datetime.fromtimestamp(timestamp / 1000)


def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage=usage)
    parser.add_option("-f", "--file", dest="yaml_file", action="store", type="string", help="require yaml path")
    parser.add_option("-m", "--from", dest="mongo_db", action="store", type="string", help="mongo database.collection")
    parser.add_option("-t", "--to", dest="hive_db", action="store", help="hive database.table")
    parser.add_option("-p", "--partition", dest="partition", action="store",
                      help="hive partition key=value")
    return parser


def yaml2dict(yaml_file):
    return yaml.load(open(yaml_file, 'r'))


def dict2json(dict):
    jsonStr = json.dumps(dict, indent=4, sort_keys=False, ensure_ascii=False)
    jsonStr = jsonStr.replace("\\\\", "\\")
    return jsonStr


def remove_dir(dir_name):
    print "删除 HDFS 目录:" + str(dir_name)
    os.system("hadoop fs -rmr " + dir_name)


def create_dir(dir_name):
    print "创建 HDFS 目录:" + str(dir_name)
    os.system("hadoop fs -mkdir -p " + dir_name)


def parse_hive_db(hive_db_table):
    hive_db_table_array = hive_db_table.split(".")
    hive_db = hive_db_table_array[0]
    hive_table = hive_db_table_array[1]
    return (hive_db, hive_table)


def parse_mongo(mongo_db_collection):
    mongo_db_collection_array = mongo_db_collection.split(".")
    mongo_db = mongo_db_collection_array[0]
    collection = mongo_db_collection_array[1]
    return (mongo_db, collection)


def read_yaml_schema(options):
    yaml_path = config_util.get("yaml.path")
    yaml_file = options.yaml_file
    yaml_dict = yaml2dict(yaml_path + "/" + yaml_file)
    return yaml_dict


def build_json_file(options, args):
    (hive_db, hive_table) = parse_hive_db(options.hive_db)

    (mongo_db, collection) = parse_mongo(options.mongo_db)

    partition = options.partition

    partition_key = None
    partition_value = None
    if partition is not None:
        connection = Connection.get_hive_connection(config_util,hive_db)
        cursor = connection.cursor()
        cursor.execute("use " + hive_db)
        partition_array = partition.split("=")
        partition_key = partition_array[0].strip()
        partition_value = partition_array[1].strip()
        if partition_key is not None:  # 先删除再重建防止partition里面有数据
            cursor.execute(
                    "alter table " + hive_table + " drop partition(" + partition_key + "='" + partition_value + "')")
            cursor.execute(
                    "alter table " + hive_table + " add partition(" + partition_key + "='" + partition_value + "')")
        connection.close()

    default_fs = config_util.get("hdfs.uri")
    hive_path = config_util.get("hive.warehouse") + "/" + hive_db + ".db/" + hive_table

    if partition is not None:
        hive_path = hive_path + "/" + partition_value

    remove_dir(default_fs + hive_path)
    create_dir(default_fs + hive_path)

    yaml_dict = read_yaml_schema(options)
    # 替换 query 里面的参数
    parameter_dict = yaml_dict["job"]["content"][0]["reader"]["parameter"]
    query = None
    if parameter_dict and parameter_dict.has_key("query"):
        query = parameter_dict["query"]
        query = json2dict(query)

    if query:
        parameter_dict["query"] = query
    parameter_dict["dbName"] = mongo_db
    parameter_dict["collectionName"] = collection
    columns = parameter_dict["column"]

    hive_columns = []
    for column in columns:
        hive_columns.append({"name": column["name"], "type": HiveType.change_type(column["type"])})

    mongo_host = config_util.get("mongo." + mongo_db + ".host")
    mongo_port = config_util.get("mongo." + mongo_db + ".port")
    address = [mongo_host + ":" + mongo_port]
    parameter_dict["address"] = address

    yaml_dict["job"]["content"][0]["writer"] = {}  # set {}

    writer_dict = {
        "name": "hdfswriter",
        "parameter": {
            "defaultFS": default_fs,
            "fileType": "orc",
            "path": hive_path,
            "fileName": hive_table,
            "column": hive_columns,
            "writeMode": "append",
            "fieldDelimiter": "\u0001"
        }
    }
    yaml_dict["job"]["content"][0]["writer"] = writer_dict
    json_str = dict2json(yaml_dict)
    (file_path, temp_filename) = os.path.split(options.yaml_file)
    (shotname, extension) = os.path.splitext(temp_filename)
    datax_json_base_path = config_util.get("datax.json.path")
    if not os.path.exists(datax_json_base_path):
        os.makedirs(datax_json_base_path)
    datax_json_path = datax_json_base_path + "/" + shotname + ".json"
    print datax_json_path
    datax_json_file_handler = open(datax_json_path, "w")
    datax_json_file_handler.write(json_str)
    datax_json_file_handler.close()
    return datax_json_path


def run_datax(json_file):
    dataxpath = config_util.get("datax.path")
    child_process = subprocess.Popen("python " + dataxpath + " " + json_file, shell=True)
    (stdout, stderr) = child_process.communicate()
    return child_process.returncode


def run_check(options):
    (hive_db, hive_table) = parse_hive_db(options.hive_db)
    (mongo_db, collection) = parse_mongo(options.mongo_db)
    mongo_connection = Connection.get_mongo_connection(config_util,mongo_db)
    connection_db = mongo_connection[mongo_db]
    mongo_collection = connection_db[collection]
    # 需要获取 yaml 文件中的 query 条件
    yaml_dict = read_yaml_schema(options)
    # 替换 query 里面的参数
    parameter_dict = yaml_dict["job"]["content"][0]["reader"]["parameter"]
    query = None
    if parameter_dict and parameter_dict.has_key("query"):
        query = parameter_dict["query"]
        query = json2dict_utc(query)
    mongo_count = -1
    if query:
        print "query:", query
        mongo_count = mongo_collection.find(query).count()
    else:
        mongo_count = mongo_collection.find().count()
    mongo_connection.close()
    hive_connection = Connection.get_hive_connection(config_util,hive_db)
    count_hive = "select count(*) as hcount from " + options.hive_db
    partition = options.partition
    if partition is not None and len(partition) > 0:
        count_hive = count_hive + " where" + partition
    print "count_hive_sql:" + count_hive
    hive_cursor = hive_connection.cursor()
    hive_cursor.execute(count_hive)
    r2 = hive_cursor.fetchone()
    hive_count = r2[0]
    hive_connection.close()
    diff_count = abs(hive_count - mongo_count)
    print "hive_count:" + str(hive_count)
    print "mongo_count:" + str(mongo_count)
    if diff_count == 0:
        return 0
    threshold = diff_count * 100 / hive_count
    if threshold > 5:
        print "导出的数据总数有差异 mongodb:" + options.mongo_db + ":" + str(mongo_count) \
              + " hive:" + options.hive_db + ":" + str(hive_count) + " 差值:" + str(diff_count) \
              + " threshold:" + str(threshold)
        return 1
    else:
        print "导出的数据总数 mongodb:" + options.mongo_db + ":" + str(mongo_count) \
              + " hive:" + options.hive_db + ":" + str(hive_count) + " 差值:" + str(diff_count) \
              + " threshold:" + str(threshold)
        return 0


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # basepath = os.path.dirname(os.getcwd())
    # yaml_file = basepath + "/yaml/ods_beeper2_mongodb_car_team.yml"
    # fakeArgs = ["-f", yaml_file, '-t', "db_stg.driver_quiz_score"]
    # options, args = optParser.parse_args(fakeArgs)

    optParser = get_option_parser()
    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.yaml_file is None or options.hive_db is None:
        optParser.print_help()
        sys.exit(-1)
    else:
        if options.yaml_file is None:
            print("require yaml file")
            sys.exit(1)
        if options.hive_db is None:
            print("require hive database.table")
            sys.exit(1)
        if options.mongo_db is None:
            print("require mongo database.collection")
            sys.exit(1)
        json_file = build_json_file(options, args)
        code = run_datax(json_file)
        if code != 0:
            print("datax load failed")
            sys.exit(1)
        else:
            complete = run_check(options)
            sys.exit(complete)
