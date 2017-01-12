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
from bin.configutil import ConfigUtil

config_util = ConfigUtil()


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
    os.system("hadoop fs -rmr " + dir_name)


def create_dir(dir_name):
    os.system("hadoop fs -mkdir -p " + dir_name)


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
    if ctype == "array":
        ctype = "string"
    return ctype


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


def build_json_file(options, args):
    (hive_db, hive_table) = parse_hive_db(options.hive_db)

    (mongo_db, collection) = parse_mongo(options.mongo_db)

    partition = options.partition

    partition_key = None
    partition_value = None
    if partition is not None:
        partition_array = partition.split("=")
        partition_key = partition_array[0].strip()
        partition_value = partition_array[1].strip()

    defaultFS = config_util.get("hdfs.uri")
    hive_path = "/user/hive/warehouse/" + hive_db + ".db/" + hive_table
    if partition is None:
        remove_dir(defaultFS + hive_path)
        create_dir(defaultFS + hive_path)
    else:
        hive_path = hive_path + "/" + partition_value

    yaml_path = config_util.get("yaml.path")
    yaml_file = options.yaml_file
    yaml_dict = yaml2dict(yaml_path + "/" + yaml_file)

    yaml_dict["job"]["content"][0]["reader"]["parameter"]["dbName"] = mongo_db
    yaml_dict["job"]["content"][0]["reader"]["parameter"]["collectionName"] = collection

    columns = yaml_dict["job"]["content"][0]["reader"]["parameter"]["column"]

    hive_columns = []
    for column in columns:
        hive_columns.append({"name": column["name"], "type": change_type(column["type"])})

    mongo_host = config_util.get("mongo." + mongo_db + ".host")
    mongo_port = config_util.get("mongo." + mongo_db + ".port")
    address = [mongo_host + ":" + mongo_port]
    yaml_dict["job"]["content"][0]["reader"]["parameter"]["address"] = address

    yaml_dict["job"]["content"][0]["writer"] = {}  # set {}

    writer_dict = {
        "name": "hdfswriter",
        "parameter": {
            "defaultFS": defaultFS,
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
    (filepath, tempfilename) = os.path.split(yaml_file)
    (shotname, extension) = os.path.splitext(tempfilename)
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


def get_mongo_connection(mongo_db):
    host = config_util.get("mongo." + mongo_db + ".host")
    port = config_util.get("mongo." + mongo_db + ".port")
    connection = pymongo.MongoClient(host, int(port))
    return connection


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


def run_check(options):
    (hive_db, hive_table) = parse_hive_db(options.hive_db)
    (mongo_db, collection) = parse_mongo(options.mongo_db)
    mongo_connection = get_mongo_connection(mongo_db)
    connection_db = mongo_connection[mongo_db]
    mongo_collection = connection_db[collection]
    mongo_count = mongo_collection.find().count()
    mongo_connection.close()
    hive_connection = get_hive_connection(hive_db)
    count_hive = "select count(1) as hcount from " + options.hive_db
    partition = options.partition
    if partition is not None and len(partition) > 0:
        count_hive = count_hive + " where" + partition
    hive_cursor = hive_connection.cursor()
    hive_cursor.execute(count_hive)
    r2 = hive_cursor.fetchone()
    hive_count = r2[0]
    hive_connection.close()
    diff_count = abs(hive_count - mongo_count)
    threshold = diff_count * 100 / hive_count
    if threshold > 10:
        print "导出的数据总数有差异 mongodb:" + str(mongo_count) + " hive:" + str(hive_count)
        return 1
    else:
        print "导出数据总数 mongodb:" + str(mongo_count) + " hive:" + str(hive_count)
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
