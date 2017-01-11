#!/usr/bin/env python
# -*- coding:utf-8 -*-


import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import json
import subprocess
import yaml
from bin.configutil import ConfigUtil


configUtil = ConfigUtil()

def getOptionParser():
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


def removeDir(dir_name):
    os.system("hadoop fs -rmr " + dir_name)


def createDir(dir_name):
    os.system("hadoop fs -mkdir -p " + dir_name)


def changeType(ctype):
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


def buildJsonFile(options, args):
    yamlFile = options.yaml_file
    hiveDBTable = options.hive_db.split(".")
    hiveDB = hiveDBTable[0]
    hiveTable = hiveDBTable[1]
    mongoDBCollection = options.mongo_db.split(".")
    mongoDB = mongoDBCollection[0]
    collection = mongoDBCollection[1]
    partition = options.partition
    partitionKey = None
    partitionValue = None
    if partition is not None:
        partitionArray = partition.split("=")
        partitionKey = partitionArray[0].strip()
        partitionValue = partitionArray[1].strip()
    defaultFS = configUtil.get("hdfs.uri")
    hive_path = "/user/hive/warehouse/" + hiveDB + ".db/" + hiveTable
    if partition is None:
        removeDir(defaultFS + hive_path)
        createDir(defaultFS + hive_path)
    else:
        hive_path = hive_path + "/" + partitionValue

    yamlPath = configUtil.get("yaml.path")
    yamlDict = yaml2dict(yamlPath + "/" + yamlFile)

    yamlDict["job"]["content"][0]["reader"]["parameter"]["dbName"] = mongoDB
    yamlDict["job"]["content"][0]["reader"]["parameter"]["collectionName"] = collection

    columns = yamlDict["job"]["content"][0]["reader"]["parameter"]["column"]

    hive_columns = []
    for column in columns:
        hive_columns.append({"name": column["name"], "type": changeType(column["type"])})

    address = [configUtil.get("mongo." + mongoDB + ".address")]
    yamlDict["job"]["content"][0]["reader"]["parameter"]["address"] = address

    yamlDict["job"]["content"][0]["writer"] = {}  # set {}

    writer_dict = {
        "name": "hdfswriter",
        "parameter": {
            "defaultFS": defaultFS,
            "fileType": "orc",
            "path": hive_path,
            "fileName": hiveTable,
            "column": hive_columns,
            "writeMode": "append",
            "fieldDelimiter": "\u0001"
        }
    }
    yamlDict["job"]["content"][0]["writer"] = writer_dict
    json_str = dict2json(yamlDict)
    (filepath, tempfilename) = os.path.split(yamlFile)
    (shotname, extension) = os.path.splitext(tempfilename)
    basePath = configUtil.get("datax.json.path")
    if not os.path.exists(basePath):
        os.makedirs(basePath)
    path = basePath + "/" + shotname + ".json"
    print path
    baseFile = open(path, "w")
    baseFile.write(json_str)
    baseFile.close()
    return path


def runDatax(jsonFile):
    dataxpath = configUtil.get("datax.path")
    child_process = subprocess.Popen("python " + dataxpath + " " + jsonFile, shell=True)
    (stdout, stderr) = child_process.communicate()
    sys.exit(child_process.returncode)


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # basepath = os.path.dirname(os.getcwd())
    # yaml_file = basepath + "/yaml/ods_beeper2_mongodb_car_team.yml"
    # fakeArgs = ["-f", yaml_file, '-t', "db_stg.driver_quiz_score"]
    # options, args = optParser.parse_args(fakeArgs)

    optParser = getOptionParser()
    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.yaml_file is None or options.hive_db is None:
        optParser.print_help()
        sys.exit(-1)
    else:
        if options.yaml_file is None:
            print("require yaml file")
            sys.exit(-1)
        if options.hive_db is None:
            print("require hive database.table")
            sys.exit(-1)
        if options.mongo_db is None:
            print("require mongo database.collection")
            sys.exit(-1)
        jsonFile = buildJsonFile(options, args)
        runDatax(jsonFile)
