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

configUtil = ConfigUtil()


def getOptionParser():
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


def getMySQLConfig(mysqlDB):
    prefix = "mysql" + "." + mysqlDB
    dbConfig = {}
    dbConfig["username"] = configUtil.get(prefix + ".username")
    dbConfig["password"] = configUtil.get(prefix + ".password")
    dbConfig["host"] = configUtil.get(prefix + ".host")
    dbConfig["port"] = configUtil.get(prefix + ".port")
    print dbConfig
    return dbConfig


def mysqlConnection(mysqlDB, mysqlTable):
    mysqlConfig = getMySQLConfig(mysqlDB)
    host = mysqlConfig["host"]
    username = mysqlConfig["username"]
    password = mysqlConfig["password"]
    port = int(mysqlConfig["port"])
    connection = MySQLdb.connect(host, username, password, mysqlDB, port, use_unicode=True, charset='utf8')
    return connection


def readBaseJson():
    jsonStr = """{
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
    jsonData = json.loads(jsonStr)
    return jsonData


# List[(columnName,columnType,comment)]
def getMySQLTableColumns(columns, excludeColumns, mysqlDB, mysqlTable):
    columnList = []
    connection = mysqlConnection(mysqlDB, mysqlTable)
    command = """show full columns from """ + mysqlTable
    cursor = connection.cursor()
    cursor.execute(command)
    result = cursor.fetchall()
    excludeColumnsSet = set()
    if excludeColumns is not None:
        excludeColumnsArray = excludeColumns.split(",")
        for excludeColumn in excludeColumnsArray:
            excludeColumnsSet.add(excludeColumn.strip())
    tableColumns = []
    tableColumnDict = {}
    for r in result:
        (field, ctype, c2, c3, c4, c5, c6, c7, comment) = r
        if field not in excludeColumnsSet:
            tableColumns.append(field)
            tableColumnDict[field] = {"Field": field, "Type": ctype, "Comment": comment}
    if columns is None:
        for columnName in tableColumns:
            column = tableColumnDict[columnName]
            columnList.append((column['Field'], column['Type'], column['Comment']))
    else:
        column_array = columns.split(",")
        for columnName in column_array:
            columnName = columnName.strip()
            if columnName not in tableColumns:
                print("column:" + columnName + "not in table:" + mysqlTable)
                sys.exit(-1)
            else:
                column = tableColumnDict[columnName]
                columnList.append((column['Field'], column['Type'], column['Comment']))
    return columnList


def getHiveConnection(host, port, db):
    connection = pyhs2.connect(host=host,
                               port=int(port),
                               authMechanism="PLAIN",
                               user="hadoop",
                               password="hadoop",
                               database=db)
    return connection


def createHiveTable(hiveDB, hiveTable, columnList, partition, where):
    host = configUtil.get("hive.host")
    port = configUtil.get("hive.port")
    connection = getHiveConnection(host, int(port), hiveDB)
    cursor = connection.cursor()
    cursor.execute("use " + hiveDB)
    cursor.execute("show tables")
    result = cursor.fetchall()
    tables = set()
    for table in result:
        tables.add(table[0])

    if partition is None and where is None and hiveTable in tables:
        cursor.execute("drop table " + hiveTable)
        tables.remove(hiveTable)

    partitionKey = None
    partitionValue = None
    if partition is not None:
        partitionArray = partition.split("=")
        partitionKey = partitionArray[0].strip()
        partitionValue = partitionArray[1].strip()

    if hiveTable in tables:
        if partitionKey is not None:
            cursor.execute(
                    "alter table " + hiveTable + " drop partition(" + partitionKey + "='" + partitionValue + "')")
            cursor.execute("alter table " + hiveTable + " add partition(" + partitionKey + "='" + partitionValue + "')")
    else:
        createColumn = []
        for column in columnList:
            (name, typestring, comment) = column
            createColumn.append(
                    "`" + str(name) + "` " + str(typestring).strip() + " comment \"" + str(comment).strip() + "\"")
        createColumnStr = " , ".join(createColumn)
        createStr = "create table " + hiveTable + " ( " + createColumnStr + " )"
        if partitionKey is not None:
            createStr += " partitioned by(" + partitionKey + " string)"
        createStr += " STORED AS ORC"
        print(createStr)
        cursor.execute(createStr)
        if partitionKey is not None:
            cursor.execute("alter table " + hiveTable + " add partition(" + partitionKey + "='" + partitionValue + "')")
    connection.close()


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
    if ctype == "decimal":
        ctype = "double"
    return ctype


def buildJsonFile(options, args):
    jsonData = readBaseJson()
    partition = options.partition
    if partition is not None:
        partition = partition.strip()
    columns = options.columns
    excludeColumns = options.exclude_columns

    where = options.where
    if where is not None:
        where = where.strip()

    mysqlDBTable = options.mysql_db.split(".")
    mysqlDB = mysqlDBTable[0]
    mysqlTable = mysqlDBTable[1]
    hiveDBTable = options.hive_db.split(".")
    hiveDB = hiveDBTable[0]
    hiveTable = hiveDBTable[1]

    columnList = getMySQLTableColumns(columns, excludeColumns, mysqlDB, mysqlTable)
    columnNameList = []
    columnNameTypeList = []
    formatColumnList = []
    for column in columnList:
        (name, ctype, comment) = column
        columnNameList.append("`" + name + "`")
        if "(" in ctype:
            ctype = ctype[:ctype.index("(")]
        ctype = changeType(ctype)
        columnNameTypeList.append({"name": name, "type": ctype})
        formatColumnList.append((name, ctype, comment))
    createHiveTable(hiveDB, hiveTable, formatColumnList, partition, where)
    mysqlConfigDict = getMySQLConfig(mysqlDB)
    querySql = "select " + ", ".join(columnNameList) + " from  " + mysqlTable + " where 1=1 "
    if where is not None and len(where) > 0:
        querySql += " and " + where
    readerParameterDict = {
        "connection": [{
            "querySql": [querySql],
            "jdbcUrl": ["jdbc:mysql://" + mysqlConfigDict["host"] + ":" + str(mysqlConfigDict["port"]) + "/" + mysqlDB]
        }],
        "username": mysqlConfigDict["username"],
        "password": mysqlConfigDict["password"]
    }
    jsonData["job"]["content"][0]["reader"]["parameter"] = readerParameterDict

    if partition is not None:
        path = "/user/hive/warehouse/" + hiveDB + ".db/" + hiveTable + "/" + partition
    else:
        path = "/user/hive/warehouse/" + hiveDB + ".db/" + hiveTable

    writerParameterDict = {
        "column": columnNameTypeList,
        "defaultFS": configUtil.get("hdfs.uri"),
        "fieldDelimiter": '\u0001',
        "fileName": mysqlTable,
        "fileType": "orc",
        "path": path,
        "writeMode": "append"
    }

    jsonData["job"]["content"][0]["writer"]["parameter"] = writerParameterDict

    basePath = configUtil.get("datax.json.path")
    if not os.path.exists(basePath):
        os.makedirs(basePath)
    path = basePath + "/" + mysqlDB + "_" + mysqlTable + "_" + hiveDB + "_" + hiveTable + ".json"
    baseFile = open(path, "w")
    jsonStr = json.dumps(jsonData, indent=4, sort_keys=False, ensure_ascii=False)
    jsonStr = jsonStr.replace("\\\\", "\\")
    # print jsonStr
    baseFile.write(jsonStr)
    baseFile.close()
    return path


def runDatax(jsonFile):
    datax = configUtil.get("datax.path") + " " + jsonFile
    print datax
    child_process = subprocess.Popen("python " + datax, shell=True)
    (stdout, stderr) = child_process.communicate()
    sys.exit(child_process.returncode)


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # fakeArgs = ["-f","test.t_test",'-t',"db_stg.driver_quiz_score"]

    optParser = getOptionParser()

    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.mysql_db is None or options.hive_db is None:
        optParser.print_help()
        sys.exit(-1)
    else:
        if options.mysql_db is None:
            print("require mysql database.table")
            sys.exit(-1)
        if options.hive_db is None:
            print("require hive database.table")
            sys.exit(-1)
        jsonFile = buildJsonFile(options, args)
        runDatax(jsonFile)
