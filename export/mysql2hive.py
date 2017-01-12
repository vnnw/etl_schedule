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


'''
 获取 mysql 连接
'''


def getMySQLConnection(mysqlDB, mysqlTable):
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


'''
获取MySQL表字段
'''


# List[(columnName,columnType,comment)]
def getMySQLTableColumns(columns, excludeColumns, mysqlDB, mysqlTable):
    columnList = []
    connection = getMySQLConnection(mysqlDB, mysqlTable)
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


def getHiveConnection(db):
    host = configUtil.get("hive.host")
    port = configUtil.get("hive.port")
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


def createHiveTable(hiveDB, hiveTable, columnList, partition):
    connection = getHiveConnection(hiveDB)
    cursor = connection.cursor()
    cursor.execute("use " + hiveDB)
    cursor.execute("show tables")
    result = cursor.fetchall()
    tables = set()
    for table in result:
        tables.add(table[0])

    if partition is None and hiveTable in tables:  # 如果有partition 不能删除表,应该增加partition
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
        if partitionKey is not None:  # 添加新的分区
            cursor.execute("alter table " + hiveTable + " add partition(" + partitionKey + "='" + partitionValue + "')")
    connection.close()


'''
 MySQL -> Hive 字段类型对应
'''


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


def parseMySQLDB(mysqlDBTable):
    mysqlDBTable = mysqlDBTable.split(".")
    mysqlDB = mysqlDBTable[0]
    mysqlTable = mysqlDBTable[1]
    return (mysqlDB, mysqlTable)


def parseHiveDB(hiveDBTable):
    hiveDBTable = hiveDBTable.split(".")
    hiveDB = hiveDBTable[0]
    hiveTable = hiveDBTable[1]
    return (hiveDB, hiveTable)


'''
处理 MySQL 的列,拼接成SQL
'''


def processMySQL(options):
    columns = options.columns  # 包含
    excludeColumns = options.exclude_columns  # 排除

    where = options.where
    if where is not None:  # where 条件
        where = where.strip()

    (mysqlDB, mysqlTable) = parseMySQLDB(options.mysql_db)
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
        columnNameTypeList.append({"name": name, "type": ctype})  # datax json 的格式
        formatColumnList.append((name, ctype, comment))  # 用来创建hive表的字段
    querySql = "select " + ", ".join(columnNameList) + " from  " + mysqlTable + " where 1=1 "
    if where is not None and len(where) > 0:
        querySql += " and " + where
    return (formatColumnList, columnNameTypeList, querySql)


def buildJsonFile(options, args):
    jsonData = readBaseJson()

    partition = options.partition
    if partition is not None:
        partition = partition.strip()


    (hiveDB, hiveTable) = parseHiveDB(options.hive_db)

    (mysqlDB, mysqlTable) = parseMySQLDB(options.mysql_db)

    mysqlConfigDict = getMySQLConfig(mysqlDB)

    (formatColumnList, columnNameTypeList, querySql) = processMySQL(options)

    createHiveTable(hiveDB, hiveTable, formatColumnList, partition)

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
    return child_process.returncode


'''
检查导入的数据是否完整,总的记录条数差距 %10
'''


def runCheck(options):
    (formatColumnList, columnNameTypeList, querySql) = processMySQL(options)
    countMySQL = "select count(1) as mcount from (" + querySql + ") t1"
    (hiveDB, hiveTable) = parseHiveDB(options.hive_db)
    (mysqlDB, mysqlTable) = parseMySQLDB(options.mysql_db)
    mysqlConnection = getMySQLConnection(mysqlDB,mysqlTable)
    mysqlCursor = mysqlConnection.cursor(MySQLdb.cursors.DictCursor)
    mysqlCursor.execute(countMySQL)
    r1 = mysqlCursor.fetchone()
    print r1
    mysqlCount = r1['mcount']
    hiveConnection = getHiveConnection(hiveDB)
    countHive = "select count(1) as hcount from " + options.hive_db
    hiveCursor = hiveConnection.cursor()
    hiveCursor.execute(countHive)
    r2 = hiveCursor.fetchone()
    hiveCount = r2[0]
    diffCount = abs(hiveCount - mysqlCount)
    threshold = diffCount * 100 / hiveCount
    if threshold > 10:
        print "导出的数据总数有差异 mysql:" + str(mysqlCount) + " hive:" + str(hiveCount)
        return 1
    else:
        print "导出数据总数 mysql:" + str(mysqlCount) + " hive:" + str(hiveCount)
        return 0

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
        code = runDatax(jsonFile)
        complete = runCheck(options)
        sys.exit(complete)
