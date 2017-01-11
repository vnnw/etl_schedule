# -*- coding:utf-8 -*-
# !/usr/bin/env python

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import datetime
import random
import pyhs2
from bin.configutil import ConfigUtil

configUtil = ConfigUtil()

def getOptionParser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-s", "--sql", dest="mysql_sql", action="store", type="string", help="mysql sql")
    parser.add_option("-i", "--hive", dest="hive_table", action="store", type="string", help="hive table")
    parser.add_option("-t", "--to", dest="mysql_db", action="store", help="mysql database.table")
    parser.add_option("-c", "--columns", dest="mysql_columns", action="store",
                      help="mysql table columns split by comma")
    parser.add_option("-m", "--mode", dest="mode", action="store", help="overwrite or append")

    return parser


'''
run hql > data
'''


def hive_connection(db):
    host = configUtil.get("hive.host")
    port = configUtil.get("hive.port")
    connection = pyhs2.connect(host=host,
                               port=int(port),
                               authMechanism="PLAIN",
                               user="hadoop",
                               password="hadoop",
                               database=db)
    return connection


def runHsql(table):
    try:
        db = table.split(".")[0]
        connection = hive_connection(db)
        print "start run hive table :" + str(table)
        sys.stdout.flush()
        hive_sql = "select * from " + table
        mills = datetime.datetime.now().microsecond
        rand = random.randint(1, 100)
        tmpdatadir = configUtil.get("tmp.dir") + "/hdatas"
        tmpdata = tmpdatadir + "/" + str(mills) + "-" + str(rand) + ".data"
        write_handler = open(tmpdata, 'w')
        cursor = connection.cursor()
        cursor.execute(hive_sql)
        rows = cursor.fetch()
        for row in rows:
            data = []
            for index in range(0, len(rows)):
                data.append(str(row[index]))
            write_handler.writelines(",".join(data))
        write_handler.flush()
        write_handler.close()
        if code != 0:
            print "run hql error exit"
            sys.stdout.flush()
            return (-1, None)
        return (code, tmpdata)
    except Exception, e:
        print(e)
        sys.stdout.flush()
        return (-1, None)


def getMySQLConfig(mysqlDB):
    prefix = "mysql" + "." + mysqlDB
    dbConfig = {}
    dbConfig["username"] = configUtil.get(prefix + ".username")
    dbConfig["password"] = configUtil.get(prefix + ".password")
    dbConfig["host"] = configUtil.get(prefix + ".host")
    dbConfig["port"] = configUtil.get(prefix + ".port")
    return dbConfig


def getUserNamePassword():
    mysqlDBTable = options.mysql_db.split(".")
    mysqlDB = mysqlDBTable[0]
    mysqlTable = mysqlDBTable[1]
    dbInfo = getMySQLConfig(mysqlDB)
    return (dbInfo["username"], dbInfo["password"], dbInfo["host"])


def runMySQLCommand(command):
    (username, password, host) = getUserNamePassword()
    mysqlPath = configUtil.get("mysql.path")
    runCommand = mysqlPath + "/bin/mysql -u" + username + " -p" + password + " -h" + host + " -e \"" + command + "\""
    print("runCommand:" + str(runCommand))
    try:
        code = os.system(runCommand)
        return code
    except Exception, e:
        print e
        return -1


'''
load data to mysql
'''


def loadMySQL(db, columns, tmpdata):
    command = "load data test infile '" + tmpdata + "' INTO TABLE " + db + " fields terminated by '\t' (" + columns + ")"
    code = runMySQLCommand(command)
    os.remove(tmpdata)  # remove data file
    return code


def runSql(sql):
    code = runMySQLCommand(sql)
    return code


'''
run hql & load to mysql
'''


def run(options, args):
    hive_table = options.hive_table

    msql = options.mysql_sql
    if msql is not None:
        msql = msql.strip()
    db = options.mysql_db.strip()
    columns = options.mysql_columns.strip()
    try:
        (code, tmpdata) = runHsql(hive_table)
        if code == 0:
            if msql is not None and len(msql) > 0:
                mcode = runSql(msql)
                if mcode != 0:
                    print("run sql error")
                    return -1
            lcode = loadMySQL(db, columns, tmpdata)
            if lcode != 0:
                print("load data error")
                return -1
            else:
                return 0
        else:
            print("run hsql error")
            return -1
    except Exception, e:
        print(e)
        return -1


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # fakeArgs = ["-s","select * from t_test",'-t',"db_stg.driver_quiz_score"]

    optParser = getOptionParser()

    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.hive_table is None:
        print("require hive table")
        optParser.print_help()
        sys.exit(-1)
    if options.mysql_db is None:
        print("require mysql db")
        optParser.print_help()
        sys.exit(-1)
    if options.mysql_columns is None:
        print("require mysql columns")
        optParser.print_help()
        sys.exit(-1)
    code = run(options, args)
    sys.exit(code)
