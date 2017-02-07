# -*- coding:utf-8 -*-
# !/usr/bin/env python

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import datetime
import random
import pyhs2
import traceback
from bin.configutil import ConfigUtil

config_util = ConfigUtil()

def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-s", "--sql", dest="mysql_sql", action="store", type="string", help="mysql sql")
    parser.add_option("-i", "--hive", dest="hive_table", action="store", type="string", help="hive table")
    parser.add_option("-t", "--to", dest="mysql_db", action="store", help="mysql database.table")
    parser.add_option("-q", "--query", dest="hive_hql", action="store",help="hive query hql")
    parser.add_option("-c", "--columns", dest="mysql_columns", action="store",
                      help="mysql table columns split by comma")
    parser.add_option("-m", "--mode", dest="mode", action="store", help="overwrite or append")

    return parser


'''
run hql > data
'''


def hive_connection(db):
    host = config_util.get("hive.host")
    port = config_util.get("hive.port")
    connection = pyhs2.connect(host=host,
                               port=int(port),
                               authMechanism="PLAIN",
                               user="hadoop",
                               password="hadoop",
                               database=db)
    return connection


def run_hsql(table, hive_hql):
    try:
        db = table.split(".")[0]
        connection = hive_connection(db)
        print "start run hive table :" + str(table)
        sys.stdout.flush()
        hive_query = "select * from " + table
        if hive_hql and len(hive_hql) > 0:
            hive_query = hive_hql.strip()
            hive_query = hive_query.replace(";", "")
        print "Query:", hive_query
        mills = datetime.datetime.now().microsecond
        rand = random.randint(1, 100)
        tmpdatadir = config_util.get("tmp.path") + "/hdatas"
        tmpdata = tmpdatadir + "/" + str(mills) + "-" + str(rand) + ".data"
        print "数据文件:", tmpdata
        write_handler = open(tmpdata, 'w')
        cursor = connection.cursor()
        cursor.execute(hive_query)
        rows = cursor.fetch()
        for row in rows:
            data = []
            for index in range(0, len(row)):
                data.append(str(row[index]))
            write_handler.writelines(",".join(data)+'\n')
        write_handler.flush()
        write_handler.close()
        return (0, tmpdata)
    except Exception, e:
        print(traceback.format_exc())
        sys.stdout.flush()
        return (-1, None)


def get_mysql_config(mysql_db):
    prefix = "mysql" + "." + mysql_db
    dbConfig = {}
    dbConfig["username"] = config_util.get(prefix + ".username")
    dbConfig["password"] = config_util.get(prefix + ".password")
    dbConfig["host"] = config_util.get(prefix + ".host")
    dbConfig["port"] = config_util.get(prefix + ".port")
    return dbConfig


def get_username_password():
    mysql_db_table = options.mysql_db.split(".")
    mysql_db = mysql_db_table[0]
    mysql_table = mysql_db_table[1]
    db_info = get_mysql_config(mysql_db)
    return (db_info["username"], db_info["password"], db_info["host"])


def run_mysql_command(command):
    (username, password, host) = get_username_password()
    mysql_path = config_util.get("mysql.path")
    print mysql_path
    run_command = mysql_path + "/bin/mysql -u" + username + " -p" + password + " -h" + host + " -e \"" + command + "\""
    print("run_command:" + str(run_command))
    try:
        code = os.system(run_command)
        return code
    except Exception, e:
        print traceback.format_exc()
        return -1


'''
load data to mysql
'''


def load_mysql(db, columns, tmpdata):
    command = "load data local infile '" + tmpdata + "' INTO TABLE " + db + " fields terminated by ','  (" + columns + ")"
    code = run_mysql_command(command)
    os.remove(tmpdata)  # remove data file
    return code


def run_sql(sql):
    code = run_mysql_command(sql)
    return code


'''
run hql & load to mysql
'''


def run(options, args):
    hive_table = options.hive_table
    hive_hql = options.hive_hql
    msql = options.mysql_sql
    if msql is not None:
        msql = msql.strip()
    db = options.mysql_db.strip()
    columns = options.mysql_columns.strip()
    try:
        (code, tmpdata) = run_hsql(hive_table,hive_hql)
        if code == 0:
            if msql is not None and len(msql) > 0:
                mcode = run_sql(msql)
                if mcode != 0:
                    print("run sql error")
                    return -1
            lcode = load_mysql(db, columns, tmpdata)
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

    optParser = get_option_parser()

    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.hive_table is None:
        print("require hive table")
        optParser.print_help()
        sys.exit(1)
    if options.mysql_db is None:
        print("require mysql db")
        optParser.print_help()
        sys.exit(1)
    if options.mysql_columns is None:
        print("require mysql columns")
        optParser.print_help()
        sys.exit(1)
    code = run(options, args)
    sys.exit(code)
