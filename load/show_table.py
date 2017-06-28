# -*- coding:utf-8 -*-

import os
import sys
import MySQLdb
from optparse import OptionParser

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bin.configutil import ConfigUtil
from export.connection import Connection

config_util = ConfigUtil()


def get_tables(connection, db):
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("show tables")
    rows = cursor.fetchall()
    tables = set()
    for row in rows:
        for (name, item) in row.items():
            tables.add(item)
    return tables


def get_hive_tables(connection):
    cursor = connection.cursor()
    cursor.execute("show tables")
    result = cursor.fetchall()
    tables = set()
    for table in result:
        tables.add(table[0])
    return tables


def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-t", "--table", dest="table", action="store", help="mysql table name")

    return parser


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    optParser = get_option_parser()

    options, args = optParser.parse_args(sys.argv[1:])

    if options.table is None:
        optParser.print_help()
        sys.exit(1)

    table = options.table.strip()

    db_set = set()
    for key in config_util.config.keys():
        if key.startswith("mysql") and len(key.split(".")) == 3:
            mysql, db, config = key.split(".")
            db_set.add(db)
    hive_connection = Connection.get_hive_connection(config_util, "ods_mysql")
    for db in db_set:
        hive_table = "ods_" + db + "__" + table
        hive_tables = get_hive_tables(hive_connection)
        if hive_table.strip() in hive_tables:
            print table + " 在hive中已经存在: ods_mysql." + hive_table
            break
        connection = Connection.get_mysql_connection(config_util, db)
        tables = get_tables(connection, db)
        connection.close()
        if table.strip() in tables:
            print "database: " + db + " table:" + table
            config = Connection.get_mysql_config(config_util, db)
            sql = "mysql -u" + config.get("username") + " -p" + \
                  config.get("password") + " -h" + config.get("host") + \
                  " -P" + config.get("port") + " -D" + db
            print sql
            break
    hive_connection.close()
