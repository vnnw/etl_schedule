# -*- coding:utf-8 -*-

import os
import sys
import MySQLdb
from optparse import OptionParser
from bin.configutil import ConfigUtil
from export.connection import Connection


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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


def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-t", "--table", dest="table", action="store", help="database table")

    return parser

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    optParser = get_option_parser()

    options, args = optParser.parse_args(sys.argv[1:])

    table = options.table.strip()

    db_set = set()
    for key in config_util.config.keys():
        if key.startswith("mysql") and len(key.split(".")) == 3:
            mysql, db, config = key.split(".")
            db_set.add(db)
    for db in db_set:
        connection = Connection.get_mysql_connection(config_util, db)
        tables = get_tables(connection, db)
        if table.strip() in tables:
            print "database: " + db + " table:" + table
            config = Connection.get_mysql_config(config_util, db)
            sql = "mysql -u" + config.get("username") + " -p" + \
                  config.get("password") + " -h" + config.get("host") + \
                  " -P" + config.get("port")
            print sql
            break
