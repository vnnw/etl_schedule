# -*- coding:utf-8 -*-

import os
import sys
from optparse import OptionParser

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bin.configutil import ConfigUtil
from export.connection import Connection
from export.hivetype import HiveType

config_util = ConfigUtil()


def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-d", "--database", dest="database", action="store", help="mysql database name")

    parser.add_option("-t", "--table", dest="table", action="store", help="mysql table name")

    return parser


def hive_table_columns(table):
    hive_connection = Connection.get_hive_connection(config_util, "ods_mysql")
    hive_cursor = hive_connection.cursor()
    hive_cursor.execute("desc " + table)
    rows = hive_cursor.fetchall()
    columns_set = set()
    for row in rows:
        column = row[0]
        if column.startswith("#"):
            break
        if column is None or len(column) == 0:
            break
        columns_set.add(column)
    return columns_set


def change_type(ctype):
    index = ctype.find("(")
    if index == -1:
        return ctype
    else:
        return ctype[:ctype.index("(")]


def mysql_table_columns(db, table):
    connection = Connection.get_mysql_connection(config_util, db)
    command = """show full columns from """ + table
    cursor = connection.cursor()
    cursor.execute(command)
    result = cursor.fetchall()
    table_columns_set = set()
    table_column_dict = {}
    for r in result:
        (field, ctype, c2, c3, c4, c5, c6, c7, comment) = r
        table_columns_set.add(field)
        table_column_dict[field] = {"Field": field, "Type": change_type(ctype), "Comment": comment}
    return (table_columns_set, table_column_dict)


if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')

    opt_parser = get_option_parser()

    options, args = opt_parser.parse_args(sys.argv[1:])

    if options.table is None or options.database is None:
        opt_parser.print_help()
        sys.exit(1)

    db = options.database.strip()
    table = options.table.strip()
    hive_table = "ods_mysql.ods_" + db + "__" + table
    hive_columns_set = hive_table_columns(hive_table)
    print hive_columns_set
    (mysql_table_columns_set, mysql_table_columns_dict) = mysql_table_columns(db, table)
    diff_columns = mysql_table_columns_set - hive_columns_set

    alter_hive_sql = "alter table {0} add columns({1}) cascade ;"
    if diff_columns:
        column_list = []
        column_name_list = []
        for column in diff_columns:
            column_dict = mysql_table_columns_dict[column]
            columns_str = column + " " + HiveType.change_type(column_dict['Type']) + " comment '" + column_dict['Comment'] + "' "
            column_list.append(columns_str)
            column_name_list.append(column)
        print alter_hive_sql.format(hive_table, ", ".join(column_list))
        print "columns:" + ",".join(column_name_list)
