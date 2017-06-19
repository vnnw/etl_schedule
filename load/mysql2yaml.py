# -*- coding:utf-8 -*-

import os
import sys
import MySQLdb
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
from export.hivetype import HiveType
from export.connection import Connection
from bin.configutil import ConfigUtil

config_util = ConfigUtil()


def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-d", "--from", dest="db", action="store", type="string", help="mysql database")
    parser.add_option("-t", "--table", dest="table", action="store", help="database table")
    parser.add_option("-p", "--path", dest="path", action="store",
                      help="sql yaml base path")

    return parser


def read_table_comment():
    path = "table_comment.txt"
    if os.path.exists(path):
        file_handler = open(path, 'r')
        table_comment_dict = {}
        for line in file_handler.readlines():
            print line
            if line and len(line.strip()) > 0:
                table_comment_dict[line.split("=")[0].strip()] = line.split("=")[1].strip()
            file_handler.close()
    return table_comment_dict


def write2File(file, sql):
    file_handler = open(file, 'w')
    file_handler.writelines(sql)
    file_handler.flush()
    file_handler.close


def gen_yaml(db, table, columns, yaml_dir):
    file_handler = open("template.yml", "r")
    yaml_file = "ods_" + db + "__" + table + ".yml"
    file_handler_write = open(yaml_dir + "/" + yaml_file, "w")
    for line in file_handler.readlines():
        if line.strip() == "mysql_db:":
            line = line.rstrip() + " " + db + "." + table + "\n"
        if line.strip() == "hive_db:":
            line = line.rstrip() + " " + "ods_mysql.ods_" + db + "__" + table + "\n"
        if line.strip() == "include_columns:":
            column_names = []
            for column in columns:
                (name, typestring, comment) = column
                column_names.append(name)
            line = line.rstrip() + " " + ",".join(column_names) + "\n"
        file_handler_write.writelines(line)
    file_handler.close()
    file_handler_write.close()
    return yaml_file


def gen_sql(db, table, columns, table_comment):
    print "-" * 20
    print table, table_comment
    include_column = []
    create_column = []
    table_name = "ods_mysql.ods_" + db + "__" + table
    for column in columns:
        (name, typestring, comment) = column
        ctype = typestring.split("(")[0]
        include_column.append(name)
        create_column.append(
                "    `" + str(name) + "` " + str(HiveType.change_type(ctype)).strip() + " comment \"" + str(
                        comment).strip() + "\"")
    create_column_str = ",\n".join(create_column)
    create_sql_str = ""
    # create_sql_str += "drop table if exists " + table_name + ";\n"
    create_sql_str += "create external table if not exists " + table_name + " ( \n" + create_column_str + " )"
    create_sql_str += "\ncomment \"" + table_comment + "\""
    create_sql_str += "\npartitioned by(p_day string)"
    create_sql_str += "\nstored as orc ;"

    print "ods_" + db + "__" + table
    print ",".join(include_column)

    return create_sql_str


def get_table_comment(connection, table):
    sql = "show table status where name = %s"
    row_dict = run_sql_dict(connection, sql, (table,))
    status = row_dict[0]
    if status:
        comment = status['Comment']
        return comment
    else:
        return None


def run(db, path, stable):
    connection = Connection.get_mysql_connection(config_util, db)
    tables = get_tables(connection)
    schedule_list = []
    sql_dir = path + "/sql"
    if not os.path.exists(sql_dir):
        os.makedirs(sql_dir)
    yaml_dir = path + "/yaml"
    if not os.path.exists(yaml_dir):
        os.makedirs(yaml_dir)
    for table in tables:
        if stable and len(stable.strip()) > 0 and stable != table:
            continue

        columns = get_table_columns(connection, table)

        comment = get_table_comment(connection, table)
        if not comment or len(comment.strip()) == 0:
            comment = "xxxx"

        sql = gen_sql(db, table, columns, comment)
        sql_name = "ods_" + db + "__" + table + ".sql"
        print "----" * 20
        write2File(sql_dir + "/" + sql_name, sql)
        yaml_file = gen_yaml(db, table, columns, yaml_dir)
        schedule = ("ods_" + db + "__" + table).lower() + ",time,0,1," + str(
                random.randint(1, 30)) + ",day,yxl,ods_mysql/" + yaml_file + "\n"
        schedule_list.append(schedule)
    gen_schedule(path + "/schedule.txt", schedule_list)


def gen_schedule(schedule_path, schedule_list):
    write2File(schedule_path, schedule_list)


def get_table_columns(connection, table):
    sql = "show full columns from " + table
    rows = run_sql_dict(connection, sql, ())
    columns = list()
    for row in rows:
        columns.append((row.get('Field'), row.get('Type'), row.get('Comment')))
    return columns


def run_sql_dict(connection, sql, params):
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def get_tables(connection):
    sql = "show tables"
    rows = run_sql_dict(connection, sql, ())
    tables = set()
    for row in rows:
        tables.add(row.values()[0])
    return tables


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    optParser = get_option_parser()

    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.db is None or options.path is None:
        optParser.print_help()
        sys.exit(1)
    else:
        run(options.db, options.path + "/gen", options.table)
