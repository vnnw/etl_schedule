# -*- coding:utf-8 -*-

import os
import sys
import MySQLdb

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from export.hivetype import HiveType
from export.connection import Connection
from bin.configutil import ConfigUtil

config_util = ConfigUtil()


def read_table_comment():
    file_handler = open("table_comment.txt", 'r')
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
    file_handler_write = open(yaml_dir + "/" + "ods_" + db + "__" + table + ".yml", "w")
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


def run(mysql_db, sql_dir, yaml_dir):
    connection = Connection.get_mysql_connection(config_util, mysql_db)
    tables = get_tables(connection)
    table_comment_dict = read_table_comment()
    print table_comment_dict
    for table in tables:
        columns = get_table_columns(connection, table)

        if table_comment_dict.has_key(db + "." + table):
            comment = table_comment_dict[db + "." + table]
        else:
            comment = "xxxx"

        sql = gen_sql(db, table, columns, comment)
        sql_name = "ods_" + mysql_db + "__" + table + ".sql"
        print "----" * 20
        write2File(sql_dir + "/" + sql_name, sql)
        gen_yaml(db, table, columns, yaml_dir)


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
    db = "beeper_trans_settlement"
    table = ""
    run(db, "sql", "yaml")
