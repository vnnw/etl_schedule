# !/usr/bin/env python
# -*- coding:utf-8 -*-


import os
from dateutil import DateUtil
import re
import yaml


class YamlParser(object):
    def vars_map(self, key, value):
        if key == 'today':
            if value is None:
                return DateUtil.get_now_fmt(None)
            else:
                return value
        elif key == 'yesterday':
            if value is None:
                return DateUtil.get_yesterday_fmt(None)
            else:
                return value
        elif key == 'intervalday':
            if value is None:
                raise Exception("intervalday is none")
            return DateUtil.get_interval_day_fmt(value, None)
        elif key == 'lastMonth':
            if value is None:
                return DateUtil.get_last_month()
            else:
                return value
        elif key == 'currentMonth':
            if value is None:
                return DateUtil.get_current_month()
            else:
                return value
        elif key == 'yesterdayMonth':
            if value is None:
                return DateUtil.get_yesterday_month()
            else:
                return value
        else:
            return value

    '''
    返回 包含sql,vars
    '''

    def parse_hive(self, step_dict):
        vars = []
        sqls = []
        sql_paths = []
        if step_dict.has_key('vars'):
            vars_dict = step_dict['vars']
            if vars_dict is not None and len(vars_dict) > 0:
                for (var_key, var_value_dict) in vars_dict.items():
                    var_type = var_value_dict['type']
                    if var_value_dict.has_key('value'):
                        var_value = var_value_dict['value']
                    map_value = self.vars_map(var_key, var_value)
                    if var_type == "string":
                        vars.append("set hivevar:" + str(var_key) + "='" + str(map_value) + "';")
                    else:
                        vars.append("set hivevar:" + str(var_key) + "=" + str(map_value) + ";")
        if step_dict.has_key('sqls'):
            sql_list = step_dict['sqls']
            if sql_list and len(sql_list) > 0:
                for sql_dict in sql_list:
                    sql_dict_value = sql_dict['sql']
                    if sql_dict_value.has_key('value') and sql_dict_value['value']:
                        sqls.append(sql_dict_value['value'])
                    if sql_dict_value.has_key('path') and sql_dict_value['path']:
                        sql_paths.append(sql_dict_value['path'])
        return (vars, sqls, sql_paths)

    def parse_export(self, python_path, project_path, step_dict):
        vars_dict = {}
        if step_dict.has_key('vars'):
            vars_dict = step_dict['vars']
            if vars_dict is not None and len(vars_dict) > 0:
                for (var_key, var_value_dict) in vars_dict.items():
                    var_type = var_value_dict['type']
                    if var_value_dict.has_key('value'):
                        var_value = var_value_dict['value']
                    map_value = self.vars_map(var_key, var_value)
                    vars_dict[var_key] = map_value
        command_list = []
        if step_dict.has_key('ops'):
            ops_list = step_dict['ops']
            if ops_list and len(ops_list) > 0:
                for ops_dict in ops_list:
                    for (command_key, command_value) in ops_dict.items():
                        command_list.append(self.export_command(python_path,
                                                                project_path,
                                                                command_key,
                                                                command_value,
                                                                vars_dict))
        return command_list

    '''
    替换变量
    '''

    def replace_sql_param(self, sql, vars_dict):

        p = re.compile(r"\$\{[^\}\$\u0020]+\}")
        m = p.findall(sql)
        if m and len(m) > 0:
            for key in m:
                var = key.replace("${", "")
                var = vars.replace("}", "")
                sql = sql.replace(key, self.vars_map(vars, vars_dict[var]))
        return sql

    def export_command(self, python_path, project_path, command_key, command_value, vars_dict):
        mysql2hive = project_path + '/export/mysql2hive.py'
        mongo2hive = project_path + '/export/mongo2hive.py'
        hive2mysql = project_path + '/export/hive2mysql.py'
        hive2excel = project_path + '/export/hive2excel.py'
        command_list = []
        command_list.append(python_path)
        if command_key == 'mysql2hive':
            command_list.append(mysql2hive)
            command_list.append("--from")
            command_list.append(command_value['mysql_db'])
            command_list.append("--to")
            command_list.append(command_value['hive_db'])
            if command_value.has_key("include_columns") and command_value['include_columns']:
                command_list.append("--columns")
                command_list.append(command_value['include_columns'])
            if command_value.has_key("exclude_columns") and command_value['exclude_columns']:
                command_list.append("--exclude-columns")
                command_list.append(command_value['exclude_columns'])
            return command_list
        if command_key == 'mongo2hive':
            command_list.append(mongo2hive)
            command_list.append("--file")
            command_list.append(command_value["yaml_file"])
            command_list.append("--from")
            command_list.append(command_value["mongo_db"])
            command_list.append("--to")
            command_list.append(command_value["hive_db"])
            return command_list
        if command_key == 'hive2mysql':
            command_list.append(hive2mysql)
            if command_value.has_key("delete_sql") and command_value["delete_sql"]:
                command_list.append("--sql")
                sql = self.replace_sql_param(command_value["delete_sql"], vars_dict)
                command_list.append(sql)
            if command_value.has_key("query") and command_value["query"]:
                command_list.append("--query")
                hql = self.replace_sql_param(command_value["query"])
                command_list.append(hql)
            command_list.append("--hive")
            command_list.append(command_value['hive_db'])
            command_list.append("--to")
            command_list.append(command_value['mysql_db'])
            command_list.append("--columns")
            command_list.append(command_value['mysql_columns'])
            return command_list
        if command_key == 'hive2excel':
            command_list.append(hive2excel)
            command_list.append("--name")
            command_list.append(command_value['excel_name'])
            command_list.append("--subject")
            command_list.append(command_value['email_subject'])
            command_list.append("--content")
            command_list.append(command_value['email_content'])
            command_list.append("--tables")
            command_list.append(command_value['hive_db'])
            command_list.append("--receivers")
            command_list.append(command_value['email_receivers'])
            return command_list


# for test
if __name__ == '__main__':
    basepath = "../job/script/app"
    for file in os.listdir(basepath):
        print "------" + file
        yaml_file = open(basepath + "/" + file, 'r')
        yaml_sql_path = "/job/sql"
        yaml_parser = YamlParser()
        yaml_dict = yaml.safe_load(yaml_file)
        steps = yaml_dict['steps']
        if steps and len(steps) > 0:
            for step in steps:
                step_type = step['type']
                if step_type == 'hive':
                    (vars, sqls, sql_paths) = yaml_parser.parse_hive(step)
                    print "vars:", len(vars), vars
                    print "sqls:", len(sqls), sqls
                    print "sql_paths", len(sql_paths), sql_paths
                if step_type == 'export':
                    command_list = yaml_parser.parse_export("", "", step)
                    if command_list and len(command_list) > 0:
                        for command in command_list:
                            print command
