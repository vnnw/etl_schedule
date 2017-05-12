#!/usr/bin/python
# -*- coding:utf-8 -*-


config="/Users/yxl/workspaces/workspace_python_00/etl_schedule/config/test/config.ini"

if __name__ == '__main__':
    file_handler = open(config,'r')
    for line in file_handler.readlines():
        if line and len(line.strip()) > 0 and line.startswith("mysql"):
            print line