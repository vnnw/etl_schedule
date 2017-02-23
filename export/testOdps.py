#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import json
import subprocess
import pyhs2
from odps import ODPS
from bin.configutil import ConfigUtil

config_util = ConfigUtil()


def get_odps_connection(odps_db):
    access_id = config_util.get("odps_accessId")
    access_key = config_util.get("odps_accessKey")
    endpoint = config_util.get("odps_endpoint")
    odps = ODPS(access_id, access_key, odps_db, endpoint=endpoint)
    return odps

def get_odps_table_columns() :
    odps = get_odps_connection("driver_heartbeat")
    t = odps.get_table("heartbeat")
    columns =  t.schema.columns
    for column in columns:
        print str(column.name) + "\t" + str(column.type).lower() + "\t" + str(column.comment)

    sql = "select count(1) as mcount from driver_heartbeat.heartbeat"

    with odps.execute_sql(sql).open_reader() as reader:
        for record in reader:
               print record

if __name__ == '__main__':
    get_odps_table_columns()