#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import subprocess
from bin.commonutil import CommonUtil


class DataXUtil(object):

    @staticmethod
    def run_datax(config, json_file):
        dataxpath = config.get("datax.path")
        python_bin = CommonUtil.python_bin(config)
        child_process = subprocess.Popen(python_bin + [dataxpath, json_file], shell=False)
        code = child_process.wait()
        return code