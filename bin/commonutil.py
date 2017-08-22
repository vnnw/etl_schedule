#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys


class CommonUtil(object):
    @staticmethod
    def print_and_flush(content):
        print(content)
        sys.stdout.flush()

    @staticmethod
    def python_bin(config):
        python_path = config.get("python.home")
        if python_path is None or len(python_path) == 0:
            raise Exception("can't find python.home")
        python_bin = python_path + "/bin/python"
        return [python_bin, "-u"]