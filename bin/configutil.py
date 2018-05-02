#!/usr/bin/python
# -*- coding:utf-8 -*-

import os

'''
读取配置文件
'''


class ConfigUtil(object):
    def __init__(self):
        envpath = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = envpath + "/config/product/config.ini"
        self.config = self.load_config(config_path)

    def load_config(self, path):
        config = {}
        config_file_handler = open(path)
        if config_file_handler is None:
            raise IOError("无法读取配置文件")
        for line in config_file_handler.readlines():
            if line and len(line.strip()) > 0 and (not line.startswith("#")):
                key_value = line.strip().split("=")
                if len(key_value) == 2:
                    key = key_value[0].strip()
                    value = key_value[1].strip()
                    config[key] = value
                else:
                    print("无法读取配置项:" + str(line))
        return config

    def get(self, name):
        value = self.config.get(name)
        if not value:
            raise Exception("读取配置" + name + "失败")
        return value

    def get_or_else(self, name, default):
        value = self.config.get(name)
        if value is None or len(value.strip()) == 0:
            return default
        else:
            return value

    def get_boolean_or_else(self, name, default):
        value = self.config.get(name)
        if value is None or len(value.strip()) == 0:
            return default
        else:
            value = value.upper()
            if value == "TRUE":
                return True
            else:
                return False

if __name__ == '__main__':
    config = ConfigUtil()
    print config.config
