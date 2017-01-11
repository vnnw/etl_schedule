#!/usr/bin/python
# -*- coding:utf-8 -*-

import logging

import logging.handlers
import logging.config
import os
from configutil import ConfigUtil


class Logger(object):
    def __init__(self, name):
        self.config = ConfigUtil()
        # 创建一个logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')

        # 用于写入日志文件
        log_path = self.config.get("shcedule.log.path")
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        log_file = log_path + "/" + name + ".log"
        handler = logging.handlers.TimedRotatingFileHandler(log_file, when="midnight", backupCount=10)
        # 输出到控制台
        # handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def getlog(self):
        return self.logger
