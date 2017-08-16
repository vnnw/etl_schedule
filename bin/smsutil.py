# -*- coding:utf-8 -*-
# !/usr/bin/env python

import urllib2
import json
from configutil import ConfigUtil


class SMSUtil(object):
    def __init__(self):
        self.config = ConfigUtil()

    '''
    phone 用 , 分割
    content 内容
    '''

    def send(self, phone, content):
        data = {
            "mobile": phone,
            "template": "super_template",
            "data": {
                "content": content
            }
        }
        host = self.config.get("sms.host")
        request = urllib2.Request(url="http://" + host + "/api/v1/sms/send/template", data=json.dumps(data))
        request.add_header('Content-Type', 'application/json')
        response = urllib2.urlopen(request)
        rep = response.read()
        return rep