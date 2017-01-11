#!/usr/bin/python
# -*- coding:utf-8 -*-

import socket
import commands
import urllib2
import json
import time
import datetime
import traceback


class ProcessMonitor(object):

    def __init__(self):
        self.config = []
        # self.config.append("ip,host,namenode|datanode")

        self.config.append("192.168.0.75,vm-ha-dispatch75.yn.com,scheduler|executor|HiveServer2")
        self.config.append("192.168.0.30,ha-master30.yn.com,namenode|resourcemanager")
        self.config.append("192.168.0.31,ha-secondrymaster31.yn.com,namenode")
        self.config.append("192.168.0.32,ha-data32.yn.com,datanode|nodemanager|journalnode")
        self.config.append("192.168.0.33,ha-data33.yn.com,datanode|nodemanager|journalnode")
        self.config.append("192.168.0.34,ha-data34.yn.com,datanode|nodemanager|journalnode")

    def ping(self, host):
        try:
            (status, output) = commands.getstatusoutput("ping -c 2 -w 5 " + host)
            return (status, output)
        except Exception, e:
            print(traceback.format_exc())
            return (-1, None)

    def run_command(self, user, host, process_name):
        try:
            (status, output) = commands.getstatusoutput("ssh " + user + "@" + host + " \"ps -ef | grep "
                                                        + process_name + " | grep  -v grep \"")
            return (status, output)
        except Exception, e:
            print(traceback.format_exc())
            return (-1, None)

    def get_ip(self):
        try:
            ipList = socket.gethostbyname_ex(socket.gethostname())
            (host, nothing, ip) = ipList
            return (host, str(ip[0]))
        except Exception, e:
            print(traceback.format_exc())
            return ("nothing", "nothing")

    def send_msg(self, ip, host, process):
        data = {
            "mobile": "18010141306",
            "template": "super_template",
            "data": {
                "content": "服务器 ip:" + ip + " host:" + host + " process:" + process + " 不存在"
            }
        }
        host = "192.168.0.99:7001"
        request = urllib2.Request(url="http://" + host + "/api/v1/sms/send/template", data=json.dumps(data))
        request.add_header('Content-Type', 'application/json')
        response = urllib2.urlopen(request)
        print(response.read())

    def monitor(self):
        print "run monitor"
        for line in self.config:
            line = line.strip()
            print line
            line_array = line.split(",")
            user = "hadoop"
            ip = line_array[0]
            host = line_array[1]
            (status, ouput) = self.ping(ip)
            if status != 0:
                print("ip:" + ip + " host:" + host + " 服务器无法ping通")
                self.send_msg(ip, host, "ping")
            process_line = line_array[2]
            process_array = process_line.split("|")
            for process in process_array:
                (status, ouput) = self.run_command(user, host, process)
                if status != 0:
                    print("ip:" + ip + " host:" + host + " 进程:" + process + " 不存在")
                    self.send_msg(ip, host, process)

    def run(self):
        while True:
            print "------------" + str(datetime.datetime.now()) + "---------------"
            self.monitor()
            time.sleep(300)


if __name__ == '__main__':
    monitor = ProcessMonitor()
    monitor.run()
