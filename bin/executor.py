#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys
import os
import subprocess
from logutil import Logger
from dateutil import DateUtil
from configutil import ConfigUtil
from dboption import DBOption
from monitor import Monitor
from apscheduler.schedulers.blocking import BlockingScheduler
import traceback

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)


'''
1. 遍历t_etl_job_queue 表,确定可以运行的Job
2. 启动进程运行Job对应的脚本
'''


class Executor(object):

    def __init__(self):
        self.logger = Logger("executor").getlog()
        self.aps_log = Logger("apscheduler").getlog()
        self.config = ConfigUtil()
        self.dboption = DBOption()
        self.process_running = {}
        self.scheduler = BlockingScheduler()
        self.monitor = Monitor()

    '''
     运行 t_etl_job_queue 中Pending状态的job
    '''

    def run_queue_job_pending(self):
        self.logger.info("\n")
        self.logger.info("... interval run run_queue_job_pending ....")
        try:
            self.check_process_state()  # 判断已有的进程状态

            logpath = self.config.get("job.log.path")
            if logpath is None or len(logpath.strip()) == 0:
                raise Exception("can't find slave job.log.path")
            if not os.path.exists(logpath):
                os.makedirs(logpath)
            today = DateUtil.get_today()
            today_log_dir = logpath + "/" + today
            if not os.path.exists(today_log_dir):
                os.makedirs(today_log_dir)
            queue_job = self.dboption.get_queue_job_pending()
            if queue_job is not None:
                job_name = queue_job["job_name"]
                etl_job = self.dboption.get_job_info(job_name)
                job_status = etl_job["job_status"]
                if job_status != "Pending":
                    self.logger.info("当前 Job:" + job_name + " 状态 " + job_status + " 不是Pending 无法运行")
                    return

                logfile = today_log_dir + "/" + job_name + "_" + today + ".log"
                bufsize = 0
                logfile_handler = open(logfile, 'w', bufsize)
                python_path = self.config.get("python.home")
                if python_path is None or len(python_path) == 0:
                    raise Exception("can't find python.home")
                python_bin = python_path + "/bin/python"
                run_path = project_path + "/bin/" + "runcommand.py"
                child = subprocess.Popen([python_bin, run_path, "-job", job_name],
                                         stdout=logfile_handler.fileno(),
                                         stderr=subprocess.STDOUT,
                                         shell=False)
                pid = child.pid
                if pid > 0:
                    self.logger.info("创建子进程:" + str(pid) + " 运行Job:" + str(job_name))
                    code = self.dboption.update_job_running(job_name)
                    if code != 1:
                        try:
                            self.logger.info("更新Job:" + job_name + " 运行状态为Running失败,停止创建的进程")
                            self.terminate_process(child, logfile_handler)
                        except Exception, e:
                            self.logger.error(e)
                            self.logger.error("terminate 子进程异常")
                            logfile_handler.flush()
                            logfile_handler.close()
                    else:
                        self.logger.info("更新Job:" + job_name + " 运行状态Running")
                        code = self.dboption.update_job_queue_done(job_name)  # FixMe 事物问题
                        self.logger.info("更新Job Queue job:" + str(job_name) + " 状态为Done,影响行数:" + str(code))
                        if code != 1:
                            self.logger.error("更新Job Queue job:" + job_name + " 状态为Done失败")
                            self.terminate_process(child, logfile_handler)
                            self.logger.info("重新修改job_name:" + job_name + " 状态为Pending 等待下次运行")
                            self.dboption.update_job_pending_from_running(job_name)
                        else:
                            self.process_running[child] = {"logfile_handler": logfile_handler,
                                                           "job_name": job_name, "pid": pid}
                else:
                    self.logger.error("启动子进程异常pid:" + str(pid))
                    logfile_handler.flush()
                    logfile_handler.close()
            else:
                self.logger.info("t_etl_job_queue 中没有需要运行的Job")
        except Exception, e:
            self.logger.error(traceback.format_exc())
            self.logger.error("run_queue_job_pending 获取 t_etl_job_queue 的中的Job 异常")

    # 停止启动的进程
    def terminate_process(self, child, logfile):
        try:
            pid = child.pid
            self.logger.info("terminate process " + str(pid))
            child.terminate()
            logfile.flush()
            logfile.close()
        except Exception, e:
            self.logger.error(traceback.format_exc())
            self.logger.error("terminate 子进程异常")
            logfile.flush()
            logfile.close()

    '''
     子进程运行job成功
    '''

    def check_job_run_success(self, job_name):
        try:
            code = self.dboption.update_job_done(job_name)
            if code != 1:
                self.logger.error("更新Job:" + job_name + " 状态为Done失败")
            else:
                stream_jobs = self.dboption.get_stream_job(job_name)
                self.logger.info("Job:" + job_name + "触发执行的stream:" + str(stream_jobs))
                for stream_job in stream_jobs:
                    self.logger.info("处理stream_job:" + str(stream_job))
                    stream_job_name = stream_job["stream_job"]
                    etl_job = self.dboption.get_job_info(stream_job_name)
                    job_status = etl_job["job_status"]
                    if job_status == "Done":
                        code = self.dboption.update_job_pending(stream_job_name)
                        if code == 1:
                            self.logger.info(
                                    "更新Job:" + job_name + " 需要触发的stream_job:" + stream_job_name + " 状态为Pending")
                        else:
                            self.logger.error(
                                    "更新Job:" + job_name + " 需要触发的stream_job:" + stream_job_name + " 状态为Pending失败")
                    else:
                        self.logger.info(
                                "Job:" + job_name + " 出发执行的stream:" + stream_job_name + " 状态 " + job_status + " 不是Done")
        except Exception, e:
            self.logger.error(traceback.format_exc())
            self.logger.error("子进程运行Job成功,处理更新Job运行状态,触发Job异常")
            self.check_job_run_failed(job_name)

    '''
     子进程运行job失败
    '''

    def check_job_run_failed(self, job_name):
        try:
            self.dboption.update_job_failed(job_name)
            self.logger.info("更新Job:" + job_name + "运行失败,状态Failed")
            self.dboption.update_job_queue_failed(job_name)
            self.logger.info("更新Job Queue:" + job_name + " 状态为Failed")
            self.monitor.monitor(job_name)
        except Exception, e:
            self.logger.error(traceback.format_exc())
            self.logger.error("子进程运行Job:" + str(job_name) + " 运行失败,发送通知异常")

    '''
     检查进程状态
    '''

    def check_process_state(self):
        try:
            tmp_process_running = self.process_running
            for child in tmp_process_running.keys():
                file_handler = tmp_process_running[child]["logfile_handler"]
                job_name = tmp_process_running[child]["job_name"]
                pid = tmp_process_running[child]["pid"]
                return_code = child.poll()
                if return_code is not None:
                    if return_code == 0:
                        file_handler.flush()
                        file_handler.close()
                        self.logger.info("进程:" + str(pid) + " 运行code:" + str(return_code) + " job:" + job_name + " 运行成功")
                        self.check_job_run_success(job_name)
                    else:
                        self.logger.info("进程" + str(pid) + " 运行 code:" + str(return_code) + " Job:" + job_name + " 运行失败")
                        self.check_job_run_failed(job_name)
                    self.process_running.pop(child)  # 删除子进程
                else:
                    self.logger.info("child_pid:" + str(pid) + " code:" + str(return_code))
        except Exception, e:
            self.logger.error(traceback.format_exc())
            self.logger.error("关闭进程日志文件句柄错误")

    def run(self):
        run_queue_job = self.scheduler.add_job(self.run_queue_job_pending, 'interval', seconds=15)
        self.logger.info("添加处理时间触发任务:" + str([run_queue_job]))
        self.scheduler.start()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    executor = Executor()
    executor.run()
