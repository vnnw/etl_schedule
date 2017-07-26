#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys
from dateutil import DateUtil
from logutil import Logger
from dboption import DBOption
from configutil import ConfigUtil
import time

from apscheduler.schedulers.blocking import BlockingScheduler

'''
1. 每分钟运行一次,判断job配置确定是否将 job 状态置为pending
2. 判断当前运行的Job 数量,插入Pending 状态的Job 到 queue 中
'''


class Scheduler(object):
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.logger = Logger("scheduler").getlog()
        self.aps_log = Logger("apscheduler").getlog()
        self.dboption = DBOption()
        self.config = ConfigUtil()

    def get_timetrigger_job(self, current):
        dstring = DateUtil.format_year_second(current)
        day = DateUtil.get_time_day(current)
        hour = DateUtil.get_time_hour(current)
        minute = DateUtil.get_time_minute(current)
        week_day = DateUtil.get_week_day(current)
        self.logger.info("获取start_hour:" + str(hour) + " start_minute:" + str(minute) + " 运行的 Job")
        time_jobs = self.dboption.get_time_trigger_job(hour, minute)
        if time_jobs is None or len(time_jobs) == 0:
            self.logger.info(dstring + " 没有需要运行的时间触发Job")
            return
        else:
            try:
                for job in time_jobs:
                    job_name = job["job_name"]
                    trigger_type = job["trigger_type"]
                    record = 0
                    should_run = False
                    if trigger_type == "day":  # 每天运行
                        should_run = True
                    elif trigger_type == "month":  # 每月运行
                        start_day = job["start_day"]
                        if int(start_day) == day:
                            should_run = True
                    elif trigger_type == "week":  # 每周运行
                        start_day = job["start_day"]
                        if int(start_day) == week_day:
                            should_run = True

                    if should_run:
                        record = self.dboption.update_trigger_job_pending(current, job_name)
                        if record == 1:
                            self.logger.info("更新时间触发Job:" + job_name + " 状态为Pending")
                        else:
                            self.logger.error("更新时间触发Job :" + job_name + " 状态为Pending失败")
                    else:
                        self.logger.info("时间触发 Job:" + job_name + " 没有对应时间触发执行方式 trigger_type:" + str(trigger_type))

            except Exception, e:
                self.logger.error(e)
                self.logger.error("处理时间触发Job异常")

    def sched_trigger_run(self, current_time = None):
        self.logger.info("... interval run sched_trigger_run ....")
        start_time = DateUtil.get_now()
        self.get_timetrigger_job(start_time)
        end_time = DateUtil.get_now()
        self.logger.info("时间触发调度运行时长:" + str(end_time - start_time))

    def sched_job_run(self):
        self.logger.info("... interval run sched_job_run ....")
        start_time = DateUtil.get_now()
        # 当前运行的任务数
        max_running_jobs = int(self.config.get("job.run.max"))
        running_jobs = self.dboption.get_running_jobs()
        if running_jobs is not None:
            count_running_jobs = len(running_jobs)
            if count_running_jobs > max_running_jobs:
                self.logger.info("当前运行的Job 数量:" + str(count_running_jobs) + "大于系统的最大任务数:" + str(max_running_jobs))
                return
        else:
            count_running_jobs = 0

        self.logger.info(str(start_time) + " 当前RUNNING状态的Job 数量:" + str(count_running_jobs))

        pending_jobs = self.dboption.get_pending_jobs()
        if pending_jobs is None or len(pending_jobs) == 0:
            count_pending_jobs = 0
            self.logger.info("当前Pending状态的 Job 数量:" + str(count_pending_jobs))
            return
        else:
            count_pending_jobs = len(pending_jobs)
            self.logger.info("当前Pending状态的 Job 数量:" + str(count_pending_jobs))

        require_jobs_count = max_running_jobs - count_running_jobs
        if require_jobs_count > 0:
            should_require_jobs = set()
            require_time = 0
            should_continue = True
            while True:
                if not should_continue:
                    break
                if len(should_require_jobs) == require_jobs_count:
                    break
                if require_time > 10:  # 强制的判断方式有点问题,判断10次
                    break
                self.logger.info("第" + str(require_time) + " 次运行")
                self.logger.info("可以运行的job数量:" + str(len(should_require_jobs)) + " 需要运行的job数量:" + str(require_jobs_count))
                pending_jobs = self.dboption.get_pending_jobs_by_require_time(require_time, max_running_jobs)
                for job in pending_jobs:
                    job_name = job["job_name"]
                    self.logger.info("判断Job:" + job_name + "依赖是否全部运行完成")
                    should_run = self.reslove_job_dep(job)
                    if should_run:  # FixMe 需要判断今天是否运行过
                        self.logger.info("job:" + job_name + " 依赖全部运行完成添加到可以运行的job列表中")
                        should_require_jobs.add(job_name)
                        if len(should_require_jobs) == require_jobs_count:
                            should_continue = False
                            break
                if pending_jobs is None or len(pending_jobs) < require_jobs_count:
                    self.logger.info("当前Pending状态的 Job 数量 " + str(self.get_list_length(pending_jobs)) +
                                     " 小于 需要运行的任务数 " + str(require_jobs_count) +",无需循环")
                    should_continue = False

                require_time += 1
            self.logger.info("需要运行的Job:" + str(should_require_jobs))
            self.run_job_command(should_require_jobs)
            end_time = DateUtil.get_now()
            self.logger.info("依赖触发调度执行时长:" + str(end_time - start_time))
        else:
            self.logger.info("当前运行的Job 数量:" + str(count_running_jobs) + "大于系统的最大任务数")

    def get_list_length(self, objs):
        if objs is None:
            return 0
        else:
            return len(objs)

    '''
     判断job的所有依赖是否运行完成 True/False
    '''

    def reslove_job_dep(self, job):
        today = DateUtil.format_year_day(DateUtil.get_now())
        job_name = job["job_name"]
        dep_jobs = self.dboption.get_dependency_job_status(job_name)  # job_name,status
        self.logger.info("job_name:" + str(job_name) + " 依赖的 job数量:" + str(self.get_list_length(dep_jobs)))
        should_run = True
        for dep_job in dep_jobs:
            dep_job_name = dep_job["job_name"]
            dep_job_status = dep_job["job_status"]
            dep_job_last_run_date = dep_job["last_run_date"]  # 最后一次运行日期
            self.logger.info("job_name:" + job_name + " 依赖的Job:" + dep_job_name + " 运行状态:" + str(dep_job_status)
                             + " 最后运行时间:" + str(dep_job_last_run_date))
            if (dep_job_last_run_date and dep_job_last_run_date != today) or dep_job_last_run_date is None:
                should_run = False
                self.logger.info("因Job:" + job_name + " 依赖Job:" + dep_job_name +
                                 "最后运行时间:" + str(dep_job_last_run_date) + "不是:" + today + ",无法运行")
                break
            if dep_job_status != "Done":
                should_run = False
                self.logger.info("因Job:" + job_name + " 依赖Job:"
                                 + dep_job_name + " 没有运行完成状态:" + dep_job_status + ",无法运行")
                break
        return should_run

    '''
      保存记录到 t_etl_job_queue
    '''

    def run_job_command(self, should_run_jobs):
        if should_run_jobs is None or len(should_run_jobs) == 0:
            self.logger.info("需要保存到 t_etl_job_queue 等待运行的Job数量为 0")
        else:
            for job_name in should_run_jobs:
                queue_job = self.dboption.get_queue_job(job_name)
                if queue_job is None:
                    code = self.dboption.save_job_queue(job_name)
                    if code == 1:
                        self.logger.info("保存Job:" + job_name + " 到t_etl_job_queue 等待运行")
                    else:
                        self.logger.error("保存Job:" + job_name + " 到t_etl_job_queue 失败")
                else:
                    # 失败的任务,如何处理?
                    self.logger.info("Job:" + job_name + " 已经保存到 t_tel_job_queue 中,等待执行运行")

    '''
     启动执行
    '''

    def run(self):
        current_time = DateUtil.get_now()
        second = current_time.second
        time.sleep(60 - second)
        trigger_job = self.scheduler.add_job(self.sched_trigger_run, 'interval', minutes=1)
        self.logger.info("添加处理时间触发任务:" + str(trigger_job))
        run_job = self.scheduler.add_job(self.sched_job_run, 'interval', seconds=30)
        self.logger.info("添加处理Job运行任务" + str(run_job))
        self.scheduler.start()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    scheduler = Scheduler()
    scheduler.run()
    print("scheduler shutdown....")
