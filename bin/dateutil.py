#!/usr/bin/python
# -*- coding:utf-8 -*-


import datetime


class DateUtil(object):
    @staticmethod
    def get_now():
        return datetime.datetime.now()

    @staticmethod
    def get_now_fmt(fmt):
        if fmt is None or len(fmt) == 0:
            fmt = "%Y-%m-%d"
        now_time = datetime.datetime.now()
        datestring = datetime.datetime.strftime(now_time, fmt)
        return datestring

    @staticmethod
    def get_yesterday_fmt(fmt):
        if fmt is None or len(fmt) == 0:
            fmt = "%Y-%m-%d"
        now_time = datetime.datetime.now()
        now_time = now_time + datetime.timedelta(days=-1)
        datestring = datetime.datetime.strftime(now_time, fmt)
        return datestring

    @staticmethod
    def get_interval_day_fmt(interval, fmt):
        if fmt is None or len(fmt) == 0:
            fmt = "%Y-%m-%d"
        now_time = datetime.datetime.now()
        now_time = now_time + datetime.timedelta(days=interval)
        datestring = datetime.datetime.strftime(now_time, fmt)
        return datestring

    @staticmethod
    def get_yesterday():
        now_time = datetime.datetime.now()
        now_time = now_time + datetime.timedelta(days=-1)
        datestring = datetime.datetime.strftime(now_time, '%Y%m%d')
        return datestring

    @staticmethod
    def get_today():
        now_time = datetime.datetime.now()
        datestring = datetime.datetime.strftime(now_time, '%Y%m%d')
        return datestring

    @staticmethod
    def get_today_with_mode(mode):
        now_time = datetime.datetime.now()
        datestring = datetime.datetime.strftime(now_time, mode)
        return datestring

    @staticmethod
    def get_timestamp():
        now_time = datetime.datetime.now()
        return now_time

    @staticmethod
    def format_year_second(time):
        return datetime.datetime.strftime(time, '%Y-%m-%d %H:%M:%S')

    @staticmethod
    def format_year_minute(time):
        return datetime.datetime.strftime(time, '%Y-%m-%d %H:%M')

    @staticmethod
    def format_year_day(time):
        return datetime.datetime.strftime(time, '%Y-%m-%d')

    @staticmethod
    def get_time_day(time):
        return time.day

    @staticmethod
    def get_time_hour(time):
        return time.hour

    @staticmethod
    def get_time_minute(time):
        return time.minute

    @staticmethod
    def get_next_run_time(time, interval):
        return time + datetime.timedelta(seconds=interval)

    @staticmethod
    def parse_timestring(time_string):
        return datetime.datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_week_day(time):
        return time.weekday() + 1

    @staticmethod
    def get_last_month():
        today = datetime.date.today()
        first = today.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)
        return lastMonth.strftime("%Y-%m")

    @staticmethod
    def get_current_month():
        today = datetime.date.today()
        return today.strftime("%Y-%m")

    @staticmethod
    def get_yesterday_month():
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        return yesterday.strftime("%Y-%m")

    @staticmethod
    def get_current_month_first_day():
        today = datetime.date.today()
        current_month = today.strftime("%Y-%m")
        return current_month + "-01"

if __name__ == '__main__':
    now_time = datetime.datetime.now()
    print DateUtil.get_last_month()
