# -*- coding: utf-8 -*-  
# -----------------------------------------------------------------------------
# File Name: generate_candleline_task.py
# Author:    Jason. Yao
# Date:      2021/08/05
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import datetime
import time
from apscheduler.schedulers.blocking import BlockingScheduler

from settings import DBCONN
from utils.aggregate_different_periods_candleline import AggregateCandleline

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------

scheduler = BlockingScheduler()  # 后台运行


# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------


def run():
    aggregatecandleline = AggregateCandleline()
    # 5分钟
    tables = DBCONN.get_tables()
    start_time = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '9:30', '%Y-%m-%d%H:%M')
    end_time = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '15:30', '%Y-%m-%d%H:%M')
    # 当前时间
    now_time = datetime.datetime.now()

    # 判断当前时间是否在范围时间内
    for i in tables:
        aggregatecandleline.generate_minute_period_candleline(i,
                                                              int(str(
                                                                  time.strftime("%Y%m%d", time.localtime()))),
                                                              int(str(
                                                                  time.strftime("%Y%m%d", time.localtime()))),
                                                              2)
        aggregatecandleline.generate_minute_period_candleline(i,
                                                              int(str(
                                                                  time.strftime("%Y%m%d", time.localtime()))),
                                                              int(str(
                                                                  time.strftime("%Y%m%d", time.localtime()))),
                                                              3)
        aggregatecandleline.generate_minute_period_candleline(i,
                                                              int(str(
                                                                  time.strftime("%Y%m%d", time.localtime()))),
                                                              int(str(
                                                                  time.strftime("%Y%m%d", time.localtime()))),
                                                              4)
        aggregatecandleline.generate_minute_period_candleline(i,
                                                              int(str(
                                                                  time.strftime("%Y%m%d", time.localtime()))),
                                                              int(str(
                                                                  time.strftime("%Y%m%d", time.localtime()))),
                                                              5)


scheduler.add_job(run, 'cron', day="*", hour=15, minute=30)
scheduler.start()
# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
