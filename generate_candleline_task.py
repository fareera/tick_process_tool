# -*- coding: utf-8 -*-  
# -----------------------------------------------------------------------------
# File Name: generate_candleline_task.py
# Author:    Jason. Yao
# Date:      2021/08/05
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
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
    """定时任务 每天15.30 聚合多周期k线"""
    aggregatecandleline = AggregateCandleline()
    tables = DBCONN.get_tables()
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


# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    scheduler.add_job(run, 'cron', day="*", hour=15, minute=30)
    scheduler.start()
