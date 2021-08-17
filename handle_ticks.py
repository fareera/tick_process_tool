# -*- coding: utf-8 -*-  
# -----------------------------------------------------------------------------
# File Name: handle_ticks.py
# Author:    Jason. Yao
# Date:      2021/07/12
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import datetime
import threading
import time
from threading import Thread

from settings import DBCONN
from utils.aggregate_different_periods_candleline import AggregateCandleline
from utils.common_utils import stk_names, read_csv
from utils.engine import EngineManager

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------
ENGINE_MANAGER = None
csv_generator = None
lock = threading.Lock()


# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
def handle_tick_file():
    while True:
        try:
            # lock.acquire()
            data = next(csv_generator)
            # lock.release()
            exchange_id = data["exchange_id"].lower().strip()
            security_id = str(data["security_id"]).strip().rjust(6, '0')
            # if "{}{}".format(exchange_id, security_id).lower() == "sh000001":
            #     with open("{}{}.csv".format(exchange_id, securi`ty_id).lower(), "a", encoding="utf-8") as f:
            #         f.write(",".join([str(i) for i in list(data.values())]) + " \n")
            data["exchange_time"] = data["updateTime"][0:5].replace(":", "")
            ENGINE_MANAGER.addEngine(exchange_id, security_id)
            ENGINE_MANAGER["{}{}".format(exchange_id, security_id)].handle_tick(data)
        except StopIteration:
            print(">>>>行情处理结束")
            break


def run(thread_number):
    thread_list = []
    for i in range(1, thread_number):
        t = handleTickThred()
        thread_list.append(t)
        t.start()

    for t in thread_list:
        t.join()


# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------

class handleTickThred(Thread):
    def run(self):
        handle_tick_file()

    def __init__(self):
        Thread.__init__(self)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    # csv_generator = read_csv(r"D:\ipaylinks_project\tick_process_tool\sh000001.csv", stk_names)
    # ENGINE_MANAGER = EngineManager()
    # start_time = time.time()
    # handle_tick_file()
    # run(14)
    # 生成不同周期k线
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
