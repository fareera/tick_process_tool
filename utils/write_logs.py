# -*- coding: utf8 -*-
"""
recode log
"""
import datetime
import json
import logging
from logging.handlers import RotatingFileHandler
import time
import os, sys

from settings import LOG_PATH


class WriteLogs(object):
    def __init__(self):
        self.yes_time = datetime.datetime.now() + datetime.timedelta(days=-1)
        self.yesterday_time = self.yes_time.strftime('%Y%m%d')  # 格式化输出
        self.yest_times = self.yes_time.strftime('%Y-%m-%d')

    def write_logs(self, **kwargs):
        """
        写入日志
        :param step_name: 运行步骤名称
        :param logs_stauts: 运行状态
        :param logs_describe: 运行备注
        :return:
        """
        fpath = LOG_PATH
        if not os.path.exists(fpath):
            try:
                os.makedirs(fpath)  # 创建路径
            except:
                fpath = os.getcwd()
                fpath = fpath.split("tick_process_tool")[0]
                fpath = os.path.join(fpath, "tick_process_tool", "log")
                if not os.path.exists(fpath):
                    os.makedirs(fpath)  # 创建路径
        # 调用该方法运行步骤的时间
        try:
            raise Exception
        except:
            f = sys.exc_info()[2].tb_frame.f_back
        parse_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        kwargs.setdefault("timestamp", parse_time)
        # kwargs.setdefault("timestamp", str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        kwargs.setdefault("lineno", f.f_lineno)
        kwargs.setdefault("filename", f.f_code.co_filename)
        kwargs.setdefault("project_name", "tick_process_tool")
        if kwargs.get("level") == "debug":
            log_path = os.path.join(fpath, "debug.log")
        elif kwargs.get("level") == "info":
            log_path = os.path.join(fpath, "info.log")
        elif kwargs.get("level") == "warn":
            log_path = os.path.join(fpath, "warn.log")
        elif kwargs.get("level") == "error":
            log_path = os.path.join(fpath, "error.log")
        with open(log_path, 'a+', encoding='utf-8') as log_file:
            string = json.dumps(kwargs, ensure_ascii=False)
            log_file.write(string + '\n')
