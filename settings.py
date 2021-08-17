# -*- coding: utf-8 -*-

import os

import redis
from peewee import PostgresqlDatabase

DBCONN = PostgresqlDatabase('candleline', **{
    'host': '114.96.104.203',
    'password': 'Biz@2021@#',
    'port': 5432,
    'user': 'admin'
})

TICK_QUEUE = "tick_queue"

TICK_PEEWEE_CLASS = "tick_peewee_class"

LOG_PATH = os.path.join(os.getcwd(), "logs", "tick_process_tool")


def get_redis():
    while True:
        try:
            # REDIS_CONN_POOL = redis.ConnectionPool(
            #     host='114.96.104.203',
            #     port=6379,
            #     socket_connect_timeout=10,
            #     # decode_responses=True,
            #     max_connections=100,
            #     health_check_interval=30
            # )
            REDIS_CONN_POOL = redis.ConnectionPool(
                host='localhost',
                port=6379,
                socket_connect_timeout=10,
                # decode_responses=True,
                max_connections=100,
                health_check_interval=30
            )
            redis_conn = redis.Redis(connection_pool=REDIS_CONN_POOL, decode_responses=True)
            redis_conn.ping()
        except:
            print('redis连接失败,正在尝试重连')
            continue
        else:
            return redis_conn
