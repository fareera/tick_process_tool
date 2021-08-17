# -*- coding: utf-8 -*-  
# -----------------------------------------------------------------------------
# File Name: save_ticks.py
# Author:    Jason. Yao
# Date:      2021/07/29
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import json

from settings import TICK_QUEUE, DBCONN, get_redis
from utils.common_utils import create_peewee_table_class

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------

peewee_class = dict()

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------

redis_conn = get_redis()
# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    while True:
        try:
            redis_conn.ping()
        except:
            redis_conn = get_redis()
            print("[*]     重新连接至redis")

        msg = redis_conn.brpop(TICK_QUEUE, 5)
        if msg is None:
            continue
        insert_data = json.loads(msg[1].decode("utf-8"))
        instrument_id_split = insert_data["instrument_id"].split(".")
        if insert_data["instrument_id"] not in peewee_class:
            peewee_class[insert_data["instrument_id"]] = create_peewee_table_class(instrument_id_split[0],
                                                                                   instrument_id_split[1])
        with DBCONN.atomic():
            peewee_class[insert_data["instrument_id"]].insert(
                insert_data
            ).execute()
