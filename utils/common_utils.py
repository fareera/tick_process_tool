# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# File Name: common_utils.py
# Author:    Jason. Yao
# Date:      2021/07/06
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import datetime
import decimal
import json

import pandas as pd
from peewee import (
    Model,
    CharField,
    IntegerField,
    DateTimeField,
    DecimalField
)
from settings import DBCONN

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------
from utils import EXITS_TABLE_LIST

# stk_names = ["peRadio1", "peRadio2", "securityID", "securityName", "tradingDay", "exchangeID", "preClosePrice",
#              "openPrice", "upperLimitPrice", "lowerLimitPrice", "latestPrice", "turnover", "highestPrice",
#              "lowestPrice", "tradeQty", "updateTime", "bidPrice1", "bidQty1", "askPrice1", "askQty1", "bidPrice2",
#              "bidQty2", "askPrice2", "askQty2", "bidPrice3", "bidQty3", "askPrice3", "askQty3", "bidPrice4", "bidQty4",
#              "askPrice4", "askQty4", "bidPrice5", "bidQty5", "askPrice5", "askQty5", "tradingPhaseCode"]
stk_names = ["peRadio1", "peRadio2", "security_id", "securityName", "trading_day", "exchange_id", "pre_close_price",
             "openPrice", "upperLimitPrice", "lowerLimitPrice", "last_price", "turnover", "highestPrice",
             "lowestPrice", "volume", "updateTime", "bidPrice1", "bidQty1", "askPrice1", "askQty1", "bidPrice2",
             "bidQty2", "askPrice2", "askQty2", "bidPrice3", "bidQty3", "askPrice3", "askQty3", "bidPrice4", "bidQty4",
             "askPrice4", "askQty4", "bidPrice5", "bidQty5", "askPrice5", "askQty5", "tradingPhaseCode"]
stk_opt_names = ["auctionPrice", "contractID", "execPrice", "latestEnquiryTime", "positionQty", "preSettlePrice",
                 "settlePrice", "securityID", "securityName", "tradingDay", "exchangeID", "preClosePrice", "openPrice",
                 "upperLimitPrice", "lowerLimitPrice", "latestPrice", "turnover", "highestPrice", "lowestPrice",
                 "tradeQty", "updateTime", "bidPrice1", "bidQty1", "askPrice1", "askQty1", "bidPrice2", "bidQty2",
                 "askPrice2", "askQty2", "bidPrice3", "bidQty3", "askPrice3", "askQty3", "bidPrice4", "bidQty4",
                 "askPrice4", "askQty4", "bidPrice5", "bidQty5", "askPrice5", "askQty5", "tradingPhaseCode"]
peewee_class = {}


# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
def create_peewee_table_class(exchange_id: str, security_id: str, table_name=None) -> type:
    # 生成peewee orm 实例
    exchange_id = exchange_id.lower()
    if table_name is not None:
        meta_cls = type('Meta', (), {
            'primary_key': False,
            'database': DBCONN,
            'table_name': table_name,
        })

    else:
        meta_cls = type('Meta', (), {
            'primary_key': False,
            'database': DBCONN,
            'table_name': f'{exchange_id}.{security_id}',
        })
    fields = {
        'tradingday': IntegerField(null=False),
        'instrument_id': CharField(max_length=32, null=False),
        'exchange_id': CharField(max_length=32, null=False),
        'local_exchange_id': CharField(max_length=32, null=False),
        'period': CharField(max_length=1, null=False),
        'open_price': DecimalField(max_digits=15, decimal_places=4),
        'close_price': DecimalField(max_digits=15, decimal_places=4),
        'highest_price': DecimalField(max_digits=15, decimal_places=4),
        'lowest_price': DecimalField(max_digits=15, decimal_places=4),
        'backward_adjusted_open_price': DecimalField(max_digits=15, decimal_places=4, null=True),
        'backward_adjusted_close_price': DecimalField(max_digits=15, decimal_places=4, null=True),
        'backward_adjusted_highest_price': DecimalField(max_digits=15, decimal_places=4, null=True),
        'backward_adjusted_lowest_price': DecimalField(max_digits=15, decimal_places=4, null=True),
        'backward_adjusted_settlement_price': DecimalField(max_digits=15, decimal_places=4, null=True),
        'start_time': CharField(max_length=10, null=False),
        'stop_time': CharField(max_length=10, null=False),
        'create_time': DateTimeField(default=datetime.datetime.now),
        'update_time': DateTimeField(default=datetime.datetime.now),
        'Meta': meta_cls
    }
    return type(f'{exchange_id}{security_id}', (Model,), fields) if table_name is None else type(table_name, (Model,),
                                                                                                 fields)


def create_tables(peewee_table_class, tablename):
    with DBCONN.atomic():
        # 判断表是否存在不存在自动创建
        if tablename not in EXITS_TABLE_LIST:
            DBCONN.create_tables([peewee_table_class])
            peewee_class[tablename] = peewee_table_class
        else:
            if tablename not in peewee_class:
                peewee_class[tablename] = peewee_table_class


"""
['0', '510050C2109M03400  ', '3.4', '', '6020', '0.2194', '0', '10003205', '50ETF购9月3400      ', '20210628', 'SH', '0.2194', '0', '0.5735', '0.0001', '0', '0', '0', '0', '0', '08:57:52.826', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '\n']

"""


def read_csv(file_path, names):
    # 分批取出 tick 每批 1000
    df = pd.read_csv(file_path, encoding="gbk", chunksize=1000, header=None, names=names)
    for chunk in df:
        for idx, row in chunk.iterrows():
            yield row.to_dict()


# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------
"""
{
    'peRadio1': 0, 'peRadio2': 0, 'securityID': 1, 'securityName': '上证指数', 'tradingDay': 20210628,
    'exchangeID': 'SH', 'preClosePrice': 3607.56, 'openPrice': 0, 'upperLimitPrice': 0, 'lowerLimitPrice': 0,
    'latestPrice': 3607.1, 'turnover': 0, 'highestPrice': 0, 'lowestPrice': 0, 'tradeQty': 0,
    'updateTime': '08:57:21.070', 'bidPrice1': 0, 'bidQty1': 0, 'askPrice1': 0, 'askQty1': 0, 'bidPrice2': 0,
    'bidQty2': 0, 'askPrice2': 0, 'askQty2': 0, 'bidPrice3': 0, 'bidQty3': 0, 'askPrice3': 0, 'askQty3': 0,
    'bidPrice4': 0, 'bidQty4': 0, 'askPrice4': 0, 'askQty4': 0, 'bidPrice5': 0, 'bidQty5': 0, 'askPrice5': 0,
    'askQty5': 0, 'tradingPhaseCode': None
}
"""


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        super(DecimalEncoder, self).default(o)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    csv_generator = read_csv(r"C:\Users\76754\Desktop\stk_20210628.csv", stk_names)
    # read_csv(r"C:\Users\76754\Desktop\stk_opt_20210628.csv",stk_opt_names)
