# -----------------------------------------------------------------------------
# File Name: aggregate_different_periods_candleline.py
# Copyright: 2020.03 Shanghai Mowen Technology Co., Ltd
# Author:    Jason. Yao
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import datetime
import time

import pandas as pd
from settings import DBCONN
from utils.common_utils import create_peewee_table_class
from utils.wrappers import clsSingleton

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------


period_dict = {
    1: "1min",
    2: "5min",
    3: "15min",
    4: "30min",
    5: "60min",
}

CN_FUTURE_EXCHANGES = ('cffex', 'shfe', 'dce', 'czce', "ine",
                       "nib")


# ---------------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------
@clsSingleton
class AggregateCandleline(object):
    def __init__(self):
        # 字段对应的计算方式
        self.agg_map = {
            'tradingday': 'first',
            'exchange_id': 'first',
            'instrument_id': 'first',
            'local_exchange_id': 'first',
            'open_price': 'first',
            'close_price': 'last',
            'highest_price': 'max',
            'lowest_price': 'min',
            'start_time': 'first',
            'stop_time': 'last',
        }

    # 获取一分钟k线并转换为dataframe
    def get_one_min_candleline_df(self, tablename, start_date, end_date):
        tablecls = create_peewee_table_class("_", "_", table_name=tablename)
        query_res = tablecls.select().where(
            (tablecls.tradingday >= start_date) & (tablecls.tradingday <= end_date) & (tablecls.period == "1"))
        if query_res:
            query_res = query_res.dicts()
            df = pd.DataFrame()
            for i in query_res:
                df = df.append(i, ignore_index=True)
            # 将tradedate与startdate组合成为dataframe的index
            df = df.set_index(pd.DatetimeIndex(
                pd.to_datetime(df["tradingday"].apply(int).apply(str) + (df["start_time"]), format="%Y%m%d%H%M")))
            return df
        else:
            return None

    def save_candleline(self, tablename, candleline_list):
        tablecls = create_peewee_table_class("_", "_", table_name=tablename)
        with DBCONN.atomic():
            for i in candleline_list:
                is_exites = tablecls.select(tablecls.start_time).where(
                    (
                            tablecls.tradingday == i["tradingday"]
                    ) & (
                            tablecls.instrument_id == i["instrument_id"]
                    ) & (
                            tablecls.exchange_id == i["exchange_id"]
                    ) & (
                            tablecls.period == i["period"]
                    ) & (
                            tablecls.period == i["start_time"]
                    ) & (
                            tablecls.period == i["stop_time"]
                    )
                ).count()
                print(is_exites,i)
                if is_exites:
                    tablecls.update(i).execute()
                else:
                    tablecls.insert(i).execute()

    # 生成不同周期的K线
    def generate_minute_period_candleline(self, tablename, start_date, end_date, period):
        map_period = period_dict[period]
        is_stock_table = True
        # 判断生成的k线是股票还是期货
        for cn_future_exchange_id in CN_FUTURE_EXCHANGES:
            if cn_future_exchange_id in tablename:
                is_stock_table = False
        if is_stock_table:
            candleline_df = self.get_one_min_candleline_df(tablename, start_date, end_date)
            # 根据不同的period聚合不同周期的k线
            if candleline_df is not None:
                candleline_df = candleline_df.resample(f"{map_period}").agg(
                    self.agg_map
                )
                candleline_list = []
                count = 1
                candleline_df = candleline_df.dropna(axis=0, how='any')
                for i in range(len(candleline_df)):
                    candleline_dict = candleline_df.iloc[i].to_dict()
                    candleline_dict['update_time'] = datetime.datetime.now()
                    # candleline_dict['seq'] = count
                    candleline_dict['period'] = str(period)
                    count += 1
                    candleline_list.append(candleline_dict)
                self.save_candleline(tablename, candleline_list)
        else:
            pass


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    pass
