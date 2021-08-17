import datetime
import json
import time
from decimal import Decimal

from settings import TICK_QUEUE, get_redis
from utils.common_utils import create_tables, create_peewee_table_class, DecimalEncoder

redis_conn = get_redis()


def get_ts(tradedate, tradetime):
    first_number = int(str(tradetime)[0])
    if first_number > 1:
        tradetime = "0{}".format(tradetime)
    last_fmt_trade_date = datetime.datetime.strptime("{}{}".format(tradedate, str(tradetime)[0:4]),
                                                     "%Y%m%d%H%M").timetuple()
    timeStamp = int(time.mktime(last_fmt_trade_date))
    return timeStamp


def timestamp_to_str(timestamp=None, format='%H%M'):
    if timestamp:
        time_tuple = time.localtime(timestamp)  # 把时间戳转换成时间元祖
        result = time.strftime(format, time_tuple)  # 把时间元祖转换成格式化好的时间
        return result
    else:
        return time.strptime(format)


class TickEngine(object):

    def __init__(self, instrument_id):
        self.first_volume = None
        self.close_price = None
        self.exchange_id = None
        self.highest_price = None
        self.instrument_id = instrument_id
        self.lowest_price = None
        self.open_price = None
        self.start_time = None
        self.stop_time = None
        self.trading_day = None
        self.update_time = None
        self.last_trade_day = None
        self.first_turnover = None
        self.security_id = None
        self.seq = 0
        self.settlement_price = None

    def init_params(self, msg):
        self.first_volume = msg["volume"]
        self.close_price = (Decimal(msg["last_price"]))
        self.highest_price = (Decimal(msg["last_price"]))
        self.lowest_price = (Decimal(msg["last_price"]))
        self.open_price = (Decimal(msg["last_price"]))
        self.start_time = msg["exchange_time"]
        self.stop_time = msg["exchange_time"]
        self.trading_day = msg["trading_day"]
        self.turnover = None
        self.last_trade_day = msg["trading_day"]
        self.first_turnover = msg["turnover"]
        self.exchange_id = msg["exchange_id"]
        self.initial_instrument_id = msg["security_id"]
        self.seq = self.seq + 1

    def next_day_check(self, tradedate):
        # 是否跨天
        if self.last_trade_day != tradedate:
            self.seq = 0
            return True
        else:
            return False

    def first_trade_check(self):
        # 是否是第一笔交易
        if self.first_volume is None:
            return True
        else:
            return False

    def next_minute_tick_check(self, tradedate, exchange_time):
        # 是否是下一分钟的行情
        last_trade_ts = get_ts(self.last_trade_day, self.stop_time)
        trading_ts = get_ts(tradedate, exchange_time)
        if (trading_ts != last_trade_ts):
            return True
        else:
            return False

    def minute_tick_check(self, tradedate, exchange_time):
        last_trade_ts = get_ts(self.last_trade_day, self.stop_time)
        trading_ts = get_ts(tradedate, exchange_time)
        if last_trade_ts == trading_ts:
            return True
        else:
            return False

    def check_trash_data(self, start_ts):
        limit_time_1 = datetime.datetime.strptime(str(time.strftime("%Y-%m-%d", time.localtime(start_ts))) + '09:29',
                                                  '%Y-%m-%d%H:%M')
        limit_time_2 = datetime.datetime.strptime(str(time.strftime("%Y-%m-%d", time.localtime(start_ts))) + '15:05',
                                                  '%Y-%m-%d%H:%M')
        start_time = datetime.datetime.strptime(str(time.strftime("%Y-%m-%d%H:%M", time.localtime(start_ts))),
                                                '%Y-%m-%d%H:%M')

        if limit_time_1 <= start_time <= limit_time_2:
            return True
        else:
            return False

    def save_kline(self, last_turnover, volume):
        """保存分钟k线"""
        start_time_ts = get_ts(self.trading_day, self.start_time)
        if self.check_trash_data(start_time_ts):
            global redis_conn
            try:
                redis_conn.ping()
            except:
                redis_conn = get_redis()
                print("[*]      重新连接至redis")
            redis_conn.lpush(TICK_QUEUE, *[
                json.dumps({

                    "local_exchange_id": self.exchange_id,
                    # "backward_adjusted_open_price":backward_adjusted_open_price,
                    # "backward_adjusted_close_price":backward_adjusted_close_price,
                    # "backward_adjusted_highest_price":backward_adjusted_highest_price,
                    # "backward_adjusted_lowest_price":backward_adjusted_lowest_price,
                    # "backward_adjusted_settlement_price":backward_adjusted_settlement_price,
                    "close_price": (Decimal(self.close_price)),
                    "exchange_id": self.exchange_id,
                    "highest_price": (Decimal(self.highest_price)),
                    "instrument_id": self.instrument_id,
                    "lowest_price": (Decimal(self.lowest_price)),
                    "open_price": (Decimal(self.open_price)),
                    # "seq": self.seq,
                    "start_time": timestamp_to_str(timestamp=start_time_ts),
                    "stop_time": timestamp_to_str(timestamp=start_time_ts + 60),
                    "tradingday": int(self.trading_day),
                    "period": "1",
                    # "turnover": Decimal(last_turnover) - Decimal(self.first_turnover),
                    # "update_time": datetime.datetime.now(),
                    # "settlement_price": self.settlement_price,
                    # "total_open_interest": self.total_open_interest,
                    # "volume": int(Decimal(volume)) - int(Decimal(self.first_volume)),
                }, cls=DecimalEncoder)
            ])
            # with DBCONN.atomic():
            #     peewee_obj = peewee_class[self.instrument_id]
            #     # 1分钟k线插入数据库
            #     peewee_obj.insert(
            #         {
            #
            #             "local_exchange_id": self.exchange_id,
            #             # "backward_adjusted_open_price":backward_adjusted_open_price,
            #             # "backward_adjusted_close_price":backward_adjusted_close_price,
            #             # "backward_adjusted_highest_price":backward_adjusted_highest_price,
            #             # "backward_adjusted_lowest_price":backward_adjusted_lowest_price,
            #             # "backward_adjusted_settlement_price":backward_adjusted_settlement_price,
            #             "close_price": (Decimal(self.close_price)),
            #             "exchange_id": self.exchange_id,
            #             "highest_price": (Decimal(self.highest_price)),
            #             "instrument_id": self.instrument_id,
            #             "lowest_price": (Decimal(self.lowest_price)),
            #             "open_price": (Decimal(self.open_price)),
            #             # "seq": self.seq,
            #             "start_time": timestamp_to_str(timestamp=start_time_ts),
            #             "stop_time": timestamp_to_str(timestamp=start_time_ts + 60),
            #             "tradingday": int(self.trading_day),
            #             "period": "1",
            #             # "turnover": Decimal(last_turnover) - Decimal(self.first_turnover),
            #             "update_time": datetime.datetime.now(),
            #             # "settlement_price": self.settlement_price,
            #             # "total_open_interest": self.total_open_interest,
            #             # "volume": int(Decimal(volume)) - int(Decimal(self.first_volume)),
            #         }
            #     ).execute()

    def update_params(self, close_price, last_px,
                      trade_time):
        self.stop_time = trade_time
        if (Decimal(last_px)) > self.highest_price:
            self.highest_price = (Decimal(last_px))
        elif (Decimal(last_px)) < self.lowest_price:
            self.lowest_price = (Decimal(last_px))
        self.close_price = last_px

    def handle_tick(self, tick):
        if self.first_trade_check():
            print(f"////////////////////////////////////////////////[{self.instrument_id}],正在初始化行情")
            self.init_params(msg=tick)
        else:
            if self.next_minute_tick_check(tick["trading_day"], tick["exchange_time"]):
                print("////////////////////////////////////////////////下一分钟行情,保存k线!")
                self.save_kline(tick["turnover"], tick["volume"])
                self.init_params(msg=tick)

            # 判断是否都是此分钟内的行情
            elif self.minute_tick_check(tick["trading_day"], tick["exchange_time"]):
                print("////////////////////////////////////////////////此分钟行情,更新参数!")
                self.update_params(tick["pre_close_price"], tick["last_price"],
                                   tick["exchange_time"]
                                   )


class EngineManager(object):
    """Doc."""
    __instance = None
    __Engine = {}

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(EngineManager, cls).__new__(cls, *args, **kwargs)
        return cls.__instance

    def __getitem__(self, instrument_id):
        return self.__Engine[instrument_id]

    def addEngine(self, exchange_id, security_id):
        exchange_id = exchange_id.lower()
        instrument_id = "{}.{}".format(exchange_id, security_id)
        if instrument_id not in self.__Engine:
            peewee_class = create_peewee_table_class(exchange_id, security_id)
            create_tables(peewee_class, "{}".format(instrument_id))
            self.__Engine[instrument_id] = TickEngine(instrument_id)

    def removeEngine(self, instrument_id):
        del self.__Engine[instrument_id]
