import struct
from collections import namedtuple

class QuoteType(object):
    kFutureSnapshot = 0x00000008
    kOptionSnapshot = 0x00000010


class SubscribeItem(object):
    def __init__(self) -> None:
        self._quote_type = None
        self._symbol_id = None

    @property
    def quote_type(self):
        return self._quote_type

    @quote_type.setter
    def quote_type(self, value: QuoteType):
        self._quote_type = value

    @property
    def symbol_id(self):
        return self._symbol_id

    @symbol_id.setter
    def symbol_id(self, value: str):
        self._symbol_id = value

    def pack(self) -> bytes:
        return struct.pack('Q32s', self._quote_type, self._symbol_id.encode())

    @staticmethod
    def load(buffer):
        print(f'Loading SubscribeItem from buffer, {len(buffer)}')
        fmt = 'Q32s'
        si = SubscribeItem()
        si.quote_type, si.symbol_id = struct.unpack(fmt, buffer)
        return si



# Response = namedtuple('Response', [
#     'error_no',
#     'error_msg'
# ])


FutureSnapshot = namedtuple('FutureSnapshot', [
    'cn_calendar_day',
    'cn_trading_day',
    'symbol_id',
    'cn_exchange_time',
    'local_exchange_time',
    'pre_close_price',
    'pre_settelement_price',
    'open_price',
    'high_price',
    'low_price',
    'last_price',
    'settlement_price',
    'up_limit_price',
    'down_limit_price',

    'bid_px1',
    'bid_px2',
    'bid_px3',
    'bid_px4',
    'bid_px5',
    'bid_px6',
    'bid_px7',
    'bid_px8',
    'bid_px9',
    'bid_px10',
    'bid_vol1',
    'bid_vol2',
    'bid_vol3',
    'bid_vol4',
    'bid_vol5',
    'bid_vol6',
    'bid_vol7',
    'bid_vol8',
    'bid_vol9',
    'bid_vol10',
    'ask_px1',
    'ask_px2',
    'ask_px3',
    'ask_px4',
    'ask_px5',
    'ask_px6',
    'ask_px7',
    'ask_px8',
    'ask_px9',
    'ask_px10',
    'ask_vol1',
    'ask_vol2',
    'ask_vol3',
    'ask_vol4',
    'ask_vol5',
    'ask_vol6',
    'ask_vol7',
    'ask_vol8',
    'ask_vol9',
    'ask_vol10',

    'turnover',
    'volume',
    'open_interest',
])


OptionSnapshot = namedtuple('OptionSnapshot', [
    'cn_calendar_day',
    'cn_trading_day',
    'symbol_id',
    'cn_exchange_time',
    'local_exchange_time',
    'pre_close_price',
    'pre_settelement_price',
    'open_price',
    'high_price',
    'low_price',
    'last_price',
    'settlement_price',
    'up_limit_price',
    'down_limit_price',

    'bid_px1',
    'bid_px2',
    'bid_px3',
    'bid_px4',
    'bid_px5',
    'bid_px6',
    'bid_px7',
    'bid_px8',
    'bid_px9',
    'bid_px10',
    'bid_vol1',
    'bid_vol2',
    'bid_vol3',
    'bid_vol4',
    'bid_vol5',
    'bid_vol6',
    'bid_vol7',
    'bid_vol8',
    'bid_vol9',
    'bid_vol10',
    'ask_px1',
    'ask_px2',
    'ask_px3',
    'ask_px4',
    'ask_px5',
    'ask_px6',
    'ask_px7',
    'ask_px8',
    'ask_px9',
    'ask_px10',
    'ask_vol1',
    'ask_vol2',
    'ask_vol3',
    'ask_vol4',
    'ask_vol5',
    'ask_vol6',
    'ask_vol7',
    'ask_vol8',
    'ask_vol9',
    'ask_vol10',

    'delta',
    'gamma',
    'vega',
    'theta',
    'turnover',
    'volume',
    'open_interest',
    'strike_price',
    'last_exercise_day'
])