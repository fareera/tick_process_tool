import abc
import asyncio
import datetime
import logging
import re

from pyquote_type import FutureSnapshot, OptionSnapshot, QuoteType
import struct
import threading

from utils.engine import EngineManager


class MWTCPProtocol(asyncio.Protocol, metaclass=abc.ABCMeta):
    """Doc."""

    def __init__(self):
        """Doc."""
        self._phb = 8
        self._header = bytearray()
        self._body = bytearray()
        self._pkg_len = 0
        self._buffer = bytearray()
        self._local_ip = None
        self._local_port = None
        self._remote_ip = None
        self._remote_port = None
        self._peer_name = None
        self._transport = None

        self.__logger = logging.getLogger('asyncio')

    def getPeerName(self):
        """Doc."""
        return self._peer_name

    def getTransport(self):
        """Doc."""
        return self._transport

    def reset(self):
        """Doc."""
        self._header.clear()
        self._body.clear()
        self._pkg_len = 0

    def connection_made(self, transport):
        """Doc."""
        self.__logger.debug('New connection made!')
        self.__logger.debug(transport.get_extra_info('peername'))
        self.__logger.debug(transport.get_extra_info('sockname'))

        self._transport = transport

        self._remote_ip, self._remote_port = transport.get_extra_info(
            'peername'
        )

        self._local_ip, self._local_port = transport.get_extra_info(
            'sockname'
        )

        self._peer_name = f'{self._remote_ip}:{self._remote_port}'

        self.onConnected()

    def connection_lost(self, exc):
        """
        Called when the connection is lost or closed.

        The argument is either an exception object or None. The latter means a
        regular EOF is received, or the connection was aborted or closed by
        this side of the connection.
        """
        self.__logger.debug(
            f'Connection lost: {self._remote_ip}:{self._remote_port}'
        )
        if exc:
            self.__logger.debug('Connection was closed by this side!')

        self.onDisconnected(0)

    def parseBytes(self, bdata):
        # 传入的字符不满足一个完整包头
        if len(bdata) < self._phb:
            return None

        msg_type_id, pkg_len = struct.unpack('II', bdata[0:self._phb])

        # 传入的字节不满足一个完整包
        if len(bdata) < pkg_len:
            return None

        self.onMessage(msg_type_id, bdata[self._phb:pkg_len])
        return bdata[pkg_len:]

    def data_received(self, bdata):
        """Doc."""
        self._buffer.extend(bdata)

        res = self.parseBytes(self._buffer)

        while res is not None:
            self._buffer.clear()
            self._buffer.extend(res)
            res = self.parseBytes(self._buffer)

        # print('本次循环后buffer长度为：{}'.format(len(self._buffer)))

    def eof_received(self):
        """
        start -> connection_made
            [-> data_received]*
            [-> eof_received]?
        -> connection_lost -> end.
        """
        self.__logger.debug("eof_received")

    @abc.abstractmethod
    def onConnected(self):
        pass

    @abc.abstractmethod
    def onDisconnected(self, reason):
        pass

    @abc.abstractmethod
    def onMessage(self, msg_type_id, msg):
        pass


class Client(MWTCPProtocol):
    """Doc."""

    def __init__(self, lsquote_api, conn_lost_future):
        super().__init__()
        self.lsquote_api = lsquote_api
        self.conn_lost_future = conn_lost_future

    def onMessage(self, msg_type_id, msg):
        self.lsquote_api._on_message(msg_type_id, msg)

    def onConnected(self):
        print(f'COnnected to server!')

    def onDisconnected(self, reason):
        self.conn_lost_future.set_result(True)


class LSQuoteSpi(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def on_quote(self, quote_type, quote):
        pass


class LSQuoteApi(threading.Thread):
    def __init__(self, ip, port):
        super().__init__(daemon=True)
        self.ip = ip
        self.port = port
        self.loop = asyncio.get_event_loop()
        self.loop.set_debug(True)
        self.quote_cache = {}

    def get_quote_cache(self, symbol_id: str):
        if symbol_id in self.quote_cache:
            return self.quote_cache[symbol_id]

        return None

    def _on_message(self, msg_type_id, msg):
        # size 472 - future
        # size 516 - option
        # size 449 - stock

        if 0x00000001 == msg_type_id:
            fmt = 'ii32sQQddddddddd10d10Q10d10QdQq'
            fs = FutureSnapshot._make(struct.unpack(fmt, msg))
            symbol_id = fs.symbol_id[0:fs.symbol_id.find(b'\x00')].decode()
            fs = fs._replace(symbol_id=symbol_id)

            # Locally cache the quote
            self.quote_cache[symbol_id] = fs

            self.spi.on_quote(QuoteType.kFutureSnapshot, fs)

        elif 0x00000002 == msg_type_id:
            fmt = 'ii32sQQ9d10d10Q10d10Q5dQqdi'
            opt = OptionSnapshot._make(struct.unpack(fmt, msg))
            symbol_id = opt.symbol_id[0:opt.symbol_id.find(b'\x00')].decode()
            opt = opt._replace(symbol_id=symbol_id)

            # Locally cache the quote
            self.quote_cache[symbol_id] = opt

            self.spi.on_quote(QuoteType.kOptionSnapshot, opt)

    def register_spi(self, spi):
        self.spi = spi

    def run(self):
        self.loop.create_task(self.init())
        self.loop.run_forever()

    async def init(self):
        conn1_lost = self.loop.create_future()

        self.trans, self.proto = await self.loop.create_connection(
            lambda: Client(self, conn1_lost),
            self.ip,
            self.port
        )

        try:
            await self.proto.conn_lost_future
        finally:
            self.trans.close()


if __name__ == '__main__':
    EXCHANGES = ('comex', 'iceu')
    ENGINE_MANAGER = EngineManager()


    class MySpi(LSQuoteSpi):
        def __init__(self) -> None:
            super().__init__()

        def on_quote(self, quote_type, quote):
            # print(f'QuoteType: {quote_type} - Quote: {quote}')
            data = dict(quote._asdict())
            FutureSnapshot(cn_calendar_day=0, cn_trading_day=0, symbol_id='B2110.ICEU',
                           cn_exchange_time=20210723111720000, local_exchange_time=0, pre_close_price=0.0,
                           pre_settelement_price=73.15, open_price=72.93, high_price=73.23, low_price=72.69,
                           last_price=72.75, settlement_price=73.15, up_limit_price=0.0, down_limit_price=0.0,
                           bid_px1=72.73, bid_px2=72.72, bid_px3=72.71, bid_px4=72.7, bid_px5=72.69, bid_px6=72.68,
                           bid_px7=72.58, bid_px8=72.54, bid_px9=72.48, bid_px10=72.45, bid_vol1=11, bid_vol2=3,
                           bid_vol3=2, bid_vol4=2, bid_vol5=2, bid_vol6=2, bid_vol7=4, bid_vol8=6, bid_vol9=5,
                           bid_vol10=3, ask_px1=72.75, ask_px2=72.76, ask_px3=72.77, ask_px4=72.78, ask_px5=72.79,
                           ask_px6=72.8, ask_px7=72.85, ask_px8=72.93, ask_px9=73.03, ask_px10=73.49, ask_vol1=2,
                           ask_vol2=3, ask_vol3=2, ask_vol4=2, ask_vol5=2, ask_vol6=2, ask_vol7=1, ask_vol8=1,
                           ask_vol9=1, ask_vol10=2, turnover=0.0, volume=8518, open_interest=0)
            symbol_id = data["symbol_id"].split(".")
            cn_exchange_time = str(data["cn_exchange_time"])
            exchange_id = symbol_id[1].lower()
            security_id = symbol_id[0].lower()
            if exchange_id is not None and security_id is not None:
                data["exchange_id"] = exchange_id
                data["security_id"] = security_id
                data["trading_day"] = cn_exchange_time[0:8]
                data["exchange_time"] = cn_exchange_time[8:12]
                ENGINE_MANAGER.addEngine(exchange_id, security_id)
                ENGINE_MANAGER["{}{}".format(exchange_id, security_id)].handle_tick(data)


    spi = MySpi()
    api = LSQuoteApi('58.34.152.109', 9999)
    api.register_spi(spi)
    api.start()

    input('pause')
    print('====================')
    print(f"{api.get_quote_cache('B2202.ICEU')}")
    print('====================')
    input('pause')
