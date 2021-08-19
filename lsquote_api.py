import abc
import asyncio
import copy
from collections import namedtuple
import logging
import time
import struct
import threading

class QuoteType(object):
    kSnapshot = 0x00000001


class SymbolType(object):
    kUnknown    = 'U'
    kStock      = 'S'
    kBond       = 'B'
    kFuture     = 'F'
    kOption     = 'O'
    kSpot       = 's'


class SubscribeItem(object):
    def __init__(self) -> None:
        self._quote_type = None
        self._symbol_id = None
        self._symbol_type = None
        self._bar_period = 0

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

    @property
    def symbol_type(self):
        return self._symbol_type

    @symbol_type.setter
    def symbol_type(self, value: str):
        self._symbol_type = value

    @property
    def bar_period(self):
        return self._bar_period

    @bar_period.setter
    def bar_period(self, value: str):
        self._bar_period = value

    def pack(self) -> bytes:
        return struct.pack('I32ssI', self._quote_type, self._symbol_id.encode(), self._symbol_type.encode(), self._bar_period)

    @staticmethod
    def load(buffer):
        print(f'Loading SubscribeItem from buffer, {len(buffer)}')
        fmt = 'I32ssI'
        si = SubscribeItem()
        si.quote_type, si.symbol_id, si.symbol_type, si.bar_period = struct.unpack(fmt, buffer)
        return si


SymbolSnapshotStruct = (
    ('trading_phase',        'H'),
    ('cn_calendar_day',      'I'),
    ('cn_trading_day',       'I'),
    ('symbol_id',          '32s'),
    ('symbol_type',          's'),
    ('cn_exchange_time',     'Q'),
    ('local_exchange_time',  'Q'),
    ('pre_close_price',      'd'),
    ('open_price',           'd'),
    ('high_price',           'd'),
    ('low_price',            'd'),
    ('last_price',           'd'),
    ('up_limit_price',       'd'),
    ('down_limit_price',     'd'),

    ('bid_px1',              'd'),
    ('bid_px2',              'd'),
    ('bid_px3',              'd'),
    ('bid_px4',              'd'),
    ('bid_px5',              'd'),
    ('bid_px6',              'd'),
    ('bid_px7',              'd'),
    ('bid_px8',              'd'),
    ('bid_px9',              'd'),
    ('bid_px10',             'd'),
    ('bid_vol1',             'd'),
    ('bid_vol2',             'd'),
    ('bid_vol3',             'd'),
    ('bid_vol4',             'd'),
    ('bid_vol5',             'd'),
    ('bid_vol6',             'd'),
    ('bid_vol7',             'd'),
    ('bid_vol8',             'd'),
    ('bid_vol9',             'd'),
    ('bid_vol10',            'd'),
 
    ('ask_px1',              'd'),
    ('ask_px2',              'd'),
    ('ask_px3',              'd'),
    ('ask_px4',              'd'),
    ('ask_px5',              'd'),
    ('ask_px6',              'd'),
    ('ask_px7',              'd'),
    ('ask_px8',              'd'),
    ('ask_px9',              'd'),
    ('ask_px10',             'd'),
    ('ask_vol1',             'd'),
    ('ask_vol2',             'd'),
    ('ask_vol3',             'd'),
    ('ask_vol4',             'd'),
    ('ask_vol5',             'd'),
    ('ask_vol6',             'd'),
    ('ask_vol7',             'd'),
    ('ask_vol8',             'd'),
    ('ask_vol9',             'd'),
    ('ask_vol10',            'd'),

    ('turnover',             'd'),
    ('volume',               'd'),

    # Future-symbol only
    ('pre_settelement_price','d'),
    ('settlement_price',     'd'),
    ('open_interest',        'd'),

    # Option-symbol only
    ('strike_price',         'd'),
    ('delta',                'd'),
    ('gamma',                'd'),
    ('vega',                 'd'),
    ('theta',                'd')
)

SymbolSnapshot = namedtuple('SymbolSnapshot', (x[0] for x in SymbolSnapshotStruct))
SymbolSnapshotFMT = f'<{"".join([i[1] for i in SymbolSnapshotStruct])}'
SymbolSnapshotSize = struct.calcsize(f'<{"".join([i[1] for i in SymbolSnapshotStruct])}')


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
        self._validated = False
        self._validation_len = 8
        self._validation_in = 0
        self._validation_out = bytearray()

        self.__logger = logging.getLogger('asyncio')

    # def write(self)

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

        msg_type_id, body_len = struct.unpack('II', bdata[0:self._phb])
        pkg_len = struct.calcsize('II') + body_len

        # 传入的字节不满足一个完整包
        if len(bdata) < pkg_len:
            return None

        self.onMessage(msg_type_id, bdata[self._phb:pkg_len])
        return bdata[pkg_len:]

    def scramble(self, data):
        out = data ^ 0xDEADBEEFBADEBABE
        out = (out & 0xF0F0F0F0F0F0F0) >> 4 | (out & 0xF0F0F0F0F0F0F0) << 4
        return out ^ 0xF00DBABE

    def parseValidation(self, bdata):
        if len(bdata) < self._validation_len:
            return None
        
        self._validation_in = struct.unpack('Q', bdata[0:self._validation_len])[0]
        self._validation_out.extend(struct.pack('Q', self.scramble(self._validation_in)))
        return bdata[self._validation_len:]
        

    def data_received(self, bdata):
        """Doc."""
        self._buffer.extend(bdata)

        if not self._validated:
            res = self.parseValidation(bdata)
            if res is None:
                return
            else:
                self._transport.write(bytes(self._validation_out))
                self._buffer.clear()
                self._buffer.extend(res)
                self._validated = True


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
        self.lsquote_api._on_connected()

    def onDisconnected(self, reason):
        self.conn_lost_future.set_result(True)
        self.lsquote_api._on_disconnected()


class MessageType(object):
    # Push
    kSnapshot                = 0xF0000001

    # Directive 
    kSubscribe               = 0xD0001000
    kSubscribeStreaming      = 0xD0001001
    kUnSubscribe             = 0xD0001002
    kUnSubscribeAll          = 0xD0001003      # 取消订阅该Client所有订阅的合约
    kUnSubscribeStreaming    = 0xD0001004

    # Answer
    kRspSubscribe            = 0xA0001000
    kRspSubscribeStreaming   = 0xA0001001
    kRspUnSubscribe          = 0xA0001002
    kRspUnSubscribeAll       = 0xA0001003
    kRspUnSubscribeStreaming = 0xA0001004


class Message(object):
    def __init__(self, msg_type_id:int):
        self.msg_type_id = msg_type_id
        self.size = 0
        self.buffer = bytearray()

    def push(self, bdata: bytes):
        self.buffer.extend(bdata)
        self.size += len(bdata)
    
    def pack(self):
        bdata_send = struct.pack('II', self.msg_type_id, self.size) + self.buffer
        print(f'Packing message: {self.msg_type_id}, {self.size}, {bdata_send}, {len(bdata_send)}')
        return bdata_send


class LSQuoteSpi(metaclass=abc.ABCMeta):
    # @abc.abstractmethod
    def on_connected(self):
        pass

    # @abc.abstractmethod
    def on_disconnected(self):
        pass

    # @abc.abstractmethod
    def on_quote(self, quote_type, quote):
        pass

    # @abc.abstractmethod
    def on_subscribe(self, subscribe_item: SubscribeItem, error_no: int):
        pass


class LSQuoteApi(threading.Thread):
    def __init__(self, ip, port):
        super().__init__(daemon=True)
        self.ip = ip
        self.port = port
        self.loop = asyncio.get_event_loop()
        if __debug__:
            self.loop.set_debug(True)
        self.quote_cache = {}
        self.quote_cache_lock = threading.Lock()

    def get_all_quote_cache(self):
        self.quote_cache_lock.acquire()
        
        cache = copy.deepcopy(self.quote_cache)

        self.quote_cache_lock.release()

        return cache

    def get_quote_cache(self, symbol_id: str):
        self.quote_cache_lock.acquire()

        cache = None

        if symbol_id in self.quote_cache:
            cache = copy.deepcopy(self.quote_cache[symbol_id])

        self.quote_cache_lock.release()
        
        return cache

    def _on_message(self, msg_type_id, msg):
        # print(f'Recvd message: {msg}, {len(msg)}')
        self.quote_cache_lock.acquire()

        if MessageType.kSnapshot == msg_type_id:
            fmt = SymbolSnapshotFMT
            ss = SymbolSnapshot._make(struct.unpack(fmt, msg))
            symbol_id = ss.symbol_id[0:ss.symbol_id.find(b'\x00')].decode()
            ss = ss._replace(symbol_id=symbol_id)

            # TODO
            # 临时方案，后续改为从db获取存到mem后根据symbol_id匹配
            if '-C-' in symbol_id or '-P-' in symbol_id:
                k = symbol_id.split('.')[0].split('-')[-1]
                ss = ss._replace(strike_price=k)

            # Locally cache the quote
            self.quote_cache[symbol_id] = ss

            self.spi.on_quote(QuoteType.kSnapshot, ss)

        # if MessageType.kSymbolSnapshot == msg_type_id:
        #     fmt = 'ii32sQQddddddddd10d10Q10d10QdQq'
        #     fs = SymbolSnapshot._make(struct.unpack(fmt, msg))
        #     symbol_id = fs.symbol_id[0:fs.symbol_id.find(b'\x00')].decode()
        #     fs = fs._replace(symbol_id=symbol_id)

        #     # Locally cache the quote
        #     self.quote_cache[symbol_id] = fs

        #     self.spi.on_quote(QuoteType.kSymbolSnapshot, fs)

        # elif MessageType.kOptionSnapshot == msg_type_id:
        #     fmt = 'ii32sQQ9d10d10Q10d10Q5dQqdi'
        #     opt = OptionSnapshot._make(struct.unpack(fmt, msg))
        #     symbol_id = opt.symbol_id[0:opt.symbol_id.find(b'\x00')].decode()
        #     opt = opt._replace(symbol_id=symbol_id)

        #     # TODO
        #     # 临时方案，改为从db获取存到mem后根据symbol_id匹配
        #     k = symbol_id.split('.')[0].split('-')[-1]
        #     opt = opt._replace(strike_price=k)
            
        #     # Locally cache the quote
        #     self.quote_cache[symbol_id] = opt

        #     self.spi.on_quote(QuoteType.kOptionSnapshot, opt)

        elif MessageType.kRspSubscribe == msg_type_id:
            fmt = 'q'
            si = SubscribeItem.load(msg[:len(msg) - struct.calcsize(fmt)])
            error_no = struct.unpack(fmt, msg[len(msg) - struct.calcsize(fmt):])[0]

            if __debug__:
                print(f'收到订阅行情的回复, {msg}, {error_no}, {si.symbol_id}')

        elif MessageType.kRspUnSubscribe == msg_type_id:
            fmt = 'q'
            si = SubscribeItem.load(msg[:len(msg) - struct.calcsize(fmt)])
            error_no = struct.unpack(fmt, msg[len(msg) - struct.calcsize(fmt):])[0]

            if __debug__:
                print(f'收到取消订阅行情的回复, {error_no}, {si.symbol_id}')

        elif MessageType.kRspUnSubscribeAll == msg_type_id:
            fmt = 'q'
            error_no = struct.unpack(fmt, msg)[0]

            if __debug__:
                print(f'收到取消订阅所有行情的回复, {error_no}')

        elif MessageType.kRspSubscribeStreaming == msg_type_id:
            fmt = 'q'
            error_no = struct.unpack(fmt, msg)[0]

            if __debug__:
                print(f'收到订阅行情流的回复, {error_no}')
        
        elif MessageType.kRspUnSubscribeStreaming == msg_type_id:
            fmt = 'q'
            error_no = struct.unpack(fmt, msg)[0]

            if __debug__:
                print(f'收到取消订阅行情流的回复, {error_no}')

        self.quote_cache_lock.release()

    def _on_connected(self):
        self.spi.on_connected()

    def _on_disconnected(self):
        self.spi.on_disconnected()

    def subscribe(self, subscribe_item: SubscribeItem):
        async def _subscribe(subscribe_item: SubscribeItem):
            msg = Message(MessageType.kSubscribe)
            msg.push(subscribe_item.pack())
            bdata = msg.pack()

            print('Send subscribe message, {bdata}')
            self.trans.write(bdata)

        asyncio.run_coroutine_threadsafe(_subscribe(subscribe_item), self.loop)

    def subscribe_streaming(self):
        async def _subscribe_streaming():
            msg = Message(MessageType.kSubscribeStreaming)
            bdata = msg.pack()

            print('Send subscribe_streaming message, {bdata}')
            self.trans.write(bdata)

        asyncio.run_coroutine_threadsafe(_subscribe_streaming(), self.loop)

    def unsubscribe(self, subscribe_item: SubscribeItem):
        async def _unsubscribe(subscribe_item: SubscribeItem):
            msg = Message(MessageType.kUnSubscribe)
            msg.push(subscribe_item.pack())
            bdata = msg.pack()

            print(f'Send unsubscribe message, {bdata}')
            self.trans.write(bdata)

        asyncio.run_coroutine_threadsafe(_unsubscribe(subscribe_item), self.loop)

    def unsubscribe_all(self):
        async def _unsubscribe_all():
            msg = Message(MessageType.kUnSubscribeAll)
            bdata = msg.pack()

            print('Send unsubscribe_all message, {bdata}')
            self.trans.write(bdata)

        asyncio.run_coroutine_threadsafe(_unsubscribe_all(), self.loop)

    def unsubscribe_streaming(self):
        async def _unsubscribe_streaming():
            msg = Message(MessageType.kUnSubscribeStreaming)
            bdata = msg.pack()

            print('Send unsubscribe_streaming message, {bdata}')
            self.trans.write(bdata)

        asyncio.run_coroutine_threadsafe(_unsubscribe_streaming(), self.loop)

    def register_spi(self, spi):
        self.spi = spi

    def join(self):
        while True:
            time.sleep(1.0)

    def run(self):
        self.loop.create_task(self.init())
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.loop.stop()

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
