from lsquote_api import LSQuoteSpi, LSQuoteApi
from utils.engine import EngineManager
from utils.write_logs import WriteLogs


class MySpi(LSQuoteSpi):
    def __init__(self) -> None:
        super().__init__()

    def on_quote(self, quote_type, quote):
        # if QuoteType.kOptionSnapshot == quote_type:
        try:
            print(f'QuoteType: {quote_type} - Quote: {quote}')
            data = dict(quote._asdict())
            symbol_id = data["symbol_id"].split(".")
            cn_exchange_time = str(data["cn_exchange_time"])
            exchange_id = symbol_id[1].lower()
            security_id = symbol_id[0].lower()
            if exchange_id is not None and security_id is not None:
                data["exchange_id"] = exchange_id
                data["security_id"] = security_id
                data["trading_day"] = str(data["cn_trading_day"])
                data["exchange_time"] = cn_exchange_time
                # 处理行情
                ENGINE_MANAGER.addEngine(exchange_id, security_id)
                ENGINE_MANAGER["{}.{}".format(exchange_id, security_id)].handle_tick(data)
        except Exception as e:
            WriteLogs().write_logs(
                level="error", message=f"{str(e)}")


if __name__ == '__main__':
    EXCHANGES = ('comex', 'iceu')
    ENGINE_MANAGER = EngineManager()
    api = LSQuoteApi('58.34.152.109', 9999)
    spi = MySpi()
    api.register_spi(spi)
    api.start()
    # SymbolSnapshot(trading_phase=0, cn_calendar_day=20210819, cn_trading_day=20210819, symbol_id='HG2110-C-444.COMEX',
    #                symbol_type=b'\x00', cn_exchange_time=170050000, local_exchange_time=0, pre_close_price=0.037,
    #                open_price=0.0, high_price=0.0, low_price=0.0, last_price=0.0, up_limit_price=0.0,
    #                down_limit_price=0.0, bid_px1=0.0195, bid_px2=0.019, bid_px3=0.018, bid_px4=0.0, bid_px5=0.0,
    #                bid_px6=0.0, bid_px7=0.0, bid_px8=0.0, bid_px9=0.0, bid_px10=0.0, bid_vol1=5.0, bid_vol2=15.0,
    #                bid_vol3=10.0, bid_vol4=0.0, bid_vol5=0.0, bid_vol6=0.0, bid_vol7=0.0, bid_vol8=0.0, bid_vol9=0.0,
    #                bid_vol10=0.0, ask_px1=0.024, ask_px2=0.0305, ask_px3=0.0, ask_px4=0.0, ask_px5=0.0, ask_px6=0.0,
    #                ask_px7=0.0, ask_px8=0.0, ask_px9=0.0, ask_px10=0.0, ask_vol1=20.0, ask_vol2=10.0, ask_vol3=0.0,
    #                ask_vol4=0.0, ask_vol5=0.0, ask_vol6=0.0, ask_vol7=0.0, ask_vol8=0.0, ask_vol9=0.0, ask_vol10=0.0,
    #                turnover=0.0, volume=0.0, pre_settelement_price=0.037, settlement_price=0.037, open_interest=10.0,
    #                strike_price='444', delta=0.0, gamma=0.0, vega=0.0, theta=0.0)
    print('====================')
    print(f"{api.get_quote_cache('B2202.ICEU')}")
    print('====================')
    input('pause')
