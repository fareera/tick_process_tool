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

    print('====================')
    print(f"{api.get_quote_cache('B2202.ICEU')}")
    print('====================')
    input('pause')
