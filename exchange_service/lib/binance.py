from .exchanges.binance import Binance as http
from .parsers.binance import BinanceParser as parser


class Binance(object):
    def __init__(self):
        self.http = http()
        self.parser = parser()

        self.exchange_info = {}

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self):
        spot = self.parser.parse_exchange_info(
            await self.http._get_exchange_info(), self.parser.spot_exchange_info_parser
        )
        return {**spot}

    async def get_ticker(self, id: str):
        _symbol = self.exchange_info[id]["raw_data"]["symbol"]

        return {id: self.parser.parse_ticker(await self.http._get_ticker(_symbol))}

    async def get_tickers(self):
        results = {}

        datas = self.parser.parse_tickers(await self.http._get_tickers())
        for i in datas:
            id = self.parser.get_id_by_symbol(i["symbol"], self.exchange_info)
            results[id] = i

        return results
