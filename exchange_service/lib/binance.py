from .exchanges.binance import BinanceInverse, BinanceLinear, BinanceSpot
from .parsers.binance import BinanceParser as parser
from .utils import query_dict


class Binance(object):
    def __init__(self):
        self.spot = BinanceSpot()
        self.linear = BinanceLinear()
        self.inverse = BinanceInverse()
        self.parser = parser()

        self.exchange_info = {}

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: str = None):
        spot = self.parser.parse_exchange_info(
            await self.spot._get_exchange_info(), self.parser.spot_exchange_info_parser
        )
        linear = self.parser.parse_exchange_info(
            await self.linear._get_exchange_info(), self.parser.futures_exchange_info_parser("linear")
        )
        inverse = self.parser.parse_exchange_info(
            await self.inverse._get_exchange_info(), self.parser.futures_exchange_info_parser("inverse")
        )
        result = {**spot, **linear, **inverse}
        return result if not market_type else query_dict(result, f"is_{market_type} == True")

    async def get_ticker(self, id: str):
        _symbol = self.exchange_info[id]["raw_data"]["symbol"]

        return {id: self.parser.parse_ticker(await self.spot._get_ticker(_symbol))}

    async def get_tickers(self):
        results = {}

        tickers = [(self.spot, "spot"), (self.linear, "linear"), (self.inverse, "inverse")]

        for exchange, market_type in tickers:
            parsed_tickers = self.parser.parse_tickers(await exchange._get_tickers(), market_type)
            id_map = self.parser.get_id_symbol_map(self.exchange_info, market_type)

            for ticker in parsed_tickers:
                symbol = ticker["symbol"]
                if symbol not in id_map:
                    print(symbol)
                    continue
                results[id_map[symbol]] = ticker

        return results
