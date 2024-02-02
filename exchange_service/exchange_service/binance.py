from .exchanges.binance import BinanceInverse, BinanceLinear, BinanceSpot
from .parsers.binance import BinanceParser as parser
from .utils import query_dict, sort_dict


class Binance(object):
    name = "binance"

    def __init__(self):
        self.spot = BinanceSpot()
        self.linear = BinanceLinear()
        self.inverse = BinanceInverse()
        self.parser = parser()

        self.exchange_info = {}

    async def close(self):
        await self.spot.close()
        await self.linear.close()
        await self.inverse.close()

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
                id = id_map[symbol]
                info = self.exchange_info[id]
                if info["is_perp"] or info["is_futures"]:
                    ticker["quote_volume"] *= info["contract_size"]
                results[id] = ticker

        return results

    async def get_klines(self, id: str, interval: str, start: int = None, end: int = None, num: int = 500):
        print(id)
        _symbol = self.exchange_info[id]["raw_data"]["symbol"]
        market_type = self.parser.get_market_type(self.exchange_info[id])
        limit = 1000

        method_map = {
            "spot": self.spot._get_klines,
            "linear": self.linear._get_klines,
            "inverse": self.inverse._get_klines,
        }

        query_end = None

        results = {}
        if start and end:
            query_end = end
            while True:
                klines = self.parser.parse_klines(
                    await method_map[market_type](_symbol, interval, endTime=query_end, limit=limit), market_type
                )
                if not klines:
                    break

                results.update(klines)
                query_end = list(klines.keys())[0]
                if len(klines) < limit or query_end <= start:
                    break
                continue

            return {id: sort_dict({k: v for k, v in results.items() if end >= k >= start}, ascending=True)}

        elif num:
            while True:
                klines = self.parser.parse_klines(
                    await method_map[market_type](_symbol, interval, limit=limit)
                    if not query_end
                    else await method_map[market_type](_symbol, interval, endTime=query_end, limit=limit),
                    market_type,
                )
                results.update(klines)
                if len(klines) < limit or len(results) >= num:
                    break
                query_end = list(klines.keys())[0]
                continue

            return {id: sort_dict(results, ascending=True, num=num)}
