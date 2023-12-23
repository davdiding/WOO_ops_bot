from .exchanges.okx import OkxUnified
from .parsers.okx import OkxParser


class Okx(object):
    market_type_map = {"spot": "SPOT", "margin": "MARGIN", "futures": "FUTURES", "perp": "SWAP"}
    _market_type_map = {"SPOT": "spot", "MARGIN": "margin", "FUTURES": "futures", "SWAP": "perp"}

    def __init__(self):
        self.okx = OkxUnified()
        self.parser = OkxParser()
        self.exchange_info = {}

    async def close(self):
        await self.okx._session.close()

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: str = None):
        if market_type:
            parser = (
                self.parser.spot_margin_exchange_info_parser
                if market_type in ["spot", "margin"]
                else self.parser.futures_perp_exchange_info_parser
            )
            return self.parser.parse_exchange_info(
                await self.okx._get_exchange_info(self.market_type_map[market_type]), parser
            )

        else:
            spot = self.parser.parse_exchange_info(
                await self.okx._get_exchange_info("SPOT"), self.parser.spot_margin_exchange_info_parser
            )
            margin = self.parser.parse_exchange_info(
                await self.okx._get_exchange_info("MARGIN"), self.parser.spot_margin_exchange_info_parser
            )
            futures = self.parser.parse_exchange_info(
                await self.okx._get_exchange_info("FUTURES"), self.parser.futures_perp_exchange_info_parser
            )
            perp = self.parser.parse_exchange_info(
                await self.okx._get_exchange_info("SWAP"), self.parser.futures_perp_exchange_info_parser
            )
            exchange_info = {**self.parser.combine_spot_margin_exchange_info(spot, margin), **futures, **perp}
        return exchange_info

    async def get_tickers(self, market_type: str = None) -> list:

        results = {}

        tickers = [("SPOT", "spot"), ("FUTURES", "futures"), ("SWAP", "perp")]

        for _market_type, market_type in tickers:
            if market_type != market_type:
                continue
            parsed_tickers = self.parser.parse_tickers(await self.okx._get_tickers(_market_type), market_type)
            id_map = self.parser.get_id_symbol_map(self.exchange_info, market_type, "instId")

            for ticker in parsed_tickers:
                symbol = ticker["symbol"]
                if symbol not in id_map:
                    print(symbol)
                    continue
                results[id_map[symbol]] = ticker
        return results

    async def get_ticker(self, id: str):
        _id = self.exchange_info[id]["raw_data"]["instId"]
        market_type = self._market_type_map[self.exchange_info[id]["raw_data"]["instType"]]
        return {id: self.parser.parse_ticker(await self.okx._get_ticker(_id), market_type)}
