from .exchanges.bybit import BybitUnified
from .parsers.bybit import BybitParser


class Bybit(object):
    def __init__(self):
        self.bybit = BybitUnified()
        self.parser = BybitParser()
        self.exchange_info = {}

    async def close(self):
        await self.bybit._session.close()

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: str = None):
        spot = self.parser.parse_exchange_info(
            await self.bybit._get_exchange_info("spot"), self.parser.spot_exchange_info_parser
        )
        linear = self.parser.parse_exchange_info(
            await self.bybit._get_exchange_info("linear"), self.parser.futures_perp_exchange_info_parser
        )
        inverse = self.parser.parse_exchange_info(
            await self.bybit._get_exchange_info("inverse"), self.parser.futures_perp_exchange_info_parser
        )

        return {**spot, **linear, **inverse}
