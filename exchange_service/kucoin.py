from .exchanges.kucoin import KucoinFutures, KucoinSpot
from .parsers.kucoin import KucoinParser


class Kucoin(object):
    name = "kucoin"

    def __init__(self):
        self.spot = KucoinSpot()
        self.futures = KucoinFutures()
        self.parser = KucoinParser()

        self.exchange_info = {}

    async def close(self):
        await self.spot.close()
        await self.futures.close()

    async def get_exchange_info(self) -> dict:
        spot = self.parser.parse_exchange_info(await self.spot._get_exchange_info())

        return {
            **spot,
        }
