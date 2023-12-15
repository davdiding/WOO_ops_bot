from ..lib.binance import Binance
from ..lib.utils import Tools


class BinanceJob(object):
    NAME = "binance"

    def __init__(self):
        self.binance = Binance().create()
        self.tools = Tools()

    async def run(self):
        tickers = await self.binance.get_tickers()
        return tickers
