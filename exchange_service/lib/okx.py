from .exchanges.okx import OkxUnified
from .parsers.okx import OkxParser


class Okx(object):
    def __init__(self):
        self.okx = OkxUnified()
        self.parser = OkxParser()
        self.exchange_info = {}

    async def get_exchange_info(self, market_type: str = None):
        if market_type:
            exchange_info = await self.okx._get_exchange_info(market_type)
        else:
            pass
        return exchange_info
