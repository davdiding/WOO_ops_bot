import asyncio

from lib.utils import Tool

from exchange_service.binance import Binance
from exchange_service.bybit import Bybit
from exchange_service.okx import Okx


class Updater(object):
    def __init__(self):
        self.tool = Tool()
        self.logger = self.tool.get_logger("main")
        self.collection = self.tool.init_collection("CexData", "exchange_info")

    async def run(self):
        binance = await Binance().create()
        okx = await Okx().create()
        bybit = await Bybit().create()

        # get exchange_info
        for exchange in [binance, okx, bybit]:
            exchange_info = await exchange.get_exchange_info()
            result = {
                "timestamp": self.tool.get_timestamp(),
                "datetime": self.tool.get_datetime(),
                "exchange": exchange.name,
                "data": exchange_info,
            }
            self.collection.insert_one(result)

        self.logger.info("Update exchange_info of binance, okx, bybit")

        await binance.close()
        await okx.close()
        await bybit.close()


if __name__ == "__main__":
    updater = Updater()
    asyncio.run(updater.run())
