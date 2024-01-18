import asyncio

from ..exchange_service.lib.binance import Binance
from ..exchange_service.lib.bybit import Bybit
from ..exchange_service.lib.okx import Okx


class Updater(object):
    async def run(self):
        binance = await Binance().create()
        okx = await Okx().create()
        bybit = await Bybit().create()

        # get exchange_info
        for exchange in [binance, okx, bybit]:
            exchange_info = await exchange.get_exchange_info()
            print(exchange_info)
            continue


if __name__ == "__main__":
    updater = Updater()
    asyncio.run(updater.run())
