from cex_services.binance import Binance
from cex_services.bitget import Bitget
from cex_services.bybit import Bybit
from cex_services.gateio import Gateio
from cex_services.htx import Htx
from cex_services.kucoin import Kucoin
from cex_services.okx import Okx
from lib.utils import Tool


class Updater(object):
    def __init__(self):
        self.tool = Tool()
        self.logger = self.tool.get_logger("updater")
        self.collection = self.tool.init_collection("CexData", "exchange_info")

    async def run(self):
        binance = await Binance().create()
        okx = await Okx().create()
        bybit = await Bybit().create()
        kucoin = await Kucoin().create()
        gateio = await Gateio().create()
        htx = await Htx().create()
        bitget = await Bitget().create()

        # get exchange_info
        for exchange in [binance, okx, bybit, kucoin, gateio, htx, bitget]:
            exchange_info = await exchange.get_exchange_info()
            timestamp = self.tool.get_timestamp()
            result = {
                "id": self.tool.get_id(timestamp),
                "timestamp": timestamp,
                "datetime": self.tool.get_datetime(),
                "exchange": exchange.name,
                "data": exchange_info,
            }
            self.collection.insert_one(result)

        self.logger.info("Update exchange_info of binance, okx, bybit, kucoin, gateio, htx, bitget.")

        await binance.close()
        await okx.close()
        await bybit.close()
        await kucoin.close()
        await gateio.close()
        await htx.close()
        await bitget.close()


async def main():
    updater = Updater()
    await updater.run()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
