from lib.utils import Tools
from pymongo import UpdateOne

from exchange_service.bybit import Bybit

from .exchange_job import ExchangeJob

EXCHANGE = "bybit"


class TickersJob(ExchangeJob):
    NAME = "tickers"

    def __init__(self):
        self.tools = Tools()
        self.logger = self.tools.get_logger(EXCHANGE)

    async def parse_tickers(self, tickers: dict) -> list:
        results = []
        for id in tickers:
            ticker = tickers[id]
            if ticker["base_volume"] == 0:
                continue

            ticker["instrument_id"] = id
            ticker["exchange"] = EXCHANGE
            ticker["timestamp"] = self.tools.get_timestap()
            ticker["updated_time"] = self.tools.get_timestap()
            del ticker["symbol"]
            results.append(ticker)
        return results

    async def save(self, datas: list):
        collection = self.tools.init_collection("CexData", "tickers")
        operations = [
            UpdateOne(
                {
                    "instrument_id": data["instrument_id"],
                    "timestamp": data["timestamp"],
                    "exchange": data["exchange"],
                },
                {
                    "$set": data,
                },
                upsert=True,
            )
            for data in datas
        ]
        collection.bulk_write(operations)

    async def run(self, **kwargs):
        okx = await Bybit().create()

        # get volume
        tickers = await okx.get_tickers()

        results = await self.parse_tickers(tickers)

        await self.save(results)

        await okx.close()
        self.logger.info(f"Updates {len(results)} tickers of Bybit")


class InfoJob(ExchangeJob):
    NAME = "exchange_info"

    def __init__(self):
        self.tools = Tools()
        self.logger = self.tools.get_logger(EXCHANGE)

    async def run(self, **kwargs):
        bybit = await Bybit().create()
        result = {
            "timestamp": self.tools.get_timestap(),
            "datetime": self.tools.get_datetime(),
            "exchange": EXCHANGE,
            "data": bybit.exchange_info,
        }

        await self.save(result)
        await bybit.close()

        self.logger.info(f"Insert exchange info of Bybit")

    async def save(self, data: dict):
        collection = self.tools.init_collection("CexData", "exchange_info")
        collection.insert_one(data)
