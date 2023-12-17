import asyncio
from datetime import datetime as dt
from datetime import timedelta as td

import pandas as pd
from lib.binance import Binance
from lib.utils import Tools, query_dict
from pymongo import UpdateOne

from .exchange_job import ExchangeJob

EXCHANGE = "binance"


class TickersJob(ExchangeJob):
    NAME = "tickers"

    def __init__(self):
        self.tools = Tools()
        self.logger = self.tools.get_logger(EXCHANGE)

    async def parse_tickers(self, tickers: dict, exchange_info: dict) -> list:
        results = []
        for id in tickers:
            info = exchange_info[id]
            ticker = tickers[id]
            if info["is_inverse"]:
                ticker["quote_volume"] *= info["contract_size"]
            if ticker["base_volume"] == 0:
                continue

            ticker["instrument_id"] = id
            ticker["exchange"] = "binance"
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
        binance = await Binance().create()

        # get volume
        exchange_info = binance.exchange_info
        tickers = await binance.get_tickers()

        results = await self.parse_tickers(tickers, exchange_info)

        await self.save(results)

        await binance.close()
        self.logger.info(f"Updates {len(results)} tickers of Binance")


class KlinesJob(ExchangeJob):
    NAME = "klines"
    UNIQUE_KEY = ["instrument_id", "open_time", "exchange"]

    def __init__(self):
        self.tools = Tools()
        self.logger = self.tools.get_logger(EXCHANGE)

    @staticmethod
    def handle_kwargs(**kwargs):
        if kwargs["start"]:
            start = int(dt.strptime(kwargs["start"], "%Y%m%d").timestamp() * 1000)
        else:
            start = int(dt.combine(dt.today() - td(days=7), dt.min.time()).timestamp() * 1000)

        if kwargs["end"]:
            end = int(dt.strptime(kwargs["end"], "%Y%m%d").timestamp() * 1000)
        else:
            end = int(dt.combine(dt.today(), dt.min.time()).timestamp() * 1000)

        return {
            "start": start,
            "end": end,
        }

    async def parse_klines(self, id: str, klines: dict, info: dict) -> list:

        results = pd.DataFrame(klines).T
        results["instrument_id"] = id
        results["exchange"] = "binance"
        results["updated_time"] = self.tools.get_timestap()

        results.reset_index(inplace=True)
        results.rename(columns={"index": "open_time"}, inplace=True)

        if info["is_inverse"]:
            results["quote_volume"] *= info["contract_size"]
        return results.to_dict(orient="records")

    async def save(self, datas: list):
        if not datas:
            return

        collection = self.tools.init_collection("CexData", "klines")
        operations = [
            UpdateOne(
                {
                    "instrument_id": data["instrument_id"],
                    "open_time": data["open_time"],
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

    def create_batch(self, batch_num: int, ids: list):
        batches = []
        for i in range(0, len(ids), batch_num):
            batches.append(ids[i : i + batch_num])
        return batches

    async def run(self, **kwargs):
        kwargs = self.handle_kwargs(**kwargs)

        binance = await Binance().create()
        info = query_dict(binance.exchange_info, "active == True")

        # get klines
        batch_size = 10
        batches = self.create_batch(batch_size, list(info.keys()))
        self.logger.info(
            f"Total batches: {len(batches)}, total instruments: {len([item for sublist in batches for item in sublist])}"
        )
        for index, batch in enumerate(batches):
            print(index)
            tasks = []
            for id in batch:
                task = asyncio.create_task(binance.get_klines(id, "1d", kwargs["start"], kwargs["end"]))
                tasks.append(task)
            klines = await asyncio.gather(*tasks)

            # parse klines
            tasks = []
            for kline in klines:
                id = list(kline.keys())[0]
                task = asyncio.create_task(self.parse_klines(id, kline[id], binance.exchange_info[id]))
                tasks.append(task)
            results = await asyncio.gather(*tasks)

            # save klines
            tasks = []
            for result in results:
                task = asyncio.create_task(self.save(result))
                tasks.append(task)
            await asyncio.gather(*tasks)

        await binance.close()
        self.logger.info(
            f"Updates klines of Binance. Total batches: {len(batches)}, total instruments: {len([item for sublist in batches for item in sublist])}"
        )


class DcpJob(ExchangeJob):
    NAME = "dcp"
    UNIQUE_KEY = ["currency", "timestamp", "exchange"]
    STABLE_COINS = ["USDT", "BUSD", "USDC", "TUSD", "PAX", "USDS"]
    IGNORE_COINS = []

    def __init__(self):
        self.tools = Tools()
        self.logger = self.tools.get_logger(EXCHANGE)

    def handle_kwargs(self, **kwargs):
        if kwargs["start"]:
            start = int(dt.strptime(kwargs["start"], "%Y%m%d").timestamp() * 1000)
        else:
            start = int(dt.combine(dt.today() - td(days=7), dt.min.time()).timestamp() * 1000)

        if kwargs["end"]:
            end = int(dt.strptime(kwargs["end"], "%Y%m%d").timestamp() * 1000)
        else:
            end = int(dt.combine(dt.today(), dt.min.time()).timestamp() * 1000)

        return {
            "start": start,
            "end": end,
        }

    async def parse_dcp(self, timestamp: int, currency: str, klines: dict) -> list:
        sub_klines = klines[(klines["open_time"] == timestamp) & (klines["base"] == currency)]
        if sub_klines.empty:
            return {}

        average_close = sub_klines["base_close_price"].mean()

        return {
            "timestamp": int(timestamp),
            "exchange": "binance",
            "currency": currency,
            "close": float(average_close),
        }

    async def save(self, datas: list):
        if not datas:
            return

        collection = self.tools.init_collection("CexData", "dcp")
        operations = [
            UpdateOne(
                {
                    "timestamp": data["timestamp"],
                    "currency": data["currency"],
                    "exchange": data["exchange"],
                },
                {
                    "$set": data,
                },
                upsert=True,
            )
            for data in datas
            if data
        ]
        collection.bulk_write(operations)

    async def run(self, **kwargs):
        kwargs = self.handle_kwargs(**kwargs)

        binance = await Binance().create()

        query_env = {"STABLE_COINS": self.STABLE_COINS, "IGNORE_COINS": self.IGNORE_COINS}
        infos = query_dict(
            binance.exchange_info,
            "active == True and quote in @STABLE_COINS and base not in @STABLE_COINS and base not in @IGNORE_COINS and is_futures == False",
            query_env,
        )

        klines_collections = self.tools.init_collection("CexData", "klines")

        filter_ = {
            "open_time": {"$gte": kwargs["start"], "$lte": kwargs["end"]},
            "instrument_id": {"$in": list(infos.keys())},
            "exchange": "binance",
        }

        klines = klines_collections.find(filter_, {"_id": 0})
        klines = pd.DataFrame(klines)
        klines["base"] = klines["instrument_id"].apply(lambda x: infos[x]["base"])
        klines["multiplier"] = klines["instrument_id"].apply(lambda x: infos[x]["multiplier"])
        klines["base_close_price"] = klines["close"] / klines["multiplier"]
        unique_timestamp = klines["open_time"].unique()
        unique_currency = klines["base"].unique()

        for timestamp in unique_timestamp:
            print(timestamp)
            tasks = []
            for currency in unique_currency:
                task = asyncio.create_task(self.parse_dcp(timestamp, currency, klines))
                tasks.append(task)
            result = await asyncio.gather(*tasks)
            await self.save(result)

        await binance.close()

        self.logger.info(
            f"Updates dcp of Binance from {kwargs['start']} to {kwargs['end']}. With {len(unique_timestamp)} timestamps and {len(unique_currency)} currencies."
        )
