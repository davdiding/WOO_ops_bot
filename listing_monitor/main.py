import asyncio

import pandas as pd
import pymongo as pm
from lib.utils import Tool

# import pymongo as pm
# from telegram import Bot


class ListingMonitor:
    def __init__(self):
        self.tool = Tool()
        self.logger = self.tool.get_logger("main")
        self.collection = self.tool.init_collection("CexData", "exchange_info")

    async def run(self, exchange: str = None):
        # get the last two exchange info and do some comparison
        filter_ = {
            "exchange": exchange,
        }
        df = pd.DataFrame(self.collection.find(filter_).sort("timestamp", pm.DESCENDING).limit(2))

        time1 = df["timestamp"].min()
        time2 = df["timestamp"].max()

        info1 = df[df["timestamp"] == time1]["data"].values[0]
        info2 = df[df["timestamp"] == time2]["data"].values[0]

        id1 = df[df["timestamp"] == time1]["id"].values[0]
        id2 = df[df["timestamp"] == time2]["id"].values[0]

        date1 = df[df["timestamp"] == time1]["datetime"].values[0]
        date2 = df[df["timestamp"] == time2]["datetime"].values[0]

        for key2 in info2.keys():
            if key2 not in info1.keys():
                self.logger.warning(
                    f"New instrument {key2} is added to {exchange}. old record id and datetime: {id1}, {date1}. new record id and datetime: {id2}, {date2}"
                )
                continue
        self.logger.info(
            f"Finished checking {exchange}. old record id and datetime: {id1}, {date1}. new record id and datetime: {id2}, {date2}"
        )


async def main():
    monitor = ListingMonitor()

    for exchange in ["binance", "okx", "bybit"]:
        asyncio.run(monitor.run(exchange))
