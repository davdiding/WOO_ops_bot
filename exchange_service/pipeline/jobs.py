from lib.binance import Binance
from lib.utils import Tools


class BinanceJob(object):
    NAME = "binance"

    def __init__(self):
        self.tools = Tools()
        self.logger = self.tools.get_logger(self.NAME)

    async def run(self):
        binance = await Binance().create()

        # get volume
        exchange_info = binance.exchange_info
        tickers = await binance.get_tickers()

        results = []
        for id in tickers:
            info = exchange_info[id]
            ticker = tickers[id]
            if info["is_inverse"]:
                ticker["quote_volume"] *= info["contract_size"]
            if ticker["base_volume"] == 0:
                continue

            ticker["id"] = id
            ticker["exchange"] = "binance"
            ticker["timestamp"] = self.tools.get_today()
            del ticker["raw_data"]
            del ticker["symbol"]
            results.append(ticker)

        collection = self.tools.init_collection("CexData", "tickers")
        collection.insert_many(results)

        self.logger.info(f"Inserted {len(results)} tickers of Binance")
