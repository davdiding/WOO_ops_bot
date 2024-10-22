from datetime import datetime as dt
from datetime import timedelta as td

import pandas as pd
from lib.utils import Formatter, Grabber, Tools, send_message


class ListingJob:
    def __init__(self, config):
        self.config = config
        self.name = "ListingJob"

    def run(self):
        bot_key = self.config["BOT_KEY"]
        chat_id = self.config["DAVID_CHAT_ID"]
        exchange_info = {
            "binance": "binance",
            "houbi": "huobi-global",
            "okx": "okx",
            "gate.io": "gate-io",
            "kraken": "kraken",
            "coinbase": "coinbase-exchange",
            "crypto.com": "crypto-com-exchange",
            "kucoin": "kucoin",
            "bitfinex": "bitfinex",
            "bybit": "bybit",
        }

        send_message(token=bot_key, message=f"{dt.today().date()} START LISTING DB RENEW", chat_id=chat_id)

        tool = Tools()
        grabber = Grabber()

        listing_db = grabber.get_listing_info(exchange_info=exchange_info)
        woo_listing = grabber.get_woo_listing()
        listing_db = pd.concat([listing_db, woo_listing], axis=0)

        # update to mongo DB
        listing_mongo_db = tool.init_collection("TradingVolumeDB", "Listing")
        exchange_lst = listing_db["Exchange"].unique().tolist()
        for exchange in exchange_lst:
            filter_ = {"Exchange": exchange}
            listing_mongo_db.delete_many(filter_)
            listing_mongo_db.insert_many(listing_db.query("Exchange == @exchange").to_dict("records"))

        send_message(token=bot_key, message=f"{dt.today().date()} FINISH LISTING DB RENEW", chat_id=chat_id)


class VolumeJob:
    def __init__(self, config):
        self.config = config
        self.name = "VolumeJob"

    def run(self, num):
        bot_key = self.config["BOT_KEY"]
        chat_id = self.config["DAVID_CHAT_ID"]
        send_message(token=bot_key, message=f"{dt.today().date()} START VOLUME DB RENEW", chat_id=chat_id)

        grabber = Grabber()
        tools = Tools()
        token_info = grabber.get_token_info(num=num).reset_index()

        # update to mongo DB
        volume_mongo_db = tools.init_collection("TradingVolumeDB", "Volume")
        volume_mongo_db.insert_many(token_info.to_dict("records"))

        send_message(token=bot_key, message=f"{dt.today().date()} FINISH VOLUME DB RENEW", chat_id=chat_id)


class ReportJob:
    def __init__(self, config):
        self.config = config
        self.tools = Tools()
        self.formatter = Formatter()
        self.name = "ReportJob"

    def run(self, date: str, cat: str, num: int, test: bool = False):
        if date is None:
            date = (dt.today() - td(days=1)).date().strftime("%Y%m%d")

        bot_key = self.config["BOT_KEY"]
        chat_id = self.config["REPORT_CHAT_ID"] if not test else self.config["DAVID_CHAT_ID"]

        # top 10
        top_volume_tokens = self.tools.get_unlisted_token_with_top_volume(date, cat)
        tier1_tokens = top_volume_tokens.query("Tier == 1").iloc[:num].drop(columns=["Tier"])
        tier2_tokens = top_volume_tokens.query("Tier == 2").iloc[:num].drop(columns=["Tier"])
        tier1_table = self.formatter.create_bt_from_df(tier1_tokens, name=f"top")
        tier2_table = self.formatter.create_bt_from_df(tier2_tokens, name=f"top")

        # new 10
        new_tokens = (
            self.tools.get_new_tokens_in_top_volume(date, cat, 200).query("Tier == 1").iloc[:num].drop(columns=["Tier"])
        )
        new_tokens_table = self.formatter.create_bt_from_df(new_tokens, name=f"new")

        message = (
            f"<b>TOP TRADING VOLUME TOKENS REPORT\n\n"
            f"Date: {self.tools.get_dates_dict(date)['current_week'][0]}~"
            f"{self.tools.get_dates_dict(date)['current_week'][-1]}\n"
            f"Category: {cat}\n\n</b>"
            f"<a>1. Top {num} trading volume tokens unlisted on WOO:\n"
            f"1.1 ⚠️Tier 1 ⚠️:\n</a>{tier1_table}<a>\n"
            f"1.2 Tier 2:\n</a>"
            f"{tier2_table}\n\n"
        )

        message2 = f"<a>2. New tokens in top 200 :\n" f"2.1 ⚠ Tier 1 ⚠:\n</a>" f"{new_tokens_table}"

        for msg in [message, message2]:
            send_message(token=bot_key, message=msg, chat_id=chat_id)


class FillMongoDBJob:
    def __init__(self, config):
        self.config = config
        self.name = "FillMongoDBJob"

    def run(self):
        bot_key = self.config["BOT_KEY"]
        chat_id = self.config["DAVID_CHAT_ID"]

        tools = Tools()
        volume_length = tools.fill_mongodb(fill_type="volume")
        exchange_length = tools.fill_mongodb(fill_type="listing")

        send_message(
            token=bot_key,
            message=f"{dt.today().date()} FINISH FILLING MONGODB\n"
            f"Volume DB: {volume_length} dates\n"
            f"Exchange DB: {exchange_length} exchanges",
            chat_id=chat_id,
        )
