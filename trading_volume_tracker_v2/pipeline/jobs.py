from datetime import datetime as dt
from typing import Optional

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
        tool.to_db(name="listing", data=listing_db, index=False)

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
        token_info = grabber.get_token_info(num=num)
        new_db = pd.concat([tools.volume_db, token_info], axis=0)
        tools.to_db(name="volume", data=new_db, index=True)
        tools.to_online_db(name="volume", data=new_db, index=True)

        send_message(token=bot_key, message=f"{dt.today().date()} FINISH VOLUME DB RENEW", chat_id=chat_id)


class CleaningJob:
    def __init__(self, config):
        self.config = config
        self.name = "CleaningJob"

    def run(self):
        pass


class ReportJob:
    def __init__(self, config):
        self.config = config
        self.tools = Tools()
        self.formatter = Formatter()
        self.name = "ReportJob"

    def run(self, date: str, cat: Optional[str] = "total", num: Optional[int] = 10):
        bot_key = self.config["BOT_KEY"]
        chat_id = self.config["DAVID_CHAT_ID"]

        # top 10
        unlisted_token_with_top_volume = (
            self.tools.get_unlisted_token_with_top_volume(date, cat)
            .iloc[:num]
            .query("Tier == 1")
            .drop(columns=["Tier"])
        )
        unlisted_token_with_top_volume_table = self.formatter.create_bt_from_df(
            unlisted_token_with_top_volume.iloc[:num], name=f"top"
        )

        # new 10
        new_tokens = (
            self.tools.get_new_tokens_in_top_volume(date, cat, 200).iloc[:num].query("Tier == 1").drop(columns=["Tier"])
        )
        new_tokens_table = self.formatter.create_bt_from_df(new_tokens, name=f"new")
