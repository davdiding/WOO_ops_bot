from datetime import datetime as dt

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

    def run(self, date: str, cat: str, num: int):
        bot_key = self.config["BOT_KEY"]
        chat_id = self.config["DAVID_CHAT_ID"]

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

        message = f"""
        <b>TOP TRADING VOLUME TOKENS REPORT\n\n
        Date: {
        self.tools.get_dates_dict(date)['current_week'][0]
        }~{
        self.tools.get_dates_dict(date)['current_week'][-1]
        }\n
        Category: {cat}\n
        </b>
        <a>
        1. Top {num} trading volume tokens unlisted on WOO:\n
        1.1 ⚠️Tier 1 ⚠️:\n
        {tier1_table}\n
        1.2 Tier 2:\n
        {tier2_table}\n\n
        2. New tokens in top 200 :\n
        2.1 ⚠ Tier 1 ⚠:\n
        {new_tokens_table}\n
        </a>
        """

        send_message(token=bot_key, message=message, chat_id=chat_id)
