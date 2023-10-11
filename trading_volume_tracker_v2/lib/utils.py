import argparse
import json
import logging
import os
from datetime import datetime as dt
from typing import Optional

import pandas as pd
import pygsheets
import requests as rq


def send_message(token, message, chat_id):
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&" f"text={message}&parse_mode=HTML"
    rq.get(url)


class BaseClient:
    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    CONFIG_PATH = os.path.join(CURRENT_PATH, "config.json")

    def _init_config(self):
        return json.load(open(self.CONFIG_PATH, "r"))

    @staticmethod
    def _init_args():
        parser = argparse.ArgumentParser(description="Manage jobs")
        parser.add_argument("--volume", action="store_true", help="Update trading volume")
        parser.add_argument("--listing", action="store_true", help="Update listing")
        parser.add_argument("--cleaning", action="store_true", help="Clean db")
        parser.add_argument("--num", type=int, help="Number of items")
        return parser

    @staticmethod
    def _init_logger():
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class Grabber(BaseClient):
    CMC_API_KEY = "CMC_API_KEY"
    BLACK_LIST = "BLACK_LIST"
    EXCHANGE_INFO = "EXCHANGE_INFO"
    TOKEN_INFO_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

    WOO_LISTING_URL = "https://api.woo.org/v1/public/info"

    def __init__(self):
        self._init_logger()
        self.config = self._init_config()

    @staticmethod
    def _handle_response(response: dict, category: str, **kwargs):
        try:
            if category == "token_info":
                token_info = pd.DataFrame(response["data"])[["id", "name", "slug", "symbol", "cmc_rank"]].loc[
                    : kwargs["num"]
                ]
                token_info["date"] = dt.today().date()
                return token_info
            elif category == "volume":
                try:
                    volume = pd.DataFrame(response["data"]["marketPairs"])[["volumeUsd"]]
                    volume.columns = [kwargs["symbol"]]
                    return volume
                except KeyError as e:
                    logging.warning(f"{kwargs['symbol']} KeyError: {e}")
                    return pd.DataFrame()

            elif category == "market_number":
                return int(response["data"]["numMarketPairs"])

            elif category == "listing":
                return [i["baseSymbol"] for i in response["data"]["marketPairs"]]

            elif category == "woo_listing":
                token = [i["symbol"].split("_")[1].upper() for i in response["rows"]]
                cat = [i["symbol"].split("_")[0].lower() for i in response["rows"]]
                cat = ["perpetual" if i == "perp" else i for i in cat]
                woo = pd.DataFrame({"symbol": token, "type": cat})
                woo["exchange"] = "WOO Network"
                return woo

        except Exception as e:
            logging.error(f"Error: {e} - Category: {category} - Response: {response}")
            if category == "token_info":
                return pd.DataFrame()
            elif category == "volume":
                return pd.DataFrame()
            elif category == "market_number":
                return 0
            elif category == "listing":
                return []

    @staticmethod
    def _create_volume_url(
        slug: str, start: Optional[int] = 1, limit: Optional[int] = 1, cat: Optional[str] = "spot"
    ) -> str:
        return (
            f"https://api.coinmarketcap.com/data-api/v3/cryptocurrency/market-pairs/latest?"
            f"slug={slug}&start={start}&limit={limit}&category={cat}&centerType=all&sort=cmc_rank_advanced"
        )

    @staticmethod
    def _create_listing_url(
        slug: str, start: Optional[int] = 1, limit: Optional[int] = 1, cat: Optional[str] = "spot"
    ) -> str:
        return (
            f"https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?"
            f"slug={slug}&category={cat}&start={start}&limit={limit}"
        )

    def _get_token_info(self, num: int) -> pd.DataFrame:
        params = {"start": "1", "limit": num, "convert": "USD"}
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": self.config[self.CMC_API_KEY],
        }
        session = rq.Session()
        session.headers.update(headers)

        response = session.get(self.TOKEN_INFO_URL, params=params).json()
        return self._handle_response(response=response, category="token_info", num=num)

    def _get_token_volume(self, token: str, limit: Optional[int] = 1000, cat: Optional[str] = "spot") -> int:
        market_number = self._get_market_number(token=token, cat=cat)
        if market_number == 0:
            return 0

        start = 1
        results = pd.DataFrame()
        while start <= market_number:
            url = self._create_volume_url(slug=token, start=start, limit=limit, cat=cat)
            response = rq.get(url).json()
            results = pd.concat(
                [results, self._handle_response(response=response, category="volume", symbol=token)], axis=0
            )
            start += limit
        logging.info(f"Token: {token} - Cat: {cat} - Volume: {results[token].sum()}")
        return results[token].sum()

    def _get_market_number(self, token: str, cat: str) -> int:
        url = self._create_volume_url(slug=token, cat=cat)
        response = rq.get(url).json()
        logging.info(
            f"Token: {token} - "
            f"Cat: {cat} - "
            f"Market Number: {self._handle_response(response=response, category='market_number')}"
        )
        return self._handle_response(response=response, category="market_number")

    def get_token_info(self, num: Optional[int] = 1000):
        token_info = self._get_token_info(num=num)
        token_info.query(f"symbol not in {self.config[self.BLACK_LIST]}", inplace=True)

        for cat in ["spot", "perpetual"]:
            token_info[f"{cat}_volume"] = token_info["slug"].apply(lambda x: self._get_token_volume(token=x, cat=cat))
            token_info[f"{cat}_market_num"] = token_info["slug"].apply(
                lambda x: self._get_market_number(token=x, cat=cat)
            )
            token_info["updated_time"] = dt.now()
        token_info["total_volume"] = token_info["spot_volume"] + token_info["perpetual_volume"]
        token_info["spot_percentage"] = token_info["spot_volume"] / token_info["total_volume"]
        token_info["perpetual_percentage"] = token_info["perpetual_volume"] / token_info["total_volume"]

        token_info["spot_volume_rank"] = token_info["spot_volume"].rank(ascending=False)
        token_info["perpetual_volume_rank"] = token_info["perpetual_volume"].rank(ascending=False)
        token_info["total_volume_rank"] = token_info["total_volume"].rank(ascending=False)

        token_info.fillna(0, inplace=True)
        return token_info.set_index("symbol")

    def _get_listing_info(self, slug: str, cat: Optional[str] = "spot", limit: Optional[int] = 1000) -> list:
        market_number = self._get_exchange_market_number(slug=slug, cat=cat)
        if market_number == 0:
            return []

        start = 1
        results = []
        while start <= market_number:
            url = self._create_listing_url(slug=slug, cat=cat, start=start, limit=limit)
            response = rq.get(url).json()
            results += self._handle_response(response=response, category="listing")
            start += limit
        return list(set(results))

    def get_listing_info(self) -> pd.DataFrame:
        exchange_info = self.config[self.EXCHANGE_INFO]

        results = pd.DataFrame()

        for cat in ["spot", "perpetual"]:
            for name, slug in exchange_info.items():
                logging.info(f"Start getting listing info for {name} - {cat}")
                tokens = self._get_listing_info(slug=slug, cat=cat)
                result = pd.DataFrame({"exchange": name, "symbol": tokens, "type": cat})
                results = pd.concat([results, result], axis=0)

        return results

    def _get_exchange_market_number(self, slug: str, cat: str) -> int:
        url = self._create_listing_url(slug=slug, cat=cat)
        return self._handle_response(response=rq.get(url).json(), category="market_number")

    def get_woo_listing(self):
        response = rq.get(self.WOO_LISTING_URL).json()
        return self._handle_response(response=response, category="woo_listing")


class Tools(BaseClient):
    VOLUME_DB_PATH = os.path.join(BaseClient.CURRENT_PATH, "../db/volume_db.csv")
    LISTING_DB_PATH = os.path.join(BaseClient.CURRENT_PATH, "../db/listing_db.csv")
    GCS_KEY_PATH = os.path.join(BaseClient.CURRENT_PATH, "gcs_key.json")
    ONLINE_VOLUME_DB_URL = (
        "https://docs.google.com/spreadsheets/d/1wfz0T-dtWScZ1WyrfSY2rjyV6kQ4CiCh07hywLZjLpQ/edit?usp=sharing"
    )

    def __init__(self):
        self.volume_db = self._init_volume_db()
        self.gcs = self._init_gcs()
        self.db_map = {
            "volume": {
                "path": self.VOLUME_DB_PATH,
                "type": "csv",
                "online": self.gcs.open_by_url(self.ONLINE_VOLUME_DB_URL),
            },
            "listing": {
                "path": self.LISTING_DB_PATH,
                "type": "csv",
            },
        }

    def _init_volume_db(self):
        if os.path.exists(self.VOLUME_DB_PATH):
            return pd.read_csv(self.VOLUME_DB_PATH).set_index("symbol")
        else:
            return pd.DataFrame()

    def _init_listing_db(self):
        if os.path.exists(self.LISTING_DB_PATH):
            return pd.read_csv(self.LISTING_DB_PATH).set_index("symbol")
        else:
            return pd.DataFrame()

    def _init_gcs(self):
        return pygsheets.authorize(service_file=self.GCS_KEY_PATH)

    def to_db(self, name: str, data: pd.DataFrame, index: Optional[bool] = False) -> bool:
        if name in self.db_map.keys():
            if self.db_map[name]["type"] == "csv":
                data.to_csv(self.db_map[name]["path"], index=index)
                return True

    def to_online_db(self, name: str, data: pd.DataFrame, index: Optional[bool] = False) -> bool:
        if name in self.db_map.keys():
            if self.db_map[name]["type"] == "csv":
                if index:
                    data = data.reset_index()
                self.db_map[name]["online"][0].set_dataframe(data, "A1")
                return True

    def _get_volume_record(self, date):
        pass

    def get_unlisted_token_with_top_volume(self, cat: str) -> str:
        pass
