import argparse
import json
import logging
import math
import os
from datetime import datetime as dt
from datetime import timedelta as td
from decimal import Decimal
from typing import Optional

import numpy as np
import pandas as pd
import pygsheets
import pymongo as pm
import requests as rq
from beautifultable import BeautifulTable


def send_message(token, message, chat_id):
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&" f"text={message}&parse_mode=HTML"
    rq.get(url)


class BaseClient:
    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    CONFIG_PATH = os.path.join(CURRENT_PATH, "config.json")

    BLACK_LIST = ["USDT", "USDC", "USDP", "DAI", "USDD", "TUSD", "BUSD", "WBTC", "FDUSD", "LUNA"]

    def _init_config(self):
        return json.load(open(self.CONFIG_PATH, "r"))

    @staticmethod
    def _init_args():
        parser = argparse.ArgumentParser(description="Manage jobs")
        parser.add_argument("--volume", action="store_true", help="Update trading volume")
        parser.add_argument("--volume_num", type=int, help="", default=1000)
        parser.add_argument("--listing", action="store_true", help="Update listing")
        parser.add_argument("--cleaning", action="store_true", help="Clean db")
        parser.add_argument("--report", action="store_true", help="Send weekly report")
        parser.add_argument("--report_cat", type=str, help="", default="total")
        parser.add_argument("--report_num", type=int, help="", default=10)
        parser.add_argument("--date", type=str, help="Date of the report")
        parser.add_argument("--fill_mongodb", action="store_true", help="Fill mongodb")

        return parser

    @staticmethod
    def _init_logger():
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class Formatter:
    @staticmethod
    def millify(n, k: int = 3) -> str:
        def remove_exponent(num):
            return num.to_integral() if num == num.to_integral() else num.normalize()

        millnames = ["", "K", "M", "B", "T"]
        n = float(n)
        millidx = max(0, min(len(millnames) - 1, int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))))

        simplified_num = n / 10 ** (3 * millidx)
        digit = max(k - (len(str(simplified_num).split(".")[0])), 0)

        simplified_num = remove_exponent(Decimal(str(round(simplified_num, digit))))

        return f"{simplified_num}{millnames[millidx]}"

    def create_bt_from_df(self, df: pd.DataFrame, name: str) -> str:
        table = BeautifulTable()
        table.columns.header = df.columns.to_list()

        if name == "top":
            for index, row in df.iterrows():
                table.rows.append(
                    [
                        row[0],
                        self.millify(row[1]),
                        f"{int(row[2]*100)}%",
                        self.millify(row[3], k=1),
                    ]
                )

        elif name == "new":
            for index, row in df.iterrows():
                table.rows.append(
                    [
                        row[0],
                        self.millify(row[1]),
                        "-" if row[2] == np.inf else f"{int(row[2]*100)}%",
                        self.millify(row[3], k=1),
                    ]
                )

        for i in range(len(df.columns)):
            if i == 0:
                table.columns.alignment[df.columns[i]] = BeautifulTable.ALIGN_LEFT
            else:
                table.columns.alignment[df.columns[i]] = BeautifulTable.ALIGN_RIGHT

        table.set_style(BeautifulTable.STYLE_BOX)

        data_width = 32 // (len(df.columns) - 1)
        table.columns.width = [8] + [data_width] * (len(df.columns) - 1)

        table_text = f"<pre>{table}</pre>"
        return table_text


class Grabber(BaseClient):
    CMC_API_KEY = "CMC_API_KEY"
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
                token = [i["symbol"].split("_")[1].upper().replace("1000", "") for i in response["rows"]]
                cat = [i["symbol"].split("_")[0].lower() for i in response["rows"]]
                cat = ["perpetual" if i == "perp" else i for i in cat]
                woo = pd.DataFrame({"symbol": token, "type": cat})
                woo["exchange"] = "WOO"
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
        token_info.query(f"symbol not in {self.BLACK_LIST}", inplace=True)

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

    def get_listing_info(self, exchange_info: dict) -> pd.DataFrame:

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


class Tools(BaseClient, Formatter):
    VOLUME_DB_PATH = os.path.join(BaseClient.CURRENT_PATH, "../db/volume_db.csv")
    LISTING_DB_PATH = os.path.join(BaseClient.CURRENT_PATH, "../db/listing_db.csv")
    ARCHIVE_DB_PATH = os.path.join(BaseClient.CURRENT_PATH, "../db/archive")
    GCS_KEY_PATH = os.path.join(BaseClient.CURRENT_PATH, "gcs_key.json")
    ONLINE_VOLUME_DB_URL = (
        "https://docs.google.com/spreadsheets/d/1wfz0T-dtWScZ1WyrfSY2rjyV6kQ4CiCh07hywLZjLpQ/edit?usp=sharing"
    )
    MONGO_URL = "MONGO_URL"

    TIER1_EXCHANGE = ["binance", "houbi", "okx"]
    TIER2_EXCHANGE = ["gate-io", "kraken", "coinbase", "crypto.com", "kucoin", "bitfinex", "bybit"]

    def __init__(self):
        self.volume_db = self._init_volume_db()
        self.listing_db = self._init_listing_db()
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
            "archive": {
                "path": self.ARCHIVE_DB_PATH,
                "type": "folder",
            },
        }

        self.config = self._init_config()
        self.mongo_client = self.init_mongo_client()

    def _init_volume_db(self):
        if os.path.exists(self.VOLUME_DB_PATH):
            return pd.read_csv(self.VOLUME_DB_PATH)
        else:
            return pd.DataFrame()

    def _init_listing_db(self):
        if os.path.exists(self.LISTING_DB_PATH):
            return pd.read_csv(self.LISTING_DB_PATH)
        else:
            return pd.DataFrame()

    def _init_gcs(self):
        return pygsheets.authorize(service_file=self.GCS_KEY_PATH)

    def to_db(
        self, name: str, data: pd.DataFrame, index: Optional[bool] = False, file_name: Optional[str] = None
    ) -> bool:
        if name in self.db_map.keys():
            if self.db_map[name]["type"] == "csv":
                data.to_csv(self.db_map[name]["path"], index=index)
                return True
            elif self.db_map[name]["type"] == "folder":
                data.to_csv(os.path.join(self.db_map[name]["path"], f"{file_name}.csv"), index=index)
                return True

    def to_online_db(self, name: str, data: pd.DataFrame, index: Optional[bool] = False) -> bool:
        if name in self.db_map.keys():
            if self.db_map[name]["type"] == "csv":
                if index:
                    data = data.reset_index()
                self.db_map[name]["online"][0].set_dataframe(data, "A1")
                return True

    def get_logger(self, name: str) -> logging.Logger:
        log_paths = {
            "cleaning": os.path.join(BaseClient.CURRENT_PATH, "../log/cleaning/cleaning.log"),
            "volume": os.path.join(BaseClient.CURRENT_PATH, "../log/volume/volume.log"),
            "report": os.path.join(BaseClient.CURRENT_PATH, "../log/report/report.log"),
        }
        return logging.getLogger(name)

    def clean_volume_db(self, last_date: dt.date) -> None:
        volume_db = self.volume_db.copy()
        volume_db["date"] = pd.to_datetime(volume_db["date"])
        old_volume_db = volume_db.query("date < @last_date")
        new_volume_db = volume_db.query("date >= @last_date")

        distinct_date = [dt.strftime(i, "%Y-%m-%d") for i in sorted(old_volume_db["date"].unique())]
        for i in distinct_date:
            print(i)
            self.to_db(name="archive", data=old_volume_db.query("date == @i"), file_name=i)

        self.to_db(name="volume", data=new_volume_db, index=False)
        self.to_online_db(name="volume", data=new_volume_db, index=False)
        return len(distinct_date)

    @staticmethod
    def get_dates_dict(date: str) -> dict:
        input_date = dt.strptime(date, "%Y%m%d")
        weekday = input_date.weekday()

        current_week = [dt.strftime(input_date - td(days=i), "%Y-%m-%d") for i in range(weekday + 1)]
        last_week = [dt.strftime(input_date - td(days=i), "%Y-%m-%d") for i in range(weekday + 1, weekday + 8)]

        current_week.reverse()
        last_week.reverse()

        return {"current_week": current_week, "last_week": last_week}

    def _get_volume_record(self, dates: list) -> pd.DataFrame:
        volume_db_columns = [
            "date",
            "symbol",
            "total_volume",
            "spot_volume",
            "perpetual_volume",
            "cmc_rank",
        ]

        volume_db = self.volume_db[volume_db_columns].query("date in @dates")

        return volume_db.drop(["date"], axis=1)

    def _get_exchange_listing(self, exchange: str, cat) -> list:
        if cat == "total":
            return sorted(self.listing_db.query("exchange == @exchange")["symbol"].unique().tolist())
        else:
            return sorted(self.listing_db.query("exchange == @exchange and type == @cat")["symbol"].unique().tolist())

    def get_unlisted_token_with_top_volume(self, date: str, cat: str) -> pd.DataFrame:
        date_dict = self.get_dates_dict(date)
        woo_listing = self._get_exchange_listing("WOO", cat)
        vol = self._get_volume_record(date_dict["current_week"])

        vol = (
            vol.query("symbol not in @woo_listing and symbol not in @self.BLACK_LIST")
            .groupby("symbol")
            .mean()
            .sort_values(by=f"{cat}_volume", ascending=False)
            .reset_index()
            .query(f"{cat}_volume > 0")
            .eval(
                f"{'spot' if cat == 'total' else cat}_percentage = "
                f"({'spot' if cat == 'total' else cat}_volume / total_volume)"
            )
        )

        vol["tier"] = vol["symbol"].apply(lambda x: self._get_token_tiers(symbol=x, cat=cat))

        if cat == "total":
            columns_map = {
                "symbol": "Ccy",
                "total_volume": "Volume (USD)",
                "spot_percentage": "Spot perc (%)",
                "cmc_rank": "Cap rank",
                "tier": "Tier",
            }
        else:
            columns_map = {
                "symbol": "Ccy",
                f"{cat}_volume": f"Volume (USD)",
                f"{cat}_percentage": f"{cat} perc (%)",
                "cmc_rank": "Cap rank",
                "tier": "Tier",
            }

        return vol[list(columns_map.keys())].rename(columns=columns_map)

    def get_new_tokens_in_top_volume(self, date: str, cat: str, num: int) -> pd.DataFrame:
        date_dict = self.get_dates_dict(date)
        woo_listing = self._get_exchange_listing("WOO", cat)
        vol = self._get_volume_record(date_dict["current_week"])
        last_vol = self._get_volume_record(date_dict["last_week"])

        # exclude woo listing and black list and calculate mean value
        vol = (
            vol.query("symbol not in @woo_listing and symbol not in @self.BLACK_LIST")
            .groupby("symbol")
            .mean()
            .sort_values(by=f"{cat}_volume", ascending=False)
            .reset_index()
            .query(f"{cat}_volume > 0")
            .eval(
                f"{'spot' if cat == 'total' else cat}_percentage = "
                f"({'spot' if cat == 'total' else cat}_volume / total_volume)"
            )
            .iloc[:num]
        )
        last_vol = (
            last_vol.query("symbol not in @woo_listing and symbol not in @self.BLACK_LIST")
            .groupby("symbol")
            .mean()
            .sort_values(by=f"{cat}_volume", ascending=False)
            .reset_index()
            .query(f"{cat}_volume > 0")
            .eval(
                f"{'spot' if cat == 'total' else cat}_percentage = "
                f"({'spot' if cat == 'total' else cat}_volume / total_volume)"
            )
        )
        last_vol_currency = last_vol["symbol"].tolist()[:num]

        # get new currency in this week
        new_tokens = vol.query("symbol not in @last_vol_currency")
        new_tokens[f"last_{cat}_volume"] = (
            new_tokens["symbol"].apply(lambda x: last_vol.query("symbol == @x")[f"{cat}_volume"].values[0]).fillna(0)
        )  # fillna to avoid nan

        new_tokens = new_tokens.eval(f"{cat}_growth_rate = {cat}_volume / last_{cat}_volume - 1")
        new_tokens["tier"] = new_tokens["symbol"].apply(lambda x: self._get_token_tiers(symbol=x, cat=cat))

        columns_map = {
            "symbol": "Ccy",
            f"{cat}_volume": f"volume (USD)",
            f"{cat}_growth_rate": "Growth rate (%)",
            "cmc_rank": "Cap rank",
            "tier": "Tier",
        }

        return new_tokens[list(columns_map.keys())].rename(columns=columns_map)

    def _get_token_tiers(self, symbol: str, cat: str) -> int:
        """
        Tier definition:
        Tier 1: list on 2 of the 3 exchanges (Binance, Huobi, OKX)
        Tier 2: list on 1 of the 3 exchanges (Binance, Huobi, OKX) and 3 of the other exchanges
        """

        tier1 = []
        for i in self.TIER1_EXCHANGE:
            if symbol in self._get_exchange_listing(i, cat):
                tier1.append(i)

        tier2 = []
        for i in self.TIER2_EXCHANGE:
            if symbol in self._get_exchange_listing(i, cat):
                tier2.append(i)

        if len(tier1) >= 2:
            return 1
        elif len(tier1) == 1 and len(tier2) >= 3:
            return 2
        else:
            return 3

    def fill_missing_symbol(self):
        volume_db = self.volume_db.copy()
        symbol_record = volume_db.loc[volume_db["symbol"].notna()].set_index(["slug", "name"])
        missing_symbol_record = volume_db.loc[volume_db["symbol"].isna()]

        for _, row in missing_symbol_record.iterrows():
            slug = row["slug"]
            name = row["name"]
            if (slug, name) in symbol_record.index:
                missing_symbol_record.loc[_, "symbol"] = symbol_record.loc[(slug, name), "symbol"]
            else:
                missing_symbol_record.loc[_, "symbol"] = np.nan

        symbol_record.reset_index(inplace=True)

        new_volume_db = pd.concat([symbol_record, missing_symbol_record], axis=0)
        self.to_db(name="volume", data=new_volume_db, index=False)
        self.to_online_db(name="volume", data=new_volume_db, index=False)
        return True

    def fill_mongodb(self, fill_type: str):
        if fill_type == "volume":
            volume_mongo_db = self.init_collection(db="TradingVolumeDB", name="Volume")
            volume_csv_db = self.volume_db.copy()

            date_lst = volume_csv_db["date"].unique().tolist()
            for date in date_lst:
                filter_ = {"date": date}
                volume_mongo_db.delete_many(filter_)
                volume_mongo_db.insert_many(volume_csv_db.query("date == @date").to_dict("records"))

            return len(date_lst)
        elif fill_type == "listing":
            listing_mongo_db = self.init_collection(db="TradingVolumeDB", name="ListingInfo")
            listing_csv_db = self.listing_db.copy()

            exchange_lst = listing_csv_db["exchange"].unique().tolist()
            for exchange in exchange_lst:
                filter_ = {"exchange": exchange}
                listing_mongo_db.delete_many(filter_)
                listing_mongo_db.insert_many(listing_csv_db.query("exchange == @exchange").to_dict("records"))

            return len(exchange_lst)

    def init_mongo_client(self) -> pm.MongoClient:
        return pm.MongoClient(self.config[self.MONGO_URL])

    def init_collection(self, db: str, name: str) -> pm.collection.Collection:
        return self.mongo_client[db][name]
