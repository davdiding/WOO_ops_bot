import json
import logging
import os
from datetime import datetime as dt

import pandas as pd
import pymongo as pm


def query_dict(dictionary: dict, query: str, query_env: dict = None) -> dict:
    """
    Query a dictionary with a query string
    :param dictionary: dictionary to query
    :param query: query string
    :param query_env: additional variables for query execution
    :return: queried dictionary
    """
    if not query:
        return dictionary

    df = pd.DataFrame(dictionary).T

    if query_env:
        df = df.query(query, local_dict=query_env)
    else:
        df = df.query(query)

    return df.to_dict(orient="index")


def nested_query_dict(dictionary: dict, key: str, query: str) -> dict:
    """
    Query a dictionary with a query string
    :param dictionary: dictionary to query
    :param query: query string
    :return: queried dictionary
    """
    if not query:
        return dictionary

    new_dict = {outer: inner[key] for outer, inner in dictionary.items() if key in inner}

    queried_dict = query_dict(new_dict, query)
    return {key: dictionary[key] for key in queried_dict.keys()}


def sort_dict(dictionary: dict, ascending: bool = True, num: int = None) -> dict:
    """
    Sort a dictionary by its values
    :param dictionary: dictionary to sort
    :param ascending: ascending or descending
    :param num: number of items to return
    :return: sorted dictionary
    """
    df = pd.DataFrame(dictionary).T.reset_index().sort_values(by="index", ascending=ascending).set_index("index")
    if num:
        df = df.iloc[-num:]
    return df.to_dict(orient="index")


class Tools(object):
    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    MONGO_URL = "MONGO_URL"

    CONFIG_PATH = os.path.join(CURRENT_PATH, "config.json")

    LOG_MAP = {
        "binance": os.path.join(CURRENT_PATH, "../log/binance/main.log"),
    }

    def __init__(self) -> None:
        self.config = self.init_config()
        self.mongo_client = self.init_mongo_client()
        self.logger = None

    def init_config(self) -> dict:
        with open(self.CONFIG_PATH) as f:
            config = json.load(f)
            f.close()
        return config

    def init_mongo_client(self) -> pm.MongoClient:
        return pm.MongoClient(self.config[self.MONGO_URL])

    def init_collection(self, db: str, name: str) -> pm.collection.Collection:
        return self.mongo_client[db][name]

    @staticmethod
    def get_timestap() -> int:
        return int(dt.now().timestamp() * 1000)

    @staticmethod
    def get_today() -> int:
        return int(dt.combine(dt.today(), dt.min.time()).timestamp() * 1000)

    def get_logger(self, name: str) -> logging.Logger:

        logger = logging.getLogger(name)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(message)s")

        file_handler = logging.FileHandler(self.LOG_MAP[name])
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        self.logger = logger
        return self.logger

    @staticmethod
    def parse_str_to_timestamp(date_str: str, from_format: str = "%Y%m%d") -> int:
        return int(dt.strptime(date_str, from_format).timestamp() * 1000)

    @staticmethod
    def parse_timestamp_to_str(timestamp: int, to_format: str = "%Y%m%d") -> str:
        return dt.fromtimestamp(timestamp / 1000).strftime(to_format)
