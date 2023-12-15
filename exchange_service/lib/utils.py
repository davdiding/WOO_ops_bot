import json
import os

import pandas as pd
import pymongo as pm


def query_dict(dictionary: dict, query: str) -> dict:
    """
    Query a dictionary with a query string
    :param dictionary: dictionary to query
    :param query: query string
    :return: queried dictionary
    """
    if not query:
        return dictionary

    df = pd.DataFrame(dictionary).T
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

    def __init__(self) -> None:
        self.config = self.init_config()
        self.mongo_client = self.init_mongo_client()

    def init_config(self) -> dict:
        with open(self.CONFIG_PATH) as f:
            config = json.load(f)
            f.close()
        return config

    def init_mongo_client(self) -> pm.MongoClient:
        return pm.MongoClient(self.config[self.MONGO_URL])

    def init_collection(self, db: str, name: str) -> pm.collection.Collection:
        return self.mongo_client[db][name]
