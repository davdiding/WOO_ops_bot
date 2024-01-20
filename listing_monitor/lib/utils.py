import json
import logging
import os
from datetime import datetime as dt
from hashlib import md5

import pymongo as pm
from telegram import Bot


class Tool(object):
    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    CONFIG_PATH = os.path.join(CURRENT_PATH, "config.json")

    DB_URL = "MONGO_URL"
    LOG_MAP = {
        "main": os.path.join(CURRENT_PATH, "log/main.log"),
        "updater": os.path.join(CURRENT_PATH, "log/updater.log"),
    }

    def __init__(self):
        self.config = self.init_config()
        self.client = pm.MongoClient(self.config[self.DB_URL])
        self.logger = None

    def init_config(self) -> dict:
        with open(self.CONFIG_PATH) as f:
            config = json.load(f)
            f.close()
        return config

    def init_collection(self, db: str, name: str) -> pm.collection.Collection:
        return self.client[db][name]

    def get_timestamp(self) -> int:
        return int(dt.now().timestamp() * 1000)

    def get_datetime(self) -> str:
        return dt.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_logger(self, name: str) -> logging.Logger:

        logger = logging.getLogger(name)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(message)s")

        file_handler = logging.FileHandler(self.LOG_MAP[name])
        file_handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(file_handler)

        logger.setLevel(logging.INFO)
        logger.propagate = False

        self.logger = logger
        return self.logger

    def get_id(self, timestamp: int) -> str:
        signature = md5(str(timestamp).encode()).hexdigest()
        return signature

    def get_tg_bot(self, key: str) -> Bot:
        return Bot(key)
