import json
import logging
import os
from logging import handlers

import pandas as pd


class Tools:
    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    CONFIG_PATH = os.path.join(CURRENT_PATH, "config.json")
    PERMISSION_PATH = os.path.join(CURRENT_PATH, "permission.csv")

    INFO_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/info/chat_info.log")
    MAIN_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/main/main.log")

    def __init__(self):
        self.config = self.init_config()
        self.permission = self.init_permission()

    def init_config(self) -> dict:
        return json.load(open(self.CONFIG_PATH, "r"))

    def init_permission(self) -> pd.DataFrame:
        return pd.read_csv(self.PERMISSION_PATH, index_col=None)

    def in_whitelist(self, id: str) -> bool:
        return id in self.permission.query("whitelist == 1")["id"].tolist()

    def is_admin(self, id: str) -> bool:
        return id in self.permission.query("admin == 1")["id"].tolist()

    def get_logger(self, name: str):

        log_path_map = {"info": self.INFO_BOT_LOG_PATH, "main": self.MAIN_BOT_LOG_PATH}
        logging.basicConfig(
            handlers=[
                logging.FileHandler(log_path_map[name], "a", "utf-8"),
            ],
            format="%(asctime)s - %(levelname)s - %(message)s",
            level=logging.INFO,
        )
        logger = logging.getLogger(name=name)

        logHandler = handlers.TimedRotatingFileHandler(log_path_map[name], when="D", interval=1)
        logger.addHandler(logHandler)

        return logger
