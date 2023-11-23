import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import pymongo as pm
from telegram import (
    Chat,
    ChatMember,
    ChatMemberUpdated,
    Message,
    MessageEntity,
    Update,
    User,
)


class Tools:
    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))

    MONGO_URL = "mongodb://localhost:27017/"
    OLD_CHAT_INFO_PATH = CURRENT_PATH + "/../db/chat/chat_info.csv"

    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    CONFIG_PATH = os.path.join(CURRENT_PATH, "config.json")
    PERMISSION_PATH = os.path.join(CURRENT_PATH, "permission.csv")

    INFO_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/info/chat_info.log")
    MAIN_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/main/main.log")

    def __init__(self):
        self.config = self.init_config()
        self.mongo_client = self.init_mongo_client()
        self.permission = self.init_collection("AnnouncementDB", "Permissions")

    def init_config(self) -> dict:
        return json.load(open(self.CONFIG_PATH, "r"))

    def init_permission(self) -> pd.DataFrame:
        return pd.read_csv(self.PERMISSION_PATH, index_col=None)

    def init_mongo_client(self) -> pm.MongoClient:
        return pm.MongoClient(self.MONGO_URL)

    def init_collection(self, db_name: str, collection_name: str) -> pm.collection.Collection:
        return self.mongo_client[db_name][collection_name]

    def in_whitelist(self, id: str) -> bool:
        filter_ = {"id": id}
        result = self.permission.find_one(filter_)
        if result is None:
            return False
        for i in result:
            return i["whitelist"]

    def is_admin(self, id: str) -> bool:
        filter_ = {"id": id}
        result = self.permission.find(filter_)
        if result is None:
            return False
        for i in result:
            return i["admin"]

    # This is the old version of init_chatinfo, new is through tools.init_collection('AnnouncementDB', 'ChatInfo'),
    # then will return a Collection object
    def init_chatinfo(self) -> pd.DataFrame:
        return pd.read_csv(self.OLD_CHAT_INFO_PATH, index_col=None)

    def get_logger(self, name: str):
        log_path_map = {"InfoBot": self.INFO_BOT_LOG_PATH, "MainBot": self.MAIN_BOT_LOG_PATH}

        if not os.path.exists(log_path_map[name]):
            open(log_path_map[name], "w").close()

        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.setLevel(logging.WARNING)

            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

            file_handler = logging.FileHandler(log_path_map[name], "a", "utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        logger.propagate = False
        return logger


class TGTestCases:
    @staticmethod
    def create_chat_member_updated(chat_id, from_user, old_status, new_status, bot_user):
        chat = Chat(id=chat_id, type="group")
        old_chat_member = ChatMember(status=old_status, user=bot_user)
        new_chat_member = ChatMember(status=new_status, user=bot_user)
        return ChatMemberUpdated(
            chat=chat,
            from_user=from_user,
            date=datetime.now(timezone.utc),
            old_chat_member=old_chat_member,
            new_chat_member=new_chat_member,
        )

    def bot_left(self):
        from_user = User(
            id=5327851721,
            is_bot=False,
            first_name="David",
            last_name="Ding",
            username="Davidding_WG",
            language_code="en",
        )
        bot_user = User(id=6144182015, is_bot=True, first_name="TEST BOT FOR DAVID", username="davidting_test_bot")
        return Update(
            update_id=943740097,
            my_chat_member=self.create_chat_member_updated(-716792704, from_user, "member", "left", bot_user),
        )

    def bot_add(self):
        from_user = User(
            id=5327851721,
            is_bot=False,
            first_name="David",
            last_name="Ding",
            username="Davidding_WG",
            language_code="en",
        )
        bot_user = User(id=6144182015, is_bot=True, first_name="TEST BOT FOR DAVID", username="davidting_test_bot")
        return Update(
            update_id=943740099,
            my_chat_member=self.create_chat_member_updated(-716792704, from_user, "left", "member", bot_user),
        )

    def bot_command(self, command: str):
        chat = Chat(first_name="David", id=5327851721, last_name="Ding", type=Chat.PRIVATE, username="Davidding_WG")
        from_user = User(
            first_name="David",
            id=5327851721,
            is_bot=False,
            language_code="en",
            last_name="Ding",
            username="Davidding_WG",
        )
        message_entity = MessageEntity(length=16, offset=0, type="bot_command")
        message = Message(
            message_id=199,
            from_user=from_user,
            date=datetime(2023, 11, 23, 16, 38, 43, tzinfo=timezone.utc),
            chat=chat,
            text="/fill_permission",
            entities=[message_entity],
            group_chat_created=False,
            supergroup_chat_created=False,
            channel_chat_created=False,
            delete_chat_photo=False,
        )
        return Update(update_id=943740120, message=message)


class ChatGroup:
    def __init__(
        self,
        id: str,
        type: str,
        name: str,
        label: list,
        description: str = None,
        add_time: str = None,
        update_time: str = None,
    ):
        self.id = id
        self.type = type
        self.name = name
        self.label = label
        self.description = description
        self.add_time = add_time
        self.update_time = update_time
        self.operator = None
        self.operator_id = None

    def add_label(self, label: str):
        if label not in self.label:
            self.label.append(label)
            return True


class Permission:
    def __init__(self, id: str, name: str, admin: bool = False, whitelist: bool = False, update_time: str = None):
        self.id = id
        self.name = name
        self.admin = admin
        self.whitelist = whitelist
        self.update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def update_permission(self, admin: bool = False, whitelist: bool = False):
        self.admin = admin
        self.whitelist = whitelist
        self.update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
