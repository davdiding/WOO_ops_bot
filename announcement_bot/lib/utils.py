import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import pygsheets as pg
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
    MONGO_URL = "MONGO_DB_URL"

    OLD_CHAT_INFO_PATH = CURRENT_PATH + "/../db/chat/chat_info.csv"
    GC_KEY_PATH = CURRENT_PATH + "/../lib/gc_key.json"
    ONLINE_CHAT_INFO_URL = (
        "https://docs.google.com/spreadsheets/d/15yR0QEKG6axFxnxvOGYTwztE33yUsVo5xktloTthedE/edit?usp=sharing"
    )
    ONLIN_CHAT_INFO_TABLE_NAME = "Chat Infomation (formal)"
    CHAT_INFO_COLUMNS_MAP = {
        "name": "Name",
        "type": "Type",
        "add_time": "Added Time",
        "label": "Labels",
        "test_channel": "Test Channel",
        "maintenance": "Maintenance",
        "listing": "Listing",
        "delisting": "Delisting",
        "trading_suspension_resumption": "Trading Suspension / Resumption",
        "funding_rate": "Funding Rate",
        "dmm_program": "DMM Program",
        "vip_program": "VIP Program",
        "new_trading_competition": "New Trading Competition",
        "description": "Note",
    }
    ANNOUNCEMENT_INFO_COLUMNS_MAP = {}

    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    CONFIG_PATH = os.path.join(CURRENT_PATH, "config.json")
    PERMISSION_PATH = os.path.join(CURRENT_PATH, "permission.csv")

    INFO_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/info/chat_info.log")
    MAIN_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/main/main.log")

    def __init__(self):
        self.config = self.init_config()
        self.mongo_client = self.init_mongo_client()
        self.gc_client = self.init_gc_client()
        self.permission = self.init_collection("AnnouncementDB", "Permissions")
        self.logger = None

    def init_config(self) -> dict:
        return json.load(open(self.CONFIG_PATH, "r"))

    def init_permission(self) -> pd.DataFrame:
        return pd.read_csv(self.PERMISSION_PATH, index_col=None)

    def init_mongo_client(self) -> pm.MongoClient:
        return pm.MongoClient(self.config[self.MONGO_URL])

    def init_gc_client(self) -> pg.client.Client:
        return pg.authorize(service_file=self.GC_KEY_PATH)

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

    def init_online_sheet(self, url: str, name: str, to_type: str = "df") -> pd.DataFrame:
        ws = self.gc_client.open_by_url(url)
        sheet_names = [i.title for i in ws.worksheets()]

        if to_type == "df":
            if name not in sheet_names:
                return pd.DataFrame()
            else:
                return ws.worksheet_by_title(name).get_as_df()
        else:
            return ws.worksheet_by_title(name)

    def get_logger(self, name: str):
        log_path_map = {"InfoBot": self.INFO_BOT_LOG_PATH, "MainBot": self.MAIN_BOT_LOG_PATH}
        logger = logging.getLogger(name)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        file_handler = logging.FileHandler(log_path_map[name])
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        self.logger = logger
        return self.logger

    def get_columns_name(self, col: str, input: str) -> str:
        """
        param col: input column name
        param input: 'cl'/'cr'/'al'/'ar',
            'c' means chat,
            'a' means announcement,
            'l' means input is local name,
            'r' means input is remote name (online sheet)
        """

        if input == "cl":
            return self.CHAT_INFO_COLUMNS_MAP[col]
        elif input == "cr":
            for k, v in self.CHAT_INFO_COLUMNS_MAP.items():
                if v == col:
                    return k

            _col = col.replace(" / ", " ").replace(" ", "_").lower()  # This indicate a new category
            self.CHAT_INFO_COLUMNS_MAP[_col] = col
            return _col

        elif input == "al":
            return self.ANNOUNCEMENT_INFO_COLUMNS_MAP[col]
        elif input == "ar":
            for k, v in self.ANNOUNCEMENT_INFO_COLUMNS_MAP.items():
                if v == col:
                    return k

    def update_chat_info(self, update_type: str) -> None:
        """
        param direction: 'download'/'upload'/'init'
        1. download: download online sheet to mongoDB, detecting new category, new labels and notes,
                     will be run when user want to post announcement
        2. upload: upload mongoDB to online sheet, detecting new id, new name
        3. init: init online sheet from mongoDB, online DB only have columns name
        """
        online_chat_info = self.init_online_sheet(self.ONLINE_CHAT_INFO_URL, self.ONLIN_CHAT_INFO_TABLE_NAME)
        chat_info = self.init_collection("AnnouncementDB", "ChatInfo")

        if update_type == "init":
            drop_columns = ["_id", "id", "update_time", "operator", "operator_id"]
            chat_info = pd.DataFrame(list(chat_info.find({}))).drop(columns=drop_columns)
            chat_info["label"] = chat_info["label"].apply(lambda x: ",".join(x))
            chat_info = chat_info.replace({False: "x", True: ""})
            chat_info.columns = [self.get_columns_name(col, "cl") for col in chat_info.columns]
            chat_info = chat_info[list(self.CHAT_INFO_COLUMNS_MAP.values())]

            # write chat_info to online sheet
            ws = self.init_online_sheet(self.ONLINE_CHAT_INFO_URL, self.ONLIN_CHAT_INFO_TABLE_NAME, to_type="ws")
            ws.set_dataframe(chat_info, (1, 1))
            self.logger.info("Init online sheet from mongoDB successfully")
            return

        elif update_type == "download":
            for _, row in online_chat_info.iterrows():
                inputs = {self.get_columns_name(key, "cr"): value for key, value in row.items()}
                inputs["id"] = None
                online_chat = ChatGroup(**inputs)

                filter_ = {"name": online_chat.name}
                if chat_info.count_documents(filter_) > 1:
                    logging.warning(f"More than one chat has name: {online_chat.name}")
                    continue
                elif chat_info.count_documents(filter_) == 0:
                    logging.warning(f"Unknow chat: {online_chat.name} in online sheet")
                    continue

                del online_chat.id  # This chat_id is fake
                chat_info.update_one(filter_, {"$set": online_chat.__dict__})

            self.logger.info("Download online sheet to mongoDB successfully")
            return

        elif update_type == "upload":
            drop_columns = ["_id", "id", "update_time", "operator", "operator_id"]


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
            text=f"/{command}",
            entities=[message_entity],
            group_chat_created=False,
            supergroup_chat_created=False,
            channel_chat_created=False,
            delete_chat_photo=False,
        )
        return Update(update_id=943740120, message=message)

    def chat_rename(self):
        chat = Chat(
            id=-716792704,
            type=Chat.GROUP,
            title="v2<>test4_v2",
        )

        from_user = User(
            id=5327851721,
            is_bot=False,
            first_name="David",
            last_name="Ding",
            username="Davidding_WG",
            language_code="en",
        )

        message = Message(
            message_id=203,
            from_user=from_user,
            date=datetime(2023, 11, 26, 7, 6, 54, tzinfo=timezone.utc),
            chat=chat,
            new_chat_title="v2<>test4_v2",
            delete_chat_photo=False,
            group_chat_created=False,
            supergroup_chat_created=False,
            channel_chat_created=False,
        )

        return Update(update_id=943740124, message=message)


class ChatGroup:
    def __init__(
        self,
        id: any,
        type: str,
        name: str,
        label: list,
        description: str = None,
        add_time: str = None,
        update_time: str = None,
        operator: str = None,
        operator_id: str = None,
        **kwargs,
    ):
        self.id = str(id)
        self.type = type
        self.name = name
        self.label = label.split(",") if isinstance(label, str) else label
        self.description = self.handle_description(description)
        self.add_time = add_time
        self.update_time = update_time
        self.operator = operator
        self.operator_id = operator_id
        self.handle_kwargs(kwargs)

    def handle_description(self, description: any) -> str:
        if description is None:
            return ""
        elif isinstance(description, str):
            return description
        else:
            return ""

    def handle_kwargs(self, kwargs: dict):
        for k, v in kwargs.items():
            if v == "":
                v = True
            elif v == "x":
                v = False
            setattr(self, k, v)

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


class Announcement:
    pass
