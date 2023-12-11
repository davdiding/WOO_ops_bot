import argparse
import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime

import pandas as pd
import pygsheets as pg
import pymongo as pm
import requests as rq
from telegram import Bot, Update


def init_args(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--test",
        action="store_true",
        default=False,
        help="Run in test mode, default is False",
    )
    return parser.parse_args()


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
    FIXED_COLUMNS = ["id", "create_time", "creator", "creator_id"]

    def __init__(
        self,
        id: str,
        create_time: datetime,
        creator: str,
        creator_id: str,
        category: str = None,
        language: str = None,
        labels: list = None,
        chats: list = None,
        content_type: str = None,
        content_text: str = None,
        content_html: str = None,
        file_path: str = None,
        available_chats: list = None,
        approved_time: datetime = None,
        approver: str = None,
        approver_id: str = None,
        record: list = None,
        status: str = None,
    ):
        self.id = id
        self.create_time = create_time
        self.creator = creator
        self.creator_id = str(creator_id)
        self.category = category
        self.language = language
        self.labels = labels
        self.chats = chats
        self.content_text = content_text
        self.content_html = content_html
        self.content_type = content_type
        self.file_path = file_path
        self.available_chats = available_chats
        self.approved_time = approved_time
        self.approver = approver
        self.approver_id = str(approver_id)
        self.record = record
        self.status = status

    def update(self, **kwargs):

        for k, v in kwargs.items():
            if k in self.__dict__ and k not in self.FIXED_COLUMNS:
                setattr(self, k, v)
            else:
                print(f"Unknow param of Announcement: {k}")


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
        "others": "Others",
        "description": "Note",
    }
    ANNOUNCEMENT_INFO_COLUMNS_MAP = {
        "english": "English",
        "chinese": "Chinese",
    }

    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    CONFIG_PATH = os.path.join(CURRENT_PATH, "config.json")
    PERMISSION_PATH = os.path.join(CURRENT_PATH, "permission.csv")
    FILE_PATH = os.path.join(CURRENT_PATH, "../db/files")

    INFO_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/info/chat_info.log")
    MAIN_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/main/main.log")

    def __init__(self):
        self.config = self.init_config()
        self.mongo_client = self.init_mongo_client()
        self.gc_client = self.init_gc_client()
        self.permission = self.init_collection("AnnouncementDB", "Permissions")
        self.logger = None

        self.update_columns_map()

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

    def in_whitelist(self, id: any) -> bool:
        filter_ = {"id": str(id)}
        result = self.permission.find(filter_)
        if result is None:
            return False
        for i in result:
            return i["whitelist"]

    def is_admin(self, id: any) -> bool:
        id = str(id)
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

    def update_columns_map(self):
        pass

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

    def get_annc_id(self) -> str:
        timestamp = str(datetime.now().timestamp() * 1000)
        signature = hashlib.sha256(timestamp.encode()).hexdigest()
        return signature

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
                   will be run when bot been added new chat or left chat or chat name changed
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

        # In current method, if an category was deleted, it will be deleted in mongoDB
        elif update_type == "download":
            missing_columns = ["id", "update_time", "operator", "operator_id"]
            for _, row in online_chat_info.iterrows():
                inputs = {self.get_columns_name(key, "cr"): value for key, value in row.items()}
                inputs["id"] = None
                online_chat = ChatGroup(**inputs)

                filter_ = {"name": online_chat.name}
                if chat_info.count_documents(filter_) > 1:
                    self.logger.warning(f"More than one chat has name: {online_chat.name}")
                    continue
                elif chat_info.count_documents(filter_) == 0:
                    self.logger.warning(f"Unknow chat: {online_chat.name} in online sheet")
                    continue
                else:
                    chat = chat_info.find_one(filter_)

                for i in missing_columns:
                    online_chat.__setattr__(i, chat[i])

                chat_info.delete_one(filter_)
                chat_info.insert_one(online_chat.__dict__)
            self.logger.info("Download online sheet to mongoDB successfully")
            return

        elif update_type == "upload":
            drop_columns = ["_id", "id", "update_time", "operator", "operator_id"]
            chat_info = pd.DataFrame(list(chat_info.find({}))).drop(columns=drop_columns)
            chat_info["label"] = chat_info["label"].apply(lambda x: ",".join(x))
            chat_info = chat_info.replace({False: "x", True: ""})
            chat_info.columns = [self.get_columns_name(col, "cl") for col in chat_info.columns]
            chat_info = chat_info[list(self.CHAT_INFO_COLUMNS_MAP.values())]

            original_labels_order = online_chat_info["Labels"].unique().tolist()

            chat_info = chat_info.sort_values(
                by="Labels", key=lambda x: x.map({label: i for i, label in enumerate(original_labels_order)})
            )

            ws = self.init_online_sheet(self.ONLINE_CHAT_INFO_URL, self.ONLIN_CHAT_INFO_TABLE_NAME, to_type="ws")
            ws.clear()
            ws.set_dataframe(chat_info, (1, 1))
            self.logger.info("Upload mongoDB to online sheet successfully")

    def handle_operator(self, update: Update) -> dict:
        result = {}
        chat = update.effective_chat
        operator = update.effective_user

        if operator is None and str(chat.type) == "channel":
            result["name"] = "channel admin"
            result["id"] = "channel admin"
        else:
            result["name"] = operator.full_name
            result["id"] = operator.id
        return result

    def get_category(self) -> list:
        chat_info = self.init_collection("AnnouncementDB", "ChatInfo")
        chat1 = chat_info.find_one({})
        chat2 = chat_info.find_one({})

        non_category_columns = [
            "_id",
            "id",
            "update_time",
            "operator",
            "operator_id",
            "label",
            "name",
            "type",
            "add_time",
            "description",
        ]

        category1 = set(chat1.keys()) - set(non_category_columns)
        category2 = set(chat2.keys()) - set(non_category_columns)

        category_mismatch = category1.symmetric_difference(category2)
        for category in category_mismatch:
            self.logger.warning(f"Category {category} mismatch")

        return sorted(list(category1))

    def get_category_pattern(self) -> str:
        category = self.get_category()
        category.append("others")
        return "|".join(category)

    def get_labels(self):
        chat_info = self.init_collection("AnnouncementDB", "ChatInfo")
        chat_info = pd.DataFrame(list(chat_info.find({})))

        labels = chat_info["label"].tolist()
        concatenated_labels = [label for sublist in labels for label in sublist if label != ""]
        unique_labels = list(set(concatenated_labels))
        return unique_labels

    def get_names(self):
        chat_info = self.init_collection("AnnouncementDB", "ChatInfo")
        chat_info = pd.DataFrame(list(chat_info.find({})))

        names = chat_info["name"].tolist()
        return names

    def get_chat_by_announcement(self, annc: Announcement) -> list:
        chat_info = self.init_collection("AnnouncementDB", "ChatInfo")

        chat_list = []
        if annc.category != "others":
            filter_ = {annc.category: True, "label": {"$in": [annc.language]}}
            chats = chat_info.find(filter_)
            for chat in chats:
                inputs = {"id": chat["id"], "name": chat["name"]}
                if inputs not in chat_list:
                    chat_list.append(inputs)

        elif annc.labels or annc.chats:
            for i in annc.labels:
                filter_ = {"label": {"$in": [i]}}
                chats = chat_info.find(filter_)
                for chat in chats:
                    inputs = {"id": chat["id"], "name": chat["name"]}
                    if inputs not in chat_list:
                        chat_list.append(inputs)

            for i in annc.chats:
                filter_ = {"name": i}
                chat = chat_info.find_one(filter_)
                inputs = {"id": chat["id"], "name": chat["name"]}
                if inputs not in chat_list:
                    chat_list.append(inputs)

        return chat_list

    def get_confirm_message(self, annc: Announcement) -> str:
        print(annc.__dict__)
        if annc.category != "others":
            message = (
                f"<b>[Confirm Message]</b>\n\n"
                f"<b>ID:</b> {annc.id}\n"
                f"<b>Creator:</b> {annc.creator}\n"
                f"<b>Category:</b> {self.get_columns_name(annc.category, 'cl')}\n"
                f"<b>Language:</b> {self.get_columns_name(annc.language, 'al')}\n"
                f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                f"<b>Contents:</b>\n\n"
                f"{annc.content_html}"
            )
        else:
            message = (
                f"<b>[Confirm Message]</b>\n\n"
                f"<b>ID:</b> {annc.id}\n"
                f"<b>Creator:</b> {annc.creator}\n"
                f"<b>Labels:</b> {', '.join(annc.labels)}\n"
                f"<b>Chats:</b> {', '.join(annc.chats)}\n"
                f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                f"<b>Contents:</b>\n\n"
                f"{annc.content_html}"
            )

        return message

    def get_report_message(self, annc: Announcement):
        if annc.category != "others":
            message = (
                f"<b>[{'Approved' if annc.status == 'approved' else 'Rejected'} Message]</b>\n\n"
                f"<b>ID:</b> {annc.id}\n"
                f"<b>Creator:</b> {annc.creator}\n"
                f"<b>Operator:</b> {annc.approver}\n"
                f"<b>Category:</b> {self.get_columns_name(annc.category, 'cl')}\n"
                f"<b>Language:</b> {self.get_columns_name(annc.language, 'al')}\n"
                f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                f"<b>Contents:</b>\n\n"
                f"{annc.content_html}"
            )
        else:
            message = (
                f"<b>[{'Approved' if annc.status == 'approved' else 'Rejected'} Message]</b>\n\n"
                f"<b>ID:</b> {annc.id}\n"
                f"<b>Creator:</b> {annc.creator}\n"
                f"<b>Operator:</b> {annc.approver}\n"
                f"<b>Labels:</b> {', '.join(annc.labels)}\n"
                f"<b>Chats:</b> {', '.join(annc.chats)}\n"
                f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                f"<b>Contents:</b>\n\n"
                f"{annc.content_html}"
            )
        return message

    async def post_annc(self, annc: Announcement, bot: Bot):
        method_map = {
            "photo": bot.send_photo,
            "video": bot.send_video,
            "text": bot.send_message,
        }

        tasks = []
        for chat in annc.available_chats:
            if annc.content_type == "text":
                inputs = {
                    "chat_id": chat["id"],
                    "text": annc.content_html,
                    "parse_mode": "HTML",
                }
                task = asyncio.create_task(method_map[annc.content_type](**inputs))
            else:
                inputs = {
                    "chat_id": chat["id"],
                    annc.content_type: open(annc.file_path, "rb"),
                    "caption": annc.content_html,
                    "parse_mode": "HTML",
                }
                task = asyncio.create_task(method_map[annc.content_type](**inputs))
            tasks.append(task)
        result = await asyncio.gather(*tasks)
        return result

    async def save_file(self, id: str, bot: Bot) -> dict:
        if id == "":
            return {
                "url": "",
                "path": "",
                "id": "",
            }

        info = await bot.get_file(id)
        url = info.file_path
        name = info.file_path.split("/")[-1]
        path = f"{self.FILE_PATH}/{name}"

        res = rq.get(url)
        with open(path, "wb") as f:
            f.write(res.content)
            f.close()

        result = {
            "url": url,
            "path": path,
            "id": id,
        }
        return result

    def parse_annc_result(self, result: list) -> list:
        parsed_result = []
        for i in result:
            chat_type = i.chat.type

            if chat_type == "private":
                name = i.chat.full_name
            else:
                name = i.chat.title

            parsed_result.append(
                {
                    "id": i.chat.id,
                    "name": name,
                    "message_id": i.message_id,
                }
            )
        return parsed_result
