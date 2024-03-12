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
from beautifultable import BeautifulTable
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
        operation: str,
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
        self.operation = operation
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
        self.approver_id = str(approver_id) if approver_id else None
        self.record = record
        self.status = status

    def update(self, **kwargs):

        for k, v in kwargs.items():
            if k in self.__dict__ and k not in self.FIXED_COLUMNS:
                setattr(self, k, v)
            else:
                print(f"Unknow param of Announcement: {k}")


class EditTicket:
    FIXED_VALUES = ["id", "create_time", "creator", "creator_id"]

    def __init__(
        self,
        id: str,
        operation: str,
        create_time: datetime,
        creator: str,
        creator_id: str,
        original_id: str = None,
        content_type: str = None,
        original_content_text: str = None,
        original_content_html: str = None,
        new_content_text: str = None,
        new_content_html: str = None,
        available_chats: list = None,
        approved_time: datetime = None,
        approver: str = None,
        approver_id: str = None,
        status: str = None,
    ) -> None:
        self.id = id
        self.operation = operation
        self.create_time = create_time
        self.creator = creator
        self.creator_id = str(creator_id)
        self.original_id = original_id
        self.content_type = content_type
        self.original_content_text = original_content_text
        self.original_content_html = original_content_html
        self.new_content_text = new_content_text
        self.new_content_html = new_content_html
        self.available_chats = available_chats
        self.approved_time = approved_time
        self.approver = approver
        self.approver_id = str(approver_id) if approver_id else None
        self.status = status

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.__dict__ and k not in self.FIXED_VALUES:
                setattr(self, k, v)
            else:
                print(f"Unknow param of EditTicket: {k}")


class DeleteTicket:
    FIXED_VALUES = ["id", "create_time", "creator", "creator_id"]

    def __init__(
        self,
        id: str,
        operation: str,
        create_time: datetime,
        creator: str,
        creator_id: str,
        original_id: str = None,
        content_type: str = None,
        original_content_text: str = None,
        original_content_html: str = None,
        available_chats: list = None,
        approved_time: datetime = None,
        approver: str = None,
        approver_id: str = None,
        status: str = None,
    ) -> None:
        self.id = id
        self.operation = operation
        self.create_time = create_time
        self.creator = creator
        self.creator_id = str(creator_id)
        self.original_id = original_id
        self.content_type = content_type
        self.original_content_text = original_content_text
        self.original_content_html = original_content_html
        self.available_chats = available_chats
        self.approved_time = approved_time
        self.approver = approver
        self.approver_id = str(approver_id) if approver_id else None
        self.status = status

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.__dict__ and k not in self.FIXED_VALUES:
                setattr(self, k, v)
            else:
                print(f"Unknow param of DeleteTicket: {k}")


class Tools:
    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    MONGO_URL = "MONGO_DB_URL"

    OLD_CHAT_INFO_PATH = CURRENT_PATH + "/../db/chat/chat_info.csv"
    GC_KEY_PATH = CURRENT_PATH + "/../lib/gc_key.json"
    ONLINE_CHAT_INFO_URL = (
        "https://docs.google.com/spreadsheets/d/15yR0QEKG6axFxnxvOGYTwztE33yUsVo5xktloTthedE/edit?usp=sharing"
    )
    ONLIN_CHAT_INFO_TABLE_NAME = "Chat Infomation (formal)"

    ONLINE_ANNC_RECORDS_URL = (
        "https://docs.google.com/spreadsheets/d/1ZWGIQNCvb_6XLiVIguXaWOjLjP90Os2d1ltOwMT4kqs/edit?usp=sharing"
    )
    ONLINE_ANNC_RECORDS_TABLE_NAME = "Announcement History (formal)"
    ONLINE_EDIT_TICKET_RECORDS_TABLE_NAME = "Edit History (formal)"

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
        "id": "ID",
        "create_time": "Create Time",
        "creator": "Creator",
        "approver": "Approver",
        "category": "Category",
        "language": "Language",
        "labels": "Labels",
        "content_text": "Content",
        "content_type": "Type",
        "approved_time": "Approved Time",
        "status": "Status",
        "expected_number": "Expected Number",
        "actual_number": "Actual Number",
        "expected_chats": "Expected Chats",
        "actual_chats": "Actual Chats",
    }
    EDIT_TICKET_INFO_COLUMNS_MAP = {
        "id": "ID",
        "original_id": "Original ID",
        "create_time": "Create Time",
        "approved_time": "Approved Time",
        "creator": "Creator",
        "approver": "Approver",
        "original_content_text": "Original Content",
        "new_content_text": "New Content",
        "status": "Status",
    }

    CURRENT_PATH = os.path.abspath(os.path.dirname(__file__))
    CONFIG_PATH = os.path.join(CURRENT_PATH, "config.json")
    PERMISSION_PATH = os.path.join(CURRENT_PATH, "permission.csv")
    FILE_PATH = os.path.join(CURRENT_PATH, "../db/files")

    INFO_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/info/chat_info.log")
    MAIN_BOT_LOG_PATH = os.path.join(CURRENT_PATH, "../db/log/main/main.log")

    ESCAPE_CHARACTERS = [".", "!", "|"]

    def __init__(self):
        self.config = self.init_config()
        self.mongo_client = self.init_mongo_client()
        self.gc_client = self.init_gc_client()
        self.permission = self.init_collection("AnnouncementDB", "Permissions")
        self.logger = None

        self.update_columns_map()

    # deal escape characters in full name
    def parse_full_name(self, name: str) -> str:
        for i in self.ESCAPE_CHARACTERS:
            name = name.replace(i, f"\{i}")
        return name

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

    def get_annc_id(self, if_test: bool) -> str:
        timestamp = str(int(datetime.now().timestamp() * 1000))
        return timestamp if not if_test else f"test-{timestamp}"

    def get_edit_id(self, if_test: bool) -> str:
        timestamp = str(int(datetime.now().timestamp() * 1000))
        signature = hashlib.md5(timestamp.encode()).hexdigest()
        return signature if not if_test else f"test-{signature}"

    def get_columns_name(self, col: str, input: str) -> str:
        """
        param col: input column name
        param input: 'cl'/'cr'/'al'/'ar',
            'c' means chat,
            'a' means announcement,
            'e' means edit ticket,
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
        elif input == "el":
            return self.EDIT_TICKET_INFO_COLUMNS_MAP[col]

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

            online_columns = list(self.CHAT_INFO_COLUMNS_MAP.values())
            online_columns.remove("Others")
            chat_info = chat_info[online_columns]

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

        category = [i for i in sorted(list(category1)) if i != ""]
        return category

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
        if annc.category != "others":
            message = (
                f"<b>[Confirm Message]</b>\n\n"
                f"<b>ID:</b> <code>{annc.id}</code>\n"
                f"<b>Creator:</b> {annc.creator}\n"
                f"<b>Category:</b> <code>{self.get_columns_name(annc.category, 'cl')}</code>\n"
                f"<b>Language:</b> <code>{self.get_columns_name(annc.language, 'al')}</code>\n"
                f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                f"<b>Contents:</b>\n\n"
                f"{annc.content_html}"
            )
        else:
            message = (
                f"<b>[Confirm Message]</b>\n\n"
                f"<b>ID:</b> <code>{annc.id}</code>\n"
                f"<b>Creator:</b> {annc.creator}\n"
                f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                f"<b>Contents:</b>\n\n"
                f"{annc.content_html}"
            )

        return message

    def get_post_confirm_message(self, annc: Announcement) -> str:
        if annc.category != "others":
            message = (
                f"<b>[Confirm Message]</b>\n\n"
                f"<b>ID:</b> <code>{annc.id}</code>\n"
                f"<b>Creator:</b> {annc.creator}\n"
                f"<b>Category:</b> <code>{self.get_columns_name(annc.category, 'cl')}</code>\n"
                f"<b>Language:</b> <code>{self.get_columns_name(annc.language, 'al')}</code>\n"
                f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                f"<a> Please check the announcement content in the next message.</a>"
            )
        else:
            message = (
                f"<b>[Confirm Message]</b>\n\n"
                f"<b>ID:</b> <code>{annc.id}</code>\n"
                f"<b>Creator:</b> {annc.creator}\n"
                f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                f"<a> Please check the announcement content in the next message.</a>"
            )

        return message

    def get_edit_confirm_message(self, ticket: EditTicket) -> str:
        message = (
            f"<b>[Confirm Message]</b>\n\n"
            f"<b>ID:</b> <code>{ticket.id}</code>\n"
            f"<b>Annc ID:</b> <code>{ticket.original_id}</code>\n"
            f"<b>Creator:</b> {ticket.creator}\n"
            f"<b>Chat numbers:</b> {len(ticket.available_chats)}\n"
            f"<b>Original Contents:</b>\n\n"
            f"{ticket.original_content_html}\n\n"
            f"<b>New Contents:</b>\n\n"
            f"{ticket.new_content_html}"
        )

        return message

    def get_report_message(self, annc: any):
        if isinstance(annc, Announcement):
            if annc.category != "others":
                message = (
                    f"<b>[{'Approved' if annc.status == 'approved' else 'Rejected'} Message]</b>\n\n"
                    f"<b>Operation:</b> <code>{annc.operation}</code>\n"
                    f"<b>ID:</b> <code>{annc.id}</code>\n"
                    f"<b>Creator:</b> {annc.creator}\n"
                    f"<b>Operator:</b> {annc.approver}\n"
                    f"<b>Category:</b> <code>{self.get_columns_name(annc.category, 'cl')}</code>\n"
                    f"<b>Language:</b> <code>{self.get_columns_name(annc.language, 'al')}</code>\n"
                    f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                    f"<b>Contents:</b>\n\n"
                    f"{annc.content_html}"
                )
            else:
                message = (
                    f"<b>[{'Approved' if annc.status == 'approved' else 'Rejected'} Message]</b>\n\n"
                    f"<b>Operation:</b> <code>{annc.operation}</code>\n"
                    f"<b>ID:</b> {annc.id}\n"
                    f"<b>Creator:</b> {annc.creator}\n"
                    f"<b>Operator:</b> {annc.approver}\n"
                    f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                    f"<b>Contents:</b>\n\n"
                    f"{annc.content_html}"
                )
        elif isinstance(annc, EditTicket):
            message = (
                f"<b>[{'Approved' if annc.status == 'approved' else 'Rejected'} Message]</b>\n\n"
                f"<b>Operation:</b> <code>{annc.operation}</code>\n"
                f"<b>ID:</b> <code>{annc.id}</code>\n"
                f"<b>Annc ID:</b> <code>{annc.original_id}</code>\n"
                f"<b>Creator:</b> {annc.creator}\n"
                f"<b>Operator:</b> {annc.approver}\n"
                f"<b>Chat numbers:</b> {len(annc.available_chats)}\n"
                f"<b>Original Contents:</b>\n\n"
                f"{annc.original_content_html}\n\n"
                f"<b>New Contents:</b>\n\n"
                f"{annc.new_content_html}"
            )
        else:
            self.logger.warning(f"Unknow type of annc: {type(annc)}")
        return message

    async def post(self, method: callable, inputs: dict):
        try:
            return await method(**inputs)
        except Exception as e:
            self.logger.warning(f"Post message failed sending to {inputs['chat_id']}: {e}")
            return {"status": "failed", "chat_id": inputs["chat_id"], "error_message": f"{e}"}

    async def post_annc(self, annc: Announcement, bot: Bot):
        method_map = {
            "photo": bot.send_photo,
            "video": bot.send_video,
            "text": bot.send_message,
        }
        num_per_batch = 20
        num_of_batch = len(annc.available_chats) // num_per_batch + 1
        results = []
        for i in range(num_of_batch):

            tasks = []
            for chat in annc.available_chats[i * num_per_batch : (i + 1) * num_per_batch]:
                if annc.content_type == "text":
                    inputs = {
                        "chat_id": chat["id"],
                        "text": annc.content_html,
                        "parse_mode": "HTML",
                    }
                    task = asyncio.create_task(self.post(method_map[annc.content_type], inputs))
                else:
                    inputs = {
                        "chat_id": chat["id"],
                        annc.content_type: open(annc.file_path, "rb"),
                        "caption": annc.content_html,
                        "parse_mode": "HTML",
                    }
                    task = asyncio.create_task(self.post(method_map[annc.content_type], inputs))
                tasks.append(task)
            result = await asyncio.gather(*tasks)
            results.extend(result)
        return results

    async def edit_annc(self, ticket: EditTicket, bot: Bot):
        method_map = {
            "photo": bot.edit_message_caption,
            "video": bot.edit_message_caption,
            "text": bot.edit_message_text,
        }

        tasks = []
        for chat in ticket.available_chats:
            if ticket.content_type == "text":
                inputs = {
                    "chat_id": chat["id"],
                    "message_id": chat["message_id"],
                    "text": ticket.new_content_html,
                    "parse_mode": "HTML",
                }
            else:
                inputs = {
                    "chat_id": chat["id"],
                    "message_id": chat["message_id"],
                    "caption": ticket.new_content_html,
                    "parse_mode": "HTML",
                }
            task = asyncio.create_task(method_map[ticket.content_type](**inputs))
            tasks.append(task)

        await asyncio.gather(*tasks)

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
            if type(i) is dict:
                parsed_result.append(
                    {
                        "id": i["chat_id"],
                        "name": i["chat_id"],
                        "message_id": "Failed",
                        "error_message": i["error_message"],
                    }
                )
                continue

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

    def input_annc_record(self, annc: Announcement) -> None:
        annc_records = self.init_collection("AnnouncementDB", "Announcement")
        annc_records.insert_one(annc.__dict__)

    def input_edit_record(self, ticket: EditTicket) -> None:
        annc_records = self.init_collection("AnnouncementDB", "Announcement")
        annc_records.insert_one(ticket.__dict__)

    def get_annc_by_id(self, id: any) -> Announcement:
        annc_records = self.init_collection("AnnouncementDB", "Announcement")
        filter_ = {"id": str(id), "operation": "post"}
        annc = annc_records.find_one(filter_)

        if annc:
            del annc["_id"]
            return Announcement(**annc)
        else:
            return None

    def get_edit_ticket_by_id(self, id: str) -> EditTicket:
        annc_records = self.init_collection("AnnouncementDB", "Announcement")
        filter_ = {"id": id, "operation": "edit"}
        ticket = annc_records.find_one(filter_)

        if ticket:
            del ticket["_id"]
            return EditTicket(**ticket)
        else:
            return None

    def update_annc_record(self) -> None:
        annc_records = self.init_collection("AnnouncementDB", "Announcement")
        filter_ = {"operation": "post", "id": {"$not": {"$regex": "^test"}}}
        annc_records = pd.DataFrame(list(annc_records.find(filter_)))

        annc_records["expected_number"] = annc_records["available_chats"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
        annc_records["actual_number"] = annc_records["record"].apply(lambda x: len(x) if isinstance(x, list) else 0)
        annc_records["expected_chats"] = annc_records["available_chats"].apply(
            lambda x: ", ".join([i["name"] for i in x] if isinstance(x, list) else [])
        )
        annc_records["actual_chats"] = annc_records["record"].apply(
            lambda x: ", ".join([i["name"] for i in x] if isinstance(x, list) else [])
        )
        annc_records["labels"] = annc_records["labels"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
        annc_records["language"] = annc_records["language"].apply(
            lambda x: self.get_columns_name(x, "al") if isinstance(x, str) else ""
        )
        annc_records["category"] = annc_records["category"].apply(
            lambda x: self.get_columns_name(x, "cl") if isinstance(x, str) else ""
        )

        drop_columns = [
            "_id",
            "record",
            "chats",
            "available_chats",
            "creator_id",
            "approver_id",
            "content_html",
            "file_path",
        ]
        annc_records = annc_records.drop(columns=drop_columns)[
            [
                "id",
                "status",
                "create_time",
                "approved_time",
                "creator",
                "approver",
                "category",
                "language",
                "labels",
                "content_text",
                "content_type",
                "expected_number",
                "actual_number",
                "expected_chats",
                "actual_chats",
            ]
        ]
        annc_records.columns = [self.get_columns_name(col, "al") for col in annc_records.columns]

        online_sheet = self.init_online_sheet(
            self.ONLINE_ANNC_RECORDS_URL, self.ONLINE_ANNC_RECORDS_TABLE_NAME, to_type="ws"
        )
        online_sheet.clear()
        online_sheet.set_dataframe(annc_records, (1, 1))
        return

    def update_edit_record(self) -> None:
        annc_records = self.init_collection("AnnouncementDB", "Announcement")
        filter_ = {"operation": "edit", "id": {"$not": {"$regex": "^test"}}}
        tickets = pd.DataFrame(list(annc_records.find(filter_)))

        drop_columns = [
            "_id",
            "available_chats",
            "creator_id",
            "approver_id",
            "original_content_html",
            "new_content_html",
        ]
        tickets = tickets.drop(columns=drop_columns)[
            [
                "id",
                "status",
                "create_time",
                "approved_time",
                "creator",
                "approver",
                "original_id",
                "original_content_text",
                "new_content_text",
            ]
        ]
        tickets.columns = [self.get_columns_name(col, "el") for col in tickets.columns]

        online_sheet = self.init_online_sheet(
            self.ONLINE_ANNC_RECORDS_URL, self.ONLINE_EDIT_TICKET_RECORDS_TABLE_NAME, to_type="ws"
        )
        online_sheet.clear()
        online_sheet.set_dataframe(tickets, (1, 1))

    def get_help_message(self) -> str:
        return """
🤖 **Welcome to the Announcement Bot**\! 🎉

[**Chat Information Link**](https://docs.google.com/spreadsheets/d/15yR0QEKG6axFxnxvOGYTwztE33yUsVo5xktloTthedE/edit#gid=761337419)
[**Announcement History Link**](https://docs.google.com/spreadsheets/d/1ZWGIQNCvb_6XLiVIguXaWOjLjP90Os2d1ltOwMT4kqs/edit#gid=1035359090)

👉 Follow these steps to post your announcement:

1\. **Start**: Type `/post` to begin\.
2\. **Choose Category**: Tap on your announcement category from the list\.
    \- If not listed, select `Others`\.
3\. **Language & Labels**:
    \- For standard categories, choose a language \(English or Chinese\)\.
    \- For `Others`, type the labels or chat names, one per line\.
4\. **Add Content**: Send your announcement text, photo, or video\.
5\. **Review & Send**: Admin will review your announcement and then post it upon approval\.
6\. **Cancel**: Type `/cancel` anytime to stop\.

🔒 You need to be whitelisted to use this bot\. If you're not, you'll be notified\.

💡 Any issues or questions? Reach out to our support team for help\!
        """

    def get_bt_from_df(self, df: pd.DataFrame) -> str:
        table = BeautifulTable()
        table.set_style(BeautifulTable.STYLE_BOX)
        table.columns.header = df.columns.tolist()
        for _, row in df.iterrows():
            table.rows.append(row.tolist())
        return f"<pre>{table}</pre>"

    def get_permission_table(self) -> str:
        permission = self.init_collection("AnnouncementDB", "Permissions")
        permission = pd.DataFrame(list(permission.find({}))).drop(columns=["_id", "id", "update_time"], axis=1)

        return self.get_bt_from_df(permission)
