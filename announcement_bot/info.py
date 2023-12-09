import argparse
from datetime import datetime as dt

from lib.utils import ChatGroup, Tools, init_args
from telegram import Update
from telegram.ext import (
    Application,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


class ChatManager:
    def get_chat_status(self, update) -> list:
        old_status = str(update.my_chat_member.old_chat_member.status)
        new_status = str(update.my_chat_member.new_chat_member.status)
        if old_status == "left" and new_status == "member":
            return "add"
        elif old_status == "member" and new_status == "left":
            return "left"
        elif old_status == "left" and new_status == "administrator":
            return "add"
        elif old_status == "administrator" and new_status == "left":
            return "left"
        else:
            return None


class InfoBot(ChatManager):
    BOT_KEY = "INFO_BOT_KEY"
    TEST_BOT_KEY = "TEST_INFO_BOT_KEY"

    OLD_TO_NEW_CHAT_INFO_COLUMNS_MAP = {
        "chat_id": "id",
        "chat_type": "type",
        "chat_name": "name",
        "chat_cat": "label",
        "chat_added_time": "add_time",
        "note": "description",
    }

    def __init__(self, test: bool = False):
        super().__init__()
        self.is_test = test
        self.tools = Tools()
        self.logger = self.tools.get_logger("InfoBot")

    async def chat_status_update(self, update: Update, context: ContextTypes) -> None:
        operator = self.tools.handle_operator(update)

        self.update_chat_info("download")
        chat_info = self.tools.init_collection("AnnouncementDB", "ChatInfo")

        status = self.get_chat_status(update)
        if status is None:
            self.logger.warning(f"Unknown status change operated by {operator['name']}({operator['id']})\n{update}")
            return

        chat = update.effective_chat

        inputs = {
            "id": chat.id,
            "type": str(chat.type),
            "name": str(chat.title),
            "label": [],
            "description": "",
            "add_time": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            "update_time": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operator": operator["name"],
            "operator_id": operator["id"],
        }

        # add old category to new group
        existing_chat = chat_info.find_one({})
        for i in existing_chat:
            if i in inputs or i == "_id":
                continue
            inputs[i] = True
        new_chat = ChatGroup(**inputs)

        filter_ = {"id": new_chat.id}
        if status == "add":  # insert new chat
            # will do nothing if the operator is not admin
            if not self.tools.is_admin(str(operator["id"])):
                self.logger.warning(f"{operator['name']}({operator['id']}) has no permission to add announcement bot.")
                return

            if chat_info.count_documents(filter_) == 0:
                chat_info.insert_one(new_chat.__dict__)
                self.logger.info(
                    f"Add {new_chat.name}({new_chat.id}) to AnnouncementDB.ChatInfo by "
                    f"{operator['name']}({operator['id']})"
                )
            else:
                self.logger.warning(f"{new_chat.name}({new_chat.id}) already in AnnouncementDB.ChatInfo")
        else:  # delete chat
            if chat_info.count_documents(filter_) == 1:
                chat_info.delete_one(filter_)
                self.logger.info(
                    f"Delete {new_chat.name}({new_chat.id}) from AnnouncementDB.ChatInfo by "
                    f"{operator['name']}({operator['id']})"
                )
            else:
                self.logger.warning(f"{new_chat.name}({new_chat.id}) not in AnnouncementDB.ChatInfo")
        self.tools.update_chat_info(update_type="upload")

    async def chat_title_update(self, update: Update, context: ContextTypes) -> None:
        chat = update.effective_chat
        operator = self.tools.handle_operator(update)

        # if the chat not in our DB, do nothing
        self.update_chat_info("download")
        chat_info = self.tools.init_collection("AnnouncementDB", "ChatInfo")
        filter_ = {"id": str(chat.id)}
        if chat_info.count_documents(filter_) == 0:
            self.logger.warning(
                f"{chat.title}({chat.id}) not in DB, chat name changed by {operator['name']}({operator['id']})"
            )
            return
        else:
            old_chat = chat_info.find_one(filter_)
            del old_chat["_id"]

        new_chat = ChatGroup(**old_chat)
        old_chat_name = new_chat.name
        new_chat.name = str(chat.title)
        new_chat.update_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        new_chat.type = str(chat.type)

        chat_info.update_one(filter_, {"$set": new_chat.__dict__})

        self.tools.update_chat_info(update_type="upload")
        self.logger.info(
            f"Update {old_chat_name}({new_chat.id}) to new name: {new_chat.name} "
            f"by {operator['name']}({operator['id']})"
        )

    # This function will move chat_info.csv to mongodb, will not used in the future
    async def copy_chat_info(self, update: Update, context: ContextTypes) -> None:
        operator = update.effective_user
        if not self.tools.is_admin(str(operator.id)):
            self.logger.warning(f"{operator.full_name}({operator.id}) has no permission to update chat info.")
            return
        online_db = self.tools.init_online_sheet(self.tools.ONLINE_CHAT_INFO_URL, self.tools.ONLIN_CHAT_INFO_TABLE_NAME)
        online_db_columns = online_db.columns.tolist()
        new_chat_info = self.tools.init_collection("AnnouncementDB", "ChatInfo")
        old_chat_info = self.tools.init_chatinfo()

        # need to extract all the columns between "Categopry" and "Note", not include them
        start = online_db_columns.index("Labels")
        end = online_db_columns.index("Note")
        category_list = online_db_columns[start + 1 : end]

        # empty mongo db
        new_chat_info.delete_many({})

        # add old chat info to mongo db
        for _, row in old_chat_info.iterrows():
            inputs = {self.OLD_TO_NEW_CHAT_INFO_COLUMNS_MAP[key]: value for key, value in row.items()}
            for category in category_list:
                _category = self.tools.get_columns_name(category, input="cr")
                if _category is None:
                    self.logger.warning(f"Unknown category: {category}")
                    continue
                inputs[_category] = True
            chat = ChatGroup(**inputs)
            new_chat_info.insert_one(chat.__dict__)
        self.logger.info(f"Add {new_chat_info.count_documents({})} chats to AnnouncementDB.ChatInfo")

    async def update_chat_info(self, update: Update, context: ContextTypes) -> None:
        operator = update.effective_user
        if not self.tools.is_admin(str(operator.id)):
            self.logger.warning(f"{operator.full_name}({operator.id}) has no permission to update chat info.")
            return
        self.tools.update_chat_info(update_type="download")
        self.tools.update_chat_info(update_type="upload")
        self.logger.info(f"Update chat info by {operator.full_name}({operator.id})")

        await update.message.reply_text(f"Chat info updated at {dt.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def run(self):
        self.logger.info("InfoBot is running...")
        application = (
            Application.builder().token(self.tools.config[self.TEST_BOT_KEY if self.is_test else self.BOT_KEY]).build()
        )
        application.add_handler(ChatMemberHandler(self.chat_status_update, ChatMemberHandler.MY_CHAT_MEMBER))
        application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_TITLE, self.chat_title_update))
        application.add_handler(CommandHandler("sync", self.update_chat_info))

        application.run_polling()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("InfoBot")
    args = init_args(parser)

    info_bot = InfoBot(test=args.test)
    info_bot.run()
