from datetime import datetime as dt

from lib.utils import ChatGroup, Permission, Tools
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
        old_status = update.my_chat_member.old_chat_member.status
        new_status = update.my_chat_member.new_chat_member.status
        if old_status == "left" and new_status == "member":
            return "add"
        elif old_status == "member" and new_status == "left":
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

    def __init__(self, test: bool = True):
        super().__init__()
        self.is_test = test
        self.tools = Tools()
        self.logger = self.tools.get_logger("InfoBot")

    async def chat_status_update(self, update: Update, context: ContextTypes) -> None:
        operator = update.effective_user

        # will do nothing if the operator is not admin
        if not self.tools.is_admin(str(operator.id)):
            self.logger.warning(
                f"{operator.full_name}({operator.id}) has no permission to add/remove announcement bot."
            )
            return

        operator_name = str(operator.full_name)
        opreator_id = str(operator.id)

        status = self.get_chat_status(update)
        if status is None:
            self.logger.warning(f"Unknown status change operated by {operator_name}({opreator_id})\n{update}")

        chat = update.effective_chat
        id = str(chat.id)
        name = str(chat.title)
        type = str(chat.type)
        description = ""
        add_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        label = []
        update_time = add_time
        group = ChatGroup(
            id=id,
            type=type,
            name=name,
            label=label,
            description=description,
            add_time=add_time,
            update_time=update_time,
        )
        group.operator = operator_name
        group.operator_id = opreator_id

        chat_info = self.tools.init_collection("AnnouncementDB", "ChatInfo")

        filter_ = {"id": id}
        if status == "add":  # insert new chat
            if chat_info.count_documents(filter_) == 0:
                chat_info.insert_one(group.__dict__)
                self.logger.warning(
                    f"Add {group.name}({group.id}) to AnnouncementDB.ChatInfo by {operator_name}({opreator_id})"
                )
            else:
                self.logger.warning(f"{group.name}({group.id}) already in AnnouncementDB.ChatInfo")
        else:  # delete chat
            if chat_info.count_documents(filter_) == 1:
                chat_info.delete_one(filter_)
                self.logger.warning(
                    f"Delete {group.name}({group.id}) from AnnouncementDB.ChatInfo by {operator_name}({opreator_id})"
                )
            else:
                self.logger.warning(f"{group.name}({group.id}) not in AnnouncementDB.ChatInfo")
        self.tools.update_chat_info(direction="up")

    async def chat_title_update(self, update: Update, context: ContextTypes) -> None:
        operator = update.effective_user
        chat = update.effective_chat

        # if the chat not in our DB, do nothing
        chat_info = self.tools.init_collection("AnnouncementDB", "ChatInfo")
        filter_ = {"id": str(chat.id)}
        if chat_info.count_documents(filter_) == 0:
            self.logger.warning(
                f"Can't update {chat.title}({chat.id}) because it's not in AnnouncementDB.ChatInfo. "
                f"Chat name changed by {operator.full_name}({operator.id})"
            )
            return
        else:
            for i in chat_info.find(filter_):
                old_chat = i
                del old_chat["_id"]

        old_chat = ChatGroup(**old_chat)
        old_chat_name = old_chat.name
        old_chat.name = str(chat.title)
        old_chat.update_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        old_chat.type = str(chat.type)

        chat_info.update_one(filter_, {"$set": old_chat.__dict__})

        self.logger.warning(
            f"Update {old_chat_name}({old_chat.id}) by {operator.full_name}({operator.id}) to new name: {old_chat.name}"
        )
        self.tools.update_chat_info(direction="up")

    # this functions will fill lib/permission to mongodb AnnouncementDB.Permissions
    async def fill_permission(self, update: Update, context: ContextTypes) -> None:
        if not self.tools.is_admin(str(update.effective_user.id)):
            self.logger.warning(
                f"{update.effective_user.full_name}({update.effective_user.id}) has no permission to fill permission."
            )
            return

        old_permission = self.tools.init_permission()
        new_permission = self.tools.init_collection("AnnouncementDB", "Permissions")
        for _, row in old_permission.iterrows():
            id = str(row["id"])
            name = str(row["name"])
            is_admin = bool(row["admin"])
            in_whitelist = bool(row["whitelist"])

            permission = Permission(id=id, name=name, admin=is_admin, whitelist=in_whitelist)

            filter_ = {"id": id}
            if new_permission.count_documents(filter_) == 0:
                new_permission.insert_one(permission.__dict__)
                self.logger.warning(f"Add {permission.name}({permission.id}) to AnnouncementDB.Permissions")
            elif new_permission.count_documents(filter_) == 1:
                self.logger.warning(f"{permission.name}({permission.id}) already in AnnouncementDB.Permissions")

    # This function will move chat_info.csv to mongodb
    def update_chat_info(self, update: Update, context: ContextTypes) -> None:
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
                    continue
                inputs[_category] = True
            chat = ChatGroup(**inputs)
            new_chat_info.insert_one(chat.__dict__)
        self.logger.info(f"Add {new_chat_info.count_documents({})} chats to AnnouncementDB.ChatInfo")

    def run(self):
        self.logger.warning("InfoBot is running...")
        application = (
            Application.builder().token(self.tools.config[self.TEST_BOT_KEY if self.is_test else self.BOT_KEY]).build()
        )
        application.add_handler(ChatMemberHandler(self.chat_status_update, ChatMemberHandler.MY_CHAT_MEMBER))
        application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_TITLE, self.chat_title_update))

        # add fill_chat_info command
        application.add_handler(CommandHandler("fill_chat_info", self.fill_chat_info))

        # add fill_permission command
        application.add_handler(CommandHandler("fill_permission", self.fill_permission))

        application.run_polling()


if __name__ == "__main__":
    info_bot = InfoBot()
    info_bot.run()
