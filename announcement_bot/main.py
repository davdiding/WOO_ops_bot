import argparse
from datetime import datetime as dt

from lib.utils import Announcement, Tools, init_args
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update, request
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

CATEGORY, LANGUAGE, LABELS, CONTENT = range(4)
ANNC_ID, NEW_CONTENT = range(2)


class AnnouncementBot:
    INFO_BOT_KEY = "INFO_BOT_KEY"
    BOT_KEY = "MAIN_BOT_KEY"
    TEST_BOT_KEY = "TEST_MAIN_BOT_KEY"
    CONFIRMATION_GROUP = "APPROVE_GROUP_ID"
    REQUEST = request.HTTPXRequest(connection_pool_size=50000, connect_timeout=300, read_timeout=300)

    def __init__(self, is_test: bool) -> None:
        self.is_test = is_test
        self.tools = Tools()
        self.logger = self.tools.get_logger("MainBot")
        self.bot = Bot(self.tools.config[self.BOT_KEY], request=self.REQUEST)
        self.info_bot = Bot(self.tools.config[self.INFO_BOT_KEY], request=self.REQUEST)

    async def post(self, update: Update, context: ContextTypes) -> int:
        operator = update.message.from_user

        if not self.tools.in_whitelist(operator.id):
            await update.message.reply_text(f"Hi {operator.full_name}, You are not in the whitelist")
            return ConversationHandler.END
        self.tools.update_chat_info("download")

        # Create category button, two choice per row
        category = self.tools.get_category()
        name_callback = [(self.tools.get_columns_name(i, "cl"), i) for i in category]
        name_callback.append(("Others", "others"))

        keyboard = []
        for i in range(len(name_callback)):
            if i % 2 == 0:
                keyboard.append([InlineKeyboardButton(name_callback[i][0], callback_data=name_callback[i][1])])
            else:
                keyboard[-1].append(InlineKeyboardButton(name_callback[i][0], callback_data=name_callback[i][1]))
        reply_markup = InlineKeyboardMarkup(keyboard)

        message = f"Hello {operator.full_name}! Please choose a category for your post."

        await update.message.reply_text(message, reply_markup=reply_markup)

        inputs = {
            "id": self.tools.get_annc_id(),
            "create_time": dt.now(),
            "creator": operator.full_name,
            "creator_id": operator.id,
        }
        context.user_data["announcement"] = Announcement(**inputs)
        return CATEGORY

    async def choose_category(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query
        context.user_data["announcement"].category = query.data
        if query.data != "others":
            keyboard = [
                [InlineKeyboardButton("English", callback_data="english")],
                [InlineKeyboardButton("Chinese", callback_data="chinese")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = (
                f"You have chosen \n"
                f"**Category** : `{self.tools.get_columns_name(query.data, 'cl')}`\n"
                f"Please choose a language for your post"
            )
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode="MarkdownV2")
            return LANGUAGE
        else:
            message = (
                f"You have chosen \n"
                f"**Category** : `{self.tools.get_columns_name(query.data, 'cl')}`\n"
                f"Please enter the labels or names of the chats you want to post, one per line\n"
            )
            await query.message.edit_text(message, parse_mode="MarkdownV2")
            return LABELS

    async def choose_labels(self, update: Update, context: ContextTypes) -> int:
        text = update.message.text
        labels_or_names = text.split("\n")
        # read all labels and chat name, if any input in label then append to labels, otherwise append to names
        labels = []
        names = []
        existing_labels = self.tools.get_labels()
        existing_names = self.tools.get_names()
        for i in labels_or_names:
            if i in existing_labels:
                labels.append(i)
            elif i in existing_names:
                names.append(i)
            else:
                if i in ["/cancel", "/cancel@WOO_Announcement_Request_Bot"]:
                    await self.cancel(update, context)
                    return ConversationHandler.END
                await update.message.reply_text(f"Label or name `{i}` not found, please check again")
                return LABELS

        context.user_data["announcement"].labels = labels
        context.user_data["announcement"].chats = names

        annc = context.user_data["announcement"]
        message = (
            f"You have chosen \n"
            f"**Category** : `{self.tools.get_columns_name(annc.category, 'cl')}`\n"
            f"**Labels** : `{', '.join(annc.labels)}`\n"
            f"**Chats** : `{', '.join(annc.chats)}`\n"
            f"Please enter the content of your post, in text, photo or video format\n"
        )
        await update.message.reply_text(message, parse_mode="MarkdownV2")
        return CONTENT

    async def choose_language(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query
        context.user_data["announcement"].language = query.data

        annc = context.user_data["announcement"]
        message = (
            f"You have chosen \n"
            f"**Category** : `{self.tools.get_columns_name(annc.category, 'cl')}`\n"
            f"**Language** : `{self.tools.get_columns_name(annc.language, 'al')}`\n"
            f"Please enter the content of your post, in text, photo or video format\n"
        )
        await query.message.edit_text(message, parse_mode="MarkdownV2")
        return CONTENT

    async def choose_content(self, update: Update, context: ContextTypes) -> int:
        message = update.message
        operator = update.message.from_user

        # check content type
        photo = message.photo
        video = message.video
        content_text = message.caption if message.caption else message.text if message.text else ""
        content_html = message.caption_html if message.caption_html else message.text_html if message.text_html else ""

        if len(photo) != 0:  # photo condition
            file_id = photo[3].file_id
            annc_type = "photo"

        elif video is not None:  # video condition
            file_id = video.file_id
            annc_type = "video"

        else:  # Only text condition
            file_id = ""
            annc_type = "text"

        file = await self.tools.save_file(file_id, self.bot)

        update_ = {
            "content_type": annc_type,
            "content_text": content_text,
            "content_html": content_html,
            "file_path": file["path"],
            "available_chats": self.tools.get_chat_by_announcement(context.user_data["announcement"]),
            "status": "pending",
        }

        context.user_data["announcement"].update(**update_)

        method_map = {
            "photo": context.bot.send_photo,
            "video": context.bot.send_video,
            "text": context.bot.send_message,
        }

        keyboard = [
            [InlineKeyboardButton("Approve", callback_data=f"approve_{context.user_data['announcement'].id}")],
            [InlineKeyboardButton("Reject", callback_data=f"reject_{context.user_data['announcement'].id}")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if annc_type in ["photo", "video"]:
            inputs = {
                "chat_id": self.tools.config[self.CONFIRMATION_GROUP] if not self.is_test else "5327851721",
                annc_type: file["id"],
                "caption": self.tools.get_confirm_message(context.user_data["announcement"]),
                "parse_mode": "HTML",
                "reply_markup": reply_markup,
            }

        else:
            inputs = {
                "chat_id": self.tools.config[self.CONFIRMATION_GROUP] if not self.is_test else "5327851721",
                "text": self.tools.get_confirm_message(context.user_data["announcement"]),
                "parse_mode": "HTML",
                "reply_markup": reply_markup,
            }

        await method_map[annc_type](**inputs)
        self.logger.info(
            f"Announcement {context.user_data['announcement'].id} ticket sent by {operator.full_name}({operator.id})"
        )

        self.tools.input_annc_record(context.user_data["announcement"])

        message = (
            f"Your post has been sent to the admin group for approval, "
            f"please wait patiently.\n"
            f"ID: {context.user_data['announcement'].id}"
        )
        await update.message.reply_text(message)

        return ConversationHandler.END

    async def confirmation(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query

        operation = query.data.split("_")[0]
        id = query.data.split("_")[1]

        approver = update.effective_user
        annc = self.tools.get_annc_by_id(id)

        if self.tools.is_admin(approver.id):
            inputs = {
                "approver": approver.full_name,
                "approver_id": approver.id,
                "approved_time": dt.now(),
            }

            if operation == "approve":
                inputs["status"] = "approved"
                result = await self.tools.post_annc(annc, self.info_bot)
                parsed_result = self.tools.parse_annc_result(result)
                inputs["record"] = parsed_result
            else:
                inputs["status"] = "rejected"

            annc.update(**inputs)

            repost_message = self.tools.get_report_message(annc)

            await query.message.edit_text(
                repost_message, parse_mode="HTML"
            ) if annc.content_type == "text" else await query.message.edit_caption(repost_message, parse_mode="HTML")

            self.logger.info(f"Announcement {annc.id} was {annc.status} by {approver.full_name}({approver.id})")

            announcement_db = self.tools.init_collection("AnnouncementDB", "Announcement")
            filter_ = {"id": annc.id}
            update_ = {"$set": annc.__dict__}
            announcement_db.update_one(filter_, update_)

            return ConversationHandler.END
        else:
            self.logger.warn(f"Unauthorized user {approver.full_name}({approver.id}) tried to post")

    async def edit(self, update: Update, context: ContextTypes) -> int:
        operator = update.message.from_user

        if not self.tools.in_whitelist(operator.id):
            await update.message.reply_text(f"Hi {operator.full_name}, You are not in the whitelist")
            return ConversationHandler.END

        message = (
            f"Hello {operator.full_name}! Please enter the ID of the announcement you want to edit. \n"
            f"Can check the ID in [**Announcement History**](https://docs.google.com/spreadsheets/d/1ZWGIQNCvb_6XLiVIguXaWOjLjP90Os2d1ltOwMT4kqs/edit#gid=1035359090)"
        )

        await update.message.reply_text(message)

        return ANNC_ID

    async def choose_annc_id(self, update: Update, context: ContextTypes) -> int:
        pass

    async def choose_edit_content(self, update: Update, context: ContextTypes) -> int:
        pass

    async def edit_confirmation(self, update: Update, context: ContextTypes) -> int:
        pass

    async def cancel(self, update: Update, context: ContextTypes) -> int:
        operator = update.message.from_user

        message = f"Bye {operator.full_name}! I hope we can talk again some day."

        await update.message.reply_text(message)
        return ConversationHandler.END

    async def help(self, update: Update, context: ContextTypes) -> None:
        operator = update.message.from_user

        if self.tools.in_whitelist(operator.id):
            help_message = self.tools.get_help_message()

            message = f"Hi {operator.full_name},\n{help_message}"

            await update.message.reply_text(message, parse_mode="MarkdownV2")

    def run(self) -> None:
        self.logger.info("MainBot is running...")
        application = Application.builder().token(self.tools.config[self.BOT_KEY]).build()

        application.add_handler(CommandHandler("help", self.help))
        post_handler = ConversationHandler(
            entry_points=[CommandHandler("post", self.post)],
            states={
                CATEGORY: [CallbackQueryHandler(self.choose_category, pattern=self.tools.get_category_pattern())],
                LANGUAGE: [CallbackQueryHandler(self.choose_language, pattern="^(english|chinese)$")],
                LABELS: [MessageHandler(filters.TEXT, self.choose_labels)],
                CONTENT: [
                    MessageHandler(
                        filters.TEXT & (~filters.COMMAND) | filters.PHOTO | filters.VIDEO, self.choose_content
                    )
                ],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            per_chat=False,
        )

        edit_handler = ConversationHandler(
            entry_points=[CommandHandler("edit", self.edit)],
            states={
                ANNC_ID: [MessageHandler(filters.TEXT, self.choose_annc_id)],
                NEW_CONTENT: [MessageHandler(filters.TEXT & (~filters.COMMAND), self.choose_edit_content)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            per_chat=False,
        )

        application.add_handler(post_handler)
        application.add_handler(CallbackQueryHandler(self.confirmation, pattern=r"^(approve|reject)_.*"))
        application.add_handler(CallbackQueryHandler(self.edit_confirmation, pattern=r"^(edit_approce|edit_reject)_.*"))
        application.add_handler(edit_handler)
        application.run_polling()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("InfoBot")
    args = init_args(parser)

    bot = AnnouncementBot(is_test=args.test)
    bot.run()
