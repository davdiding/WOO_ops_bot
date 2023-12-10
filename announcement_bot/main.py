from datetime import datetime as dt

from lib.utils import Announcement, Tools
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

CATEGORY, LANGUAGE, LABELS, CONTENT, CONFIRMATION = range(5)


class AnnouncementBot:
    BOT_KEY = "MAIN_BOT_KEY"
    TEST_BOT_KEY = "TEST_MAIN_BOT_KEY"
    CONFIRMATION_GROUP = "APPROVE_GROUP_ID"

    def __init__(self) -> None:
        self.is_test = False
        self.tools = Tools()
        self.logger = self.tools.get_logger("MainBot")
        self.bot = Bot(self.tools.config[self.TEST_BOT_KEY if self.is_test else self.BOT_KEY])

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
        print(query.data)
        context.user_data["announcement"].category = query.data
        if query.data != "others":
            keyboard = [
                [InlineKeyboardButton("English", callback_data="english")],
                [InlineKeyboardButton("Chinese", callback_data="chinese")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = (
                f"You have chosen `{self.tools.get_columns_name(query.data, 'cl')}`\n"
                f"Please choose a language for your post"
            )
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode="MarkdownV2")
            return LANGUAGE
        else:
            message = f"Please enter the labels of directly chat names you want to post to"
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
                await update.message.reply_text(f"Label or name `{i}` not found, please check again")
                return LABELS

        context.user_data["announcement"].labels = labels
        context.user_data["announcement"].names = names

        return CONTENT

    async def choose_language(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query
        print(query.data)
        context.user_data["announcement"].language = query.data

        message = f"Please enter the content of your post, it can be text, photo or video"
        await query.message.edit_text(message)
        return CONTENT

    async def choose_content(self, update: Update, context: ContextTypes) -> int:
        message = update.message
        user = update.message.from_user

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
        }

        context.user_data["announcement"].update(**update_)

        method_map = {
            "photo": context.bot.send_photo,
            "video": context.bot.send_video,
            "text": context.bot.send_message,
        }

        keyboard = [
            [InlineKeyboardButton("Approve", callback_data="approve")],
            [InlineKeyboardButton("Reject", callback_data="reject")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if annc_type in ["photo", "video"]:
            inputs = {
                "chat_id": self.tools.config[self.CONFIRMATION_GROUP],
                annc_type: file["id"],
                "caption": self.tools.get_confirm_message(context.user_data["announcement"]),
                "parse_mode": "HTML",
                "reply_markup": reply_markup,
            }

        else:
            inputs = {
                "chat_id": self.tools.config[self.CONFIRMATION_GROUP],
                "text": self.tools.get_confirm_message(context.user_data["announcement"]),
                "parse_mode": "HTML",
                "reply_markup": reply_markup,
            }

        await method_map[annc_type](**inputs)

        self.logger.info(f"Announcement {context.user_data['announcement'].id} sent to admin group")

        if message.chat.type == "private":
            message = (
                f"Your post has been sent to the admin group for approval, "
                f"please wait patiently.\n"
                f"ID: {context.user_data['announcement'].id}"
            )
            await update.message.reply_text(message)
            return ConversationHandler.END

        return CONFIRMATION

    async def confirmation(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query

        operator = query.from_user
        annc = context.user_data["announcement"]

        if self.tools.is_admin(operator.id):
            pass

        else:
            self.logger.warn(f"Unauthorized user {operator.full_name}({operator.id}) tried to post")

    async def cancel(self, update: Update, context: ContextTypes) -> int:
        operator = update.message.from_user

        message = f"Bye {operator.full_name}! I hope we can talk again some day."

        await update.message.reply_text(message)
        return ConversationHandler.END

    def run(self) -> None:
        self.logger.info("MainBot is running...")
        application = (
            Application.builder().token(self.tools.config[self.TEST_BOT_KEY if self.is_test else self.BOT_KEY]).build()
        )

        post_handler = ConversationHandler(
            entry_points=[CommandHandler("post", self.post)],
            states={
                CATEGORY: [CallbackQueryHandler(self.choose_category, pattern=self.tools.get_category_pattern())],
                LANGUAGE: [CallbackQueryHandler(self.choose_language, pattern="^(english|chinese)$")],
                LABELS: [MessageHandler(filters.Text, self.choose_labels)],
                CONTENT: [
                    MessageHandler(
                        filters.TEXT & (~filters.COMMAND) | filters.PHOTO | filters.VIDEO, self.choose_content
                    )
                ],
                CONFIRMATION: [CallbackQueryHandler(self.confirmation, pattern="^(yes|no)$")],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            per_chat=False,
        )

        application.add_handler(post_handler)
        application.run_polling()


if __name__ == "__main__":
    bot = AnnouncementBot()
    bot.run()
