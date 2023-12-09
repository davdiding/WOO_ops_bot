from lib.utils import Tools
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

POST, CATEGORY, LANGUAGE, LABELS, CONTENT, CONFIRMATION = range(6)


class AnnouncementBot:
    BOT_KEY = "MAIN_BOT_KEY"
    TEST_BOT_KEY = "TEST_MAIN_BOT_KEY"

    def __init__(self) -> None:
        self.is_test = False
        self.tools = Tools()
        self.logger = self.tools.get_logger("MainBot")

    async def post(self, update: Update, context: ContextTypes) -> int:
        operator = update.message.from_user

        if not self.tools.in_whitelist(str(operator.id)):
            await update.message.reply_text(f"Hi {operator.full_name} \nYou are not in the whitelist")
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

        return POST

    async def choose_category(self, update: Update, context: ContextTypes) -> int:
        query = update.callback_query
        print(query.data)
        if query.data != "others":
            # Let the user to choose language from English and Chinese
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
            return CATEGORY
        else:
            message = f"Please enter the labels of directly chat names you want to post to"
            await query.message.edit_text(message, parse_mode="MarkdownV2")
            return LABELS

    async def choose_labels(self, update: Update, context: ContextTypes) -> int:
        text = update.message.text
        labels_or_chat_names = text.split("\n")
        print(labels_or_chat_names)

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
                POST: [CallbackQueryHandler(self.choose_category, pattern=self.tools.get_category_pattern())],
                CATEGORY: [CallbackQueryHandler(self.choose_category, pattern="^(english|chinese)$")],
                LABELS: [MessageHandler(filters.Text, self.choose_labels)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
        )

        application.add_handler(post_handler)
        application.run_polling()


if __name__ == "__main__":
    bot = AnnouncementBot()
    bot.run()
