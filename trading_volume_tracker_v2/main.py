from lib.utils import Tools, send_message
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


class VolumeBot:
    BOT_KEY = "BOT_KEY"
    ADMIN_ID = "DAVID_CHAT_ID"

    def __init__(self):
        self.tools = Tools()

    async def fill_missing_symbol(self, update: Update, context: ContextTypes) -> None:
        operator = update.effective_user
        if self.tools.config[self.ADMIN_ID] != str(operator.id):
            return
        else:
            self.tools.fill_missing_symbol()
            send_message(
                token=self.tools.config[self.BOT_KEY],
                message="Finish filling missing symbol",
                chat_id=self.tools.config[self.ADMIN_ID],
            )

    async def fill_mongodb(self, update: Update, context: ContextTypes) -> None:
        operator = update.effective_user
        if self.tools.config[self.ADMIN_ID] != str(operator.id):
            return
        else:
            self.tools.start()
            send_message(
                token=self.tools.config[self.BOT_KEY],
                message="Finish filling mongodb",
                chat_id=self.tools.config[self.ADMIN_ID],
            )

    def run(self):
        application = Application.builder().token(self.tools.config[self.BOT_KEY]).build()

        application.add_handler(CommandHandler("fill_missing_symbol", self.fill_missing_symbol))
        application.add_handler(CommandHandler("fill_mongodb", self.tools.start))

        application.run_polling()


if __name__ == "__main__":
    volume_bot = VolumeBot()
    volume_bot.run()
