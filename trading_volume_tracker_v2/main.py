from datetime import datetime as dt
from datetime import timedelta as td

from lib.utils import ImageCreator, Tools, send_message
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


class VolumeBot:
    BOT_KEY = "BOT_KEY"
    ADMIN_ID = "DAVID_CHAT_ID"

    def __init__(self):
        self.tools = Tools()
        self.creator = ImageCreator()

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

    async def get_volume(self, update: Update, context: ContextTypes) -> None:
        operator = update.effective_user
        if self.tools.config[self.ADMIN_ID] != str(operator.id):
            return
        else:
            try:
                currency = update.message.text.split(" ")[1].split(",")[0]
            except (IndexError, ValueError) as e:
                update.message.reply_text("Please enter a valid currency. like: /get_volume BTC")

        days = 30
        start = (dt.today() - td(days=days)).strftime("%Y-%m-%d")
        end = dt.today().strftime("%Y-%m-%d")
        volume = self.tools.get_historical_volume(currency, start=start, end=end)
        fig_path = self.creator.volume_plot(currency, volume)
        with open(fig_path, "rb") as f:
            await update.message.reply_photo(f)

    def run(self):
        application = Application.builder().token(self.tools.config[self.BOT_KEY]).build()

        application.add_handler(CommandHandler("fill_missing_symbol", self.fill_missing_symbol))
        application.add_handler(CommandHandler("fill_mongodb", self.tools.fill_mongodb))
        application.add_handler(CommandHandler("get_volume", self.get_volume))

        application.run_polling()


if __name__ == "__main__":
    volume_bot = VolumeBot()
    volume_bot.run()
