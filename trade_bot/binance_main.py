import asyncio
import logging
import os

from cex_adaptors.binance import Binance
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (  # CallbackQueryHandler,; MessageHandler,; filters,
    Application,
    CommandHandler,
    ContextTypes,
)

load_dotenv()


class BinanceMarginBot:
    BOT_KEY = os.getenv("GENERAL_MONITOR_BOT_TOKEN")
    TRADE_PASSWORD = os.getenv("BINANCE_TRADE_PASSWORD")
    ADMIN_USER_ID = os.getenv("DAVID_CHAT_ID")
    LOG_PATH = "./logs/binance_margin_bot.log"

    def __init__(self):
        self.exchange = Binance(
            api_key=os.getenv("BINANCE_API_TRADE_KEY"), api_secret=os.getenv("BINANCE_API_TRADE_SECRET")
        )
        self.logger = self.init_logger()

    def init_logger(self) -> logging.Logger:
        logger = logging.getLogger("binance_margin_bot")
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler = logging.FileHandler(self.LOG_PATH)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    async def request_place_order(self, update: Update, context: ContextTypes):
        if update.message.from_user.id != int(self.ADMIN_USER_ID):
            return

        # params = update.message.text.split(" ")

    async def run(self):
        self.logger.info("Margin bot started.....")
        application = Application.builder().token(self.BOT_KEY).build()

        application.add_handler(CommandHandler("place_order", self.request_place_order))


if __name__ == "__main__":
    bot = BinanceMarginBot()
    asyncio.run(bot.run())
