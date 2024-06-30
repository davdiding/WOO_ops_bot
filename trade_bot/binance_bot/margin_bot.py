import logging
import os

from cex_adaptors.binance import Binance
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, ConversationHandler

load_dotenv()


class BinanceMarginBot:
    BOT_KEY = os.getenv("GENERAL_MONITOR_BOT_TOKEN")
    TRADE_PASSWORD = os.getenv("BINANCE_TRADE_PASSWORD")
    ADMIN_USER_ID = os.getenv("DAVID_CHAT_ID")
    LOG_PATH = "./margin_bot.log"

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
        params = update.message.text.split(" ")[1:]

        print(params)

        if len(params) != 5:
            await update.message.reply_text(
                "Invalid number of arguments\. Should be 4 \(`instrument_id, side, amount, price`\)",
                quote=True,
                parse_mode="MarkdownV2",
            )
            return ConversationHandler.END

        # check trading password
        params = {
            "instrument_id": params[0],
            "side": params[1],
            "amount": float(params[2]),
            "price": float(params[3]) if params[3] != "market" else params[3],
            "pwd": params[4],
        }

        if params["pwd"] != self.TRADE_PASSWORD:
            await update.message.reply_text("Invalid trading password", quote=True, parse_mode="MarkdownV2")
            return ConversationHandler.END

        if params["price"] == "market":
            try:
                order = await self.exchange.place_margin_market_order(
                    instrument_id=params["instrument_id"], side=params["side"], volume=params["amount"], in_quote=False
                )
                await update.message.reply_text(
                    f"Order placed successfully\n`{order}`", quote=True, parse_mode="MarkdownV2"
                )
                self.logger.info(f"Order placed successfully. {order}")

            except Exception as e:
                await update.message.reply_text(
                    f"Failed to place order\n`{str(e)}`", quote=True, parse_mode="MarkdownV2"
                )

                self.logger.error(f"Failed to place order. {str(e)}")
        else:
            await update.message.reply_text(
                "Current implementation only supports market orders", quote=True, parse_mode="MarkdownV2"
            )

    def run(self):
        self.logger.info("Margin bot started.....")
        application = Application.builder().token(self.BOT_KEY).build()

        application.add_handler(CommandHandler("place_bn_margin_order", self.request_place_order))

        application.run_polling()


if __name__ == "__main__":
    bot = BinanceMarginBot()
    bot.run()
