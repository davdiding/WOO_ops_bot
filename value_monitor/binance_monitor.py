import asyncio
import os

import pandas as pd
from beautifultable import BeautifulTable
from cex_adaptors.binance import Binance
from dotenv import load_dotenv
from telegram import Bot

load_dotenv()


class Monitor:

    IGNORE_CCY = ["USDT"]

    def __init__(self):
        self.binance = Binance(api_key=os.getenv("BINANCE_API_KEY"), api_secret=os.getenv("BINANCE_API_SECRET"))
        self.chat_id = os.getenv("DAVID_CHAT_ID")
        self.bot = Bot(token=os.getenv("GENERAL_MONITOR_BOT_TOKEN"))

    async def run(self) -> bool:
        await self.binance.sync_exchange_info()
        margin_balance = await self.binance.get_margin_balance()
        margin_value = await self.binance.get_margin_account_value()

        # get the top 5 exposure currency, sorted by value
        exposure = await self.get_top_exposure(margin_balance)

        message = self.get_exposure_message(exposure, margin_value)

        await self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode="HTML")
        return True

    async def get_top_exposure(self, balance: dict, num: int = 5) -> pd.DataFrame:
        df = pd.DataFrame(balance).T.reset_index().drop("index", axis=1)
        lp = await self.get_currency_last_price(list(balance.keys()))

        df["lp"] = df["currency"].map(lp).fillna(1).astype(float).round(3)
        df["balance"] = df["balance"].astype(float).astype(float).round(3)
        df["value"] = (df["balance"] * df["lp"]).astype(float).round(1)

        return df.sort_values("value", ascending=False).head(num)[["currency", "balance", "lp", "value"]]

    async def get_currency_last_price(self, ccys: list) -> dict:
        ccys = [ccy for ccy in ccys if ccy not in self.IGNORE_CCY]

        batch_size = 20

        last_prices = {}
        for i in range(0, len(ccys), batch_size):
            tasks = [self.binance.get_ticker(f"{ccy}/USDT:USDT") for ccy in ccys[i : i + batch_size]]
            raw_results = await asyncio.gather(*tasks)

            for ccy, ticker in zip(ccys[i : i + batch_size], raw_results):
                last_prices[ccy] = ticker[f"{ccy}/USDT:USDT"]["last_price"]

        await self.binance.close()
        return last_prices

    def get_exposure_message(self, exposure: pd.DataFrame, margin_value: float) -> str:
        # get exposure table with beautiful Table
        datetime = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        table = BeautifulTable()
        table.columns.header = ["Currency", "Balance", "Last Price", "Value"]
        for _, row in exposure.iterrows():
            table.rows.append([row["currency"], row["balance"], row["lp"], row["value"]])

        final_message = f"""
        <b>Binance Margin Account Exposure</b>\n
<b>Time:</b> <code>{datetime}</code>\n
<b>Margin Value:</b> <code>{round(margin_value, 2)} USDT</code>\n
<b>Top 5 Exposure:</b>\n
        <pre>{table}</pre>
        """
        return final_message


async def main():
    monitor = Monitor()
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())
