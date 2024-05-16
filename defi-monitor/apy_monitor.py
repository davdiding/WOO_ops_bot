import asyncio
import os

import pandas as pd
from dex_adaptors.aave_v3 import AaveV3
from dex_adaptors.compound_v2 import CompoundV2
from dotenv import load_dotenv
from lib.utils import Tools

load_dotenv()


class ApyMonitor:
    target_currency_list = {
        "WBTC": ["WBTC", "BTC"],
        "WETH": ["WETH", "ETH"],
        "USDT": ["USDT"],
        "USDC": ["USDC"],
        "DAI": ["DAI"],
    }

    credential_path = os.getenv("GC_CREDENTIAL_PATH")
    dashboard_url = os.getenv("DASHBOARD_URL")
    dashboard_name = "APY market monitor"

    def __init__(self):
        self.tool = Tools()
        self.gc = self.tool.init_gc_client(self.credential_path)
        self.dashboard = self.tool.init_wks(self.gc, self.dashboard_url, self.dashboard_name)

    def get_dashboard_index(self):
        sheet = self.dashboard.get_as_df().iloc[:7, :3]
        sheet.set_index("Currency", inplace=True)
        sheet.columns = sheet.iloc[1]
        sheet = sheet[sheet.index != ""]
        sheet.iloc[:, :] = "-"
        return sheet

    async def update_apy_data(self):
        aave = AaveV3()
        compound = CompoundV2()

        aave_borrow_rates = await aave.get_borrow_rates(1000)
        compound_markets = await compound.get_markets(1000)

        results = pd.DataFrame(columns=["protocol", "currency", "borrow_rate", "supply_rate"])
        for currency in self.target_currency_list:
            for i in self.target_currency_list[currency]:
                if i in aave_borrow_rates:
                    results.loc[len(results)] = ["aave", currency, aave_borrow_rates[i]["variable_borrow_rate"], "-"]
                if i in compound_markets:
                    results.loc[len(results)] = [
                        "compound",
                        currency,
                        compound_markets[i]["borrow_rate"],
                        compound_markets[i]["supply_rate"],
                    ]
        results.set_index("currency", inplace=True)

        dashboard = self.get_dashboard_index()

        aave = results.loc[dashboard.index].loc[results["protocol"] == "aave"][["borrow_rate", "supply_rate"]]
        compound = results.loc[dashboard.index].loc[results["protocol"] == "compound"][["borrow_rate", "supply_rate"]]

        self.tool.update_wks(aave, self.dashboard, "B4", first_clear=False, only_values=True)
        self.tool.update_wks(compound, self.dashboard, "D4", first_clear=False, only_values=True)
        return

    async def run(self):
        await self.update_apy_data()
        self.dashboard.update_value("A9", f"Last updated: {self.tool.get_datetime()} (utc+0)")


async def main():
    apy_monitor = ApyMonitor()
    await apy_monitor.run()


if __name__ == "__main__":
    asyncio.run(main())
