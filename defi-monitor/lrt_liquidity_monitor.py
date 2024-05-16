import asyncio
import os

from dex_adaptors.balancer_v2 import BalancerV2
from dex_adaptors.curve import Curve
from dotenv import load_dotenv
from lib.utils import Tools

load_dotenv()


class LiquidityMonitor:
    curve_pool_map = {
        "weETH/WETH": {"address": "0x13947303f63b363876868d070f14dc865c36463b"},
        "ezETH/WETH": {"address": "0x85de3add465a219ee25e04d22c39ab027cf5c12e"},
        "pufETH/WETH": {"address": "0x39f5b252de249790faed0c2f05abead56d2088e1"},
        "stETH/ETH": {"address": "0xdc24316b9ae028f1497c275eb9192a3ea0f67022"},
        "pufETH/wstETH": {"address": "0xeeda34a377dd0ca676b9511ee1324974fa8d980d"},
    }
    balancer_pool_map = {
        "ezETH/WETH": {"pool_id": "0x596192bb6e41802428ac943d2f1476c1af25cc0e000000000000000000000659"},
        "ezETH/weETH/rswETH": {"pool_id": "0x848a5564158d84b8a8fb68ab5d004fae11619a5400000000000000000000066a"},
        "rETH/weETH": {"pool_id": "0x05ff47afada98a98982113758878f9a8b9fdda0a000000000000000000000645"},
        "weETH/WETH": {"pool_id": "0xb9debddf1d894c79d2b2d09f819ff9b856fca55200000000000000000000062a"},
        "rsETH/WETH": {"pool_id": "0x58aadfb1afac0ad7fca1148f3cde6aedf5236b6d00000000000000000000067f"},
        "pufETH/wstETH": {"pool_id": "0x63e0d47a6964ad1565345da9bfa66659f4983f02000000000000000000000681"},
        "wstETH/WETH": {"pool_id": "0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd0000000000000000000005c2"},
        "rETH/WETH": {"pool_id": "0x1e19cf2d73a72ef1332c882f20534b6519be0276000200000000000000000112"},
    }

    credential_path = os.getenv("GC_CREDENTIAL_PATH")
    dashboard_url = os.getenv("DASHBOARD_URL")
    dashboard_name = "LRT markets liquidity pool data"

    def __init__(self):
        self.tool = Tools()

        self.gc = self.tool.init_gc_client(self.credential_path)
        self.dashboard = self.tool.init_wks(self.gc, self.dashboard_url, self.dashboard_name)

    def get_dashboard_index(self):
        sheet = self.dashboard.get_as_df().drop("pair type", axis=1)
        sheet.set_index("pair", inplace=True)
        sheet.columns = sheet.iloc[0]
        sheet = sheet[sheet.index != ""]
        sheet.iloc[:, :] = "-"
        return sheet.iloc[:, :5]

    async def update_balancer_v2_lr_data(self):
        bal = BalancerV2()
        dashboard = self.get_dashboard_index()

        for pool in self.balancer_pool_map:
            print(f"Getting data for {pool} pool from Balancer V2 on ethereum")
            pool_id = self.balancer_pool_map[pool]["pool_id"]
            data = await bal.get_pool_data(pool_id)
            self.balancer_pool_map[pool]["address"] = data["address"]
            token_list = pool.split("/")

            base = [i for i in data["currency"] if i["symbol"] == token_list[0]][0]
            quote = [i for i in data["currency"] if i["symbol"] == token_list[1]][0]

            self.balancer_pool_map[pool]["data"] = {
                "base_qty": base["liquidity"],
                "quote_qty": quote["liquidity"],
                "base_usd": None,
                "quote_usd": None,
                "pool_usd": data["pool_usd"],
            }

            if pool in dashboard.index or pool.replace("/WETH", "/ETH") in dashboard.index:
                symbol = pool if pool in dashboard.index else pool.replace("/WETH", "/ETH")
                dashboard.loc[symbol] = [
                    base["liquidity"],
                    None,
                    quote["liquidity"],
                    None,
                    data["pool_usd"] / 1000,
                ]
            continue
        dashboard.fillna("-", inplace=True)
        self.tool.update_wks(dashboard, self.dashboard, "H3", first_clear=False, only_values=True)
        return

    async def update_curve_lrt_data(self):

        cur = Curve()
        curve_pools_data = await cur.get_pools_data("ethereum")
        dashboard = self.get_dashboard_index()

        for pool in self.curve_pool_map:
            print(f"Getting data for {pool} pool from Curve on ethereum")
            pool_address = self.curve_pool_map[pool]["address"]
            token_list = pool.split("/")
            data = [i for i in curve_pools_data.values() if i["address"] == pool_address][0]

            base = [i for i in data["currency"] if i["symbol"] == token_list[0]][0]
            quote = [i for i in data["currency"] if i["symbol"] == token_list[1]][0]
            self.curve_pool_map[pool]["data"] = {
                "base_qty": base["liquidity"],
                "quote_qty": quote["liquidity"],
                "base_usd": base["price_usd"] * base["liquidity"],
                "quote_usd": quote["price_usd"] * quote["liquidity"],
                "pool_usd": data["pool_usd"],
            }

            if pool in dashboard.index or pool.replace("/WETH", "/ETH") in dashboard.index:
                symbol = pool if pool in dashboard.index else pool.replace("/WETH", "/ETH")
                dashboard.loc[symbol] = [
                    base["liquidity"],
                    base["price_usd"] * base["liquidity"] / 1000,
                    quote["liquidity"],
                    quote["price_usd"] * quote["liquidity"] / 1000,
                    data["pool_usd"] / 1000,
                ]
            continue

        dashboard.fillna("-", inplace=True)
        self.tool.update_wks(dashboard, self.dashboard, "C3", first_clear=False, only_values=True)
        await cur.close()
        return

    async def run(self, name: str):
        if name == "Curve":
            await self.update_curve_lrt_data()
        elif name == "Balancer":
            await self.update_balancer_v2_lr_data()

        self.dashboard.update_value("A13", f"Updated at {self.tool.get_datetime()}")
        return


async def main():
    lm = LiquidityMonitor()
    await lm.run("Balancer")
    await lm.run("Curve")


if __name__ == "__main__":
    asyncio.run(main())
