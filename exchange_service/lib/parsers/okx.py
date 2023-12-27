from datetime import timedelta as td

from .base import Parser


class OkxParser(Parser):
    interval_map = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "12h": "12H",
        "1d": "1D",
        "2d": "2D",
        "3d": "3D",
        "1w": "1W",
        "1M": "1M",
        "3M": "3M",
    }

    def _parse_leverage(self, lever: any):
        return int(lever) if lever else 1

    def _parse_symbol(self, data: dict) -> str:
        market_type = data["instType"]
        if market_type in ["SPOT", "MARGIN"]:
            symbol = f"{data['baseCcy']}/{data['quoteCcy']}"
        elif market_type in ["SWAP", "FUTURES"]:
            if data["ctType"] == "linear":
                symbol = f"{data['ctValCcy']}/{data['settleCcy']}"
            else:
                symbol = f"{data['settleCcy']}/{data['ctValCcy']}"
        return symbol

    @property
    def spot_margin_exchange_info_parser(self):
        return {
            "active": (lambda x: x["state"] == "live"),
            "is_spot": (lambda x: self.parse_is_spot(x["instType"])),
            "is_margin": (lambda x: self.parse_is_margin(x["instType"])),
            "is_futures": False,
            "is_perp": False,
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self._parse_symbol(x)),
            "base": (lambda x: self.parse_base_currency(x["baseCcy"])),
            "quote": (lambda x: str(x["quoteCcy"])),
            "settle": (lambda x: str(x["quoteCcy"])),
            "multiplier": 1,  # spot and margin default multiplier is 1
            "leverage": (lambda x: self._parse_leverage(x["lever"])),
            "listing_time": (lambda x: int(x["listTime"])),
            "expiration_time": None,  # spot not support this field
            "contract_size": 1,
            "tick_size": (lambda x: float(x["tickSz"])),
            "min_order_size": (lambda x: float(x["minSz"])),
            "max_order_size": (lambda x: float(x["maxMktSz"])),
            "raw_data": (lambda x: x),
        }

    @property
    def futures_perp_exchange_info_parser(self):
        return {
            "active": (lambda x: x["state"] == "live"),
            "is_spot": False,
            "is_margin": False,
            "is_futures": (lambda x: self.parse_is_futures(x["instType"])),
            "is_perp": (lambda x: self.parse_is_perpetual(x["instType"])),
            "is_linear": (lambda x: self.parse_is_linear(x["ctType"])),
            "is_inverse": (lambda x: self.parse_is_inverse(x["ctType"])),
            "symbol": (lambda x: self._parse_symbol(x)),
            "base": (lambda x: self.parse_base_currency(x["ctValCcy"] if x["ctType"] == "linear" else x["settleCcy"])),
            "quote": (lambda x: str(x["settleCcy"] if x["ctType"] == "linear" else x["ctValCcy"])),
            "settle": (lambda x: str(x["settleCcy"])),
            "multiplier": (lambda x: self.parse_multiplier(x["ctMult"])),
            "leverage": (lambda x: self._parse_leverage(x["lever"])),
            "listing_time": (lambda x: int(x["listTime"])),
            "expiration_time": (lambda x: int(x["expTime"]) if x["expTime"] else None),
            "contract_size": (
                lambda x: float(x["ctVal"])
            ),  # linear will be how many base ccy and inverse will be how many quote ccy
            "tick_size": (lambda x: float(x["tickSz"])),
            "min_order_size": (lambda x: float(x["minSz"])),
            "max_order_size": (lambda x: float(x["maxMktSz"])),
            "raw_data": (lambda x: x),
        }

    def parse_exchange_info(self, response: dict, parser: dict) -> dict:
        datas = response["data"]

        results = {}
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result
        return results

    def combine_spot_margin_exchange_info(self, spots: dict, margins: dict) -> dict:
        for instrument_id in margins.keys():
            margin = margins[instrument_id]
            if instrument_id in spots.keys() and margin["active"]:
                spot = spots[instrument_id]

                spot["is_margin"] = True
                spot["leverage"] = margin["leverage"]

                if spot["min_order_size"] != margin["min_order_size"]:
                    print(f"min_order_size of {instrument_id} is different between spot and margin")

                if spot["max_order_size"] != margin["max_order_size"]:
                    print(f"max_order_size of {instrument_id} is different between spot and margin")

                if spot["contract_size"] != margin["contract_size"]:
                    print(f"contract_size of {instrument_id} is different between spot and margin")

                if spot["tick_size"] != margin["tick_size"]:
                    print(f"tick_size of {instrument_id} is different between spot and margin")

                spots[instrument_id] = spot
        return spots

    def parse_ticker(self, response: any, market_type: str) -> list:
        if "data" in response:
            response = response["data"][0]

        if market_type == "spot":
            base_volume = float(response["vol24h"])
            quote_volume = float(response["volCcy24h"])
        else:
            base_volume = float(response["volCcy24h"])
            quote_volume = float(response["volCcy24h"]) * (float(response["last"]) + float(response["open24h"])) / 2

        return {
            "symbol": response["instId"],
            "open_time": self.adjust_timestamp(int(response["ts"]), td(days=-1)),
            "close_time": int(response["ts"]),
            "open": float(response["open24h"]),
            "high": float(response["high24h"]),
            "low": float(response["low24h"]),
            "last_price": float(response["last"]),
            "base_volume": base_volume,
            "quote_volume": quote_volume,
            "price_change": None,
            "price_change_percent": None,
            "raw_data": response,
        }

    def parse_tickers(self, response: dict, market_type: str) -> dict:
        datas = response["data"]

        results = []
        for data in datas:
            result = self.parse_ticker(data, market_type)
            results.append(result)
        return results

    @staticmethod
    def parse_kline(response: dict, market_type: str) -> dict:
        return {
            "open": float(response[1]),
            "high": float(response[2]),
            "low": float(response[3]),
            "close": float(response[4]),
            "base_volume": float(response[6] if market_type != "spot" else response[5]),
            "quote_volume": float(response[7]),
            "raw_data": response,
        }

    def parse_klines(self, response: dict, market_type: str) -> dict:
        datas = response["data"]

        results = {}
        for data in datas:
            result = self.parse_kline(data, market_type)
            timestamp = int(data[0])
            results[timestamp] = result
        return results

    def get_interval(self, interval: str) -> str:
        return self.interval_map[interval]
