from .base import Parser


class BinanceParser(Parser):
    @property
    def spot_exchange_info_parser(self):
        return {
            "active": (lambda x: x["status"] == "TRADING"),
            "is_spot": True,
            "is_margin": (lambda x: x["isMarginTradingAllowed"]),
            "is_futures": False,
            "is_perp": False,
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self.parse_unified_symbol(x["baseAsset"], x["quoteAsset"])),
            "base": (lambda x: str(x["baseAsset"])),
            "quote": (lambda x: str(x["quoteAsset"])),
            "settle": (lambda x: str(x["quoteAsset"])),
            "multiplier": 1,  # spot multiplier default 1
            "leverage": 1,  # spot leverage default 1
            "listing_time": None,  # api not support this field
            "expiration_time": None,  # spot not support this field
            "contract_size": 1,  # spot contract size default 1
            "tick_size": None,  # Not yet implemented
            "min_order_size": None,  # Not yet implemented
            "max_order_size": None,  # Not yet implemented
            "raw_data": (lambda x: x),
        }

    @property
    def futures_exchange_info_parser(self):
        pass

    def parse_exchange_info(self, response: dict, parser: dict) -> dict:
        datas = response["symbols"]
        results = {}

        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result

        return results

    def parse_ticker(self, response: dict):
        return response

    def parse_tickers(self, response: dict):

        return response

    def get_symbol(self, info: dict) -> str:
        return f'{info["base"]}{info["quote"]}'
