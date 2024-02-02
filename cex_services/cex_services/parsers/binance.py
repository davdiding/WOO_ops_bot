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

    def futures_exchange_info_parser(self, market_type: str):
        return {
            "active": (lambda x: (x["status"] if market_type != "inverse" else x["contractStatus"]) == "TRADING"),
            "is_spot": False,
            "is_margin": False,
            "is_futures": (lambda x: self.parse_is_futures(x["contractType"])),
            "is_perp": (lambda x: self.parse_is_perpetual(x["contractType"])),
            "is_linear": True if market_type == "linear" else False,
            "is_inverse": True if market_type == "inverse" else False,
            "symbol": (lambda x: self.parse_unified_symbol(self.parse_base_currency(x["baseAsset"]), x["quoteAsset"])),
            "base": (lambda x: self.parse_base_currency(x["baseAsset"])),
            "quote": (lambda x: x["quoteAsset"]),
            "settle": (lambda x: x["marginAsset"]),
            "multiplier": (lambda x: self.parse_multiplier(x["baseAsset"])),
            "leverage": 1,  # need to find another way to get the leverage data
            "listing_time": (lambda x: int(x["onboardDate"])),
            "expiration_time": (lambda x: int(x["deliveryDate"])),
            "contract_size": (
                lambda x: 1 if "contractSize" not in x else float(x["contractSize"])
            ),  # binance only have contract size to inverse perp and futures
            "tick_size": None,  # not yet implemented
            "min_order_size": None,  # not yet implemented
            "max_order_size": None,  # not yet implemented
            "raw_data": (lambda x: x),
        }

    def parse_exchange_info(self, response: dict, parser: dict) -> dict:
        datas = response["symbols"]
        results = {}

        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result

        return results

    def parse_ticker(self, response: dict, market_type: str) -> dict:
        return {
            "symbol": response["symbol"],
            "open_time": int(response["openTime"]),
            "close_time": int(response["closeTime"]),
            "open": float(response["openPrice"]),
            "high": float(response["highPrice"]),
            "low": float(response["lowPrice"]),
            "last_price": float(response["lastPrice"]),
            "base_volume": float(response["volume"] if market_type != "inverse" else response["baseVolume"]),
            "quote_volume": float(response["quoteVolume"] if market_type != "inverse" else response["volume"]),
            "price_change": float(response["priceChange"]),
            "price_change_percent": float(response["priceChangePercent"]) / 100,
            "raw_data": response,
        }

    def parse_tickers(self, response: dict, market_type: str) -> list:
        datas = response
        results = []
        for data in datas:
            result = self.parse_ticker(data, market_type)
            results.append(result)
        return results

    def parse_kline(self, response: list, market_type: str) -> dict:
        """
        # spot
        [
            [
                1499040000000,      // Kline open time
                "0.01634790",       // Open price
                "0.80000000",       // High price
                "0.01575800",       // Low price
                "0.01577100",       // Close price
                "148976.11427815",  // Volume
                1499644799999,      // Kline Close time
                "2434.19055334",    // Quote asset volume
                308,                // Number of trades
                "1756.87402397",    // Taker buy base asset volume
                "28.46694368",      // Taker buy quote asset volume
                "0"                 // Unused field, ignore.
            ]
        ]

        # linear
        # inverse
        """
        return {
            int(response[0]): {
                "open": float(response[1]),
                "high": float(response[2]),
                "low": float(response[3]),
                "close": float(response[4]),
                "base_volume": float(response[7]),
                "quote_volume": float(response[5]),
                "close_time": int(response[6]),
                "raw_data": response,
            }
        }

    def parse_klines(self, response: list, market_type: str) -> list:
        results = {}
        for data in response:
            result = self.parse_kline(data, market_type)
            results.update(result)
        return results

    def get_symbol(self, info: dict) -> str:
        return f'{info["base"]}{info["quote"]}'

    def get_market_type(self, info: dict):
        if info["is_spot"]:
            return "spot"
        elif info["is_linear"]:
            return "linear"
        elif info["is_inverse"]:
            return "inverse"
        else:
            raise Exception("market type not found")
