from datetime import datetime


class Parser:
    def __init__(self) -> None:
        pass

    def get_result_with_parser(self, data: dict, parser: dict) -> dict:
        results = {}
        for key in parser:
            if callable(parser[key]):
                results[key] = parser[key](data)
            else:
                results[key] = parser[key]
        return results

    def parse_timestamp_to_str(self, timestamp: int, _format: str = "%y%m%d") -> str:
        return datetime.fromtimestamp(timestamp / 1000).strftime(_format)

    def parse_unified_id(self, info: dict) -> str:
        if info["is_perp"]:
            instrument_id = f"{info['base']}/{info['quote']}:{info['settle']}-PERP"
        elif info["is_futures"]:
            instrument_id = (
                f"{info['base']}/{info['quote']}:{info['settle']}-"
                f"{self.parse_timestamp_to_str(info['expiration_time'])}"
            )
        else:
            instrument_id = f"{info['base']}/{info['quote']}:{info['settle']}"

        multiplier = info["multiplier"]
        return f"{multiplier if multiplier != 1 and multiplier else ''}{instrument_id}"

    def parse_unified_symbol(self, base: str, quote: str) -> str:
        return f"{base}/{quote}"
