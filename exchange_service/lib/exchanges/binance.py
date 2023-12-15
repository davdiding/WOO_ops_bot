import aiohttp


class BaseClient(object):
    BASE_ENDPOINT = "https://api{}.binance.com"
    LINEAR_BASE_ENDPOINT = "https://fapi.binance.com"
    INVERSE_BASE_ENDPOINT = "https://dapi.binance.com"

    def __init__(self) -> None:

        self._session = aiohttp.ClientSession()

    async def _request(self, method: str, url: str, **kwargs):
        return await self._handle_response(await self._session.request(method, url, **kwargs))

    async def _handle_response(self, response: aiohttp.ClientResponse):
        if response.status == 200:
            return await response.json()
        else:
            raise Exception(f"Error {response.status} {response.reason} {await response.text()}")

    async def _get(self, url: str, **kwargs):
        return await self._request("GET", url, **kwargs)

    async def _post(self, url: str, **kwargs):
        return await self._request("POST", url, **kwargs)

    async def close(self):
        await self._session.close()


class BinanceSpot(BaseClient):
    def __init__(self, api_version: int = 3):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT.format(api_version)

    async def _get_exchange_info(self):
        return await self._get(self.base_endpoint + "/api/v3/exchangeInfo")

    async def _get_ticker(self, symbol: str):
        return await self._get(self.base_endpoint + "/api/v3/ticker/24hr", params={"symbol": symbol})

    async def _get_tickers(self):
        return await self._get(self.base_endpoint + "/api/v3/ticker/24hr")

    async def _get_klines(
        self,
        symbol: str,
        interval: str,
        startTime: int = None,
        endTime: int = None,
        limit: int = 500,
        timeZone: str = "0",
    ):
        params = {"symbol": symbol, "interval": interval, "limit": limit, "timeZone": timeZone}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        return await self._get(self.base_endpoint + "/api/v3/klines", params=params)


class BinanceLinear(BaseClient):
    def __init__(self) -> None:
        super().__init__()
        self.linear_base_endpoint = self.LINEAR_BASE_ENDPOINT

    async def _get_exchange_info(self):
        return await self._get(self.linear_base_endpoint + "/fapi/v1/exchangeInfo")

    async def _get_ticker(self, symbol: str):
        return await self._get(self.linear_base_endpoint + "/fapi/v1/ticker/24hr", params={"symbol": symbol})

    async def _get_tickers(self):
        return await self._get(self.linear_base_endpoint + "/fapi/v1/ticker/24hr")

    async def _get_klines(
        self, symbol: str, interval: str, startTime: int = None, endTime: int = None, limit: int = 500
    ):
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        return await self._get(self.linear_base_endpoint + "/fapi/v1/klines", params=params)


class BinanceInverse(BaseClient):
    def __init__(self) -> None:
        super().__init__()
        self.inverse_base_endpoint = self.INVERSE_BASE_ENDPOINT

    async def _get_exchange_info(self):
        return await self._get(self.inverse_base_endpoint + "/dapi/v1/exchangeInfo")

    async def _get_ticker(self, symbol: str):
        return await self._get(self.inverse_base_endpoint + "/dapi/v1/ticker/24hr", params={"symbol": symbol})

    async def _get_tickers(self):
        return await self._get(self.inverse_base_endpoint + "/dapi/v1/ticker/24hr")

    async def _get_klines(
        self, symbol: str, interval: str, startTime: int = None, endTime: int = None, limit: int = 500
    ):
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        return await self._get(self.inverse_base_endpoint + "/dapi/v1/klines", params=params)
