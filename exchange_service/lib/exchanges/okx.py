import aiohttp


class BaseClient(object):
    BASE_ENDPOINT = "https://www.okx.com"

    def __init__(self) -> None:
        self._session = aiohttp.ClientSession()

    async def _request(self, method: str, url: str, **kwargs):
        return await self._handle_response(await self._session.request(method, url, **kwargs))

    async def _handle_response(self, response: aiohttp.ClientResponse):
        if response.status == 200:
            return await response.json()
        else:
            raise Exception(f"Error {response.status} {response.reason}")

    async def _get(self, url: str, **kwargs):
        return await self._request("GET", url, **kwargs)

    async def _post(self, url: str, **kwargs):
        return await self._request("POST", url, **kwargs)


class OkxUnified(BaseClient):
    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT

    async def _get_exchange_info(self, instType: str) -> dict:
        return await self._get(self.base_endpoint + "/api/v5/public/instruments", params={"instType": instType})
