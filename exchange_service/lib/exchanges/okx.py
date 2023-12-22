from .base import BaseClient


class OkxUnified(BaseClient):
    BASE_ENDPOINT = "https://www.okx.com"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT

    async def _get_exchange_info(self, instType: str) -> dict:
        return await self._get(self.base_endpoint + "/api/v5/public/instruments", params={"instType": instType})

    async def _get_tickers(self, instType: str):
        return await self._get(self.base_endpoint + "/api/v5/market/tickers", params={"instType": instType})

    async def _get_ticker(self, instId: str):
        return await self._get(self.base_endpoint + "/api/v5/market/ticker", params={"instId": instId})
