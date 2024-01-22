from .base import BaseClient


class KucoinSpot(BaseClient):
    BASE_ENDPOINT = "https://api.kucoin.com"

    def __init__(self) -> None:
        super().__init__()
        self.spot_base_endpoint = self.BASE_ENDPOINT

    async def _get_exchange_info(self):
        return await self._get(self.spot_base_endpoint + "/api/v3/currencies")


class KucoinFutures(BaseClient):
    BASE_ENDPOINT = "https://api-futures.kucoin.com"