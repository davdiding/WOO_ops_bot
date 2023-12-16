# exchange_job.py

from abc import ABC, abstractmethod


class ExchangeJob(ABC):
    @abstractmethod
    async def run(self, **kwargs):
        raise NotImplementedError
