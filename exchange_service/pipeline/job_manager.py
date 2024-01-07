import argparse
import asyncio
import importlib

from lib.utils import Tools

from .exchange_job import ExchangeJob


class JobManager:
    def __init__(self):
        self.tools = Tools()

    async def run(self, exchange: str, job: str, **kwargs):
        if exchange not in self.tools.config["EXCHANGES"]:
            raise ValueError(f"Exchange {exchange} not supported")

        if job not in self.tools.config["EXCHANGES"][exchange]["jobs"]:
            raise ValueError(f"Job {job} not supported for exchange {exchange}")

        job_class = self.load_job_class(exchange, job)
        if not issubclass(job_class, ExchangeJob):
            raise TypeError(f"The job class for {exchange}.{job} must implement ExchangeJob")

        job_instance = job_class()
        await job_instance.run(**kwargs)

    def load_job_class(self, exchange: str, job: str) -> ExchangeJob:
        try:
            module = importlib.import_module(f"pipeline.{exchange}_job")
            job_class = getattr(module, f"{job.capitalize()}Job")
            return job_class
        except (ImportError, AttributeError) as e:
            raise ImportError(f"Cannot load job {job} for exchange {exchange}: {e}")


async def main():
    parser = argparse.ArgumentParser(description="Run exchange jobs")

    parser.add_argument("--exchange", type=str, help="Exchange name")
    parser.add_argument("--job", type=str, help="Job name")

    parser.add_argument("--start", type=str, help="Start time", default=None)
    parser.add_argument("--end", type=str, help="End time", default=None)

    args = parser.parse_args()

    job_manager = JobManager()
    await job_manager.run(**vars(args))


if __name__ == "__main__":
    asyncio.run(main())
