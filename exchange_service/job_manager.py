import argparse
import asyncio

from pipeline.binance import KlinesJob, TickersJob


class JobManager(object):
    def __init__(self):
        self.jobs = {
            "binance": {
                "tickers": TickersJob(),
                "klines": KlinesJob(),
            },
        }

    async def run(self, exchange, job, **kwargs):
        job = self.jobs[exchange][job]
        await job.run(**kwargs)


async def main():
    parser = argparse.ArgumentParser("Job Manager")

    parser.add_argument("--job", type=str, help="job name")
    parser.add_argument("--exchange", type=str, help="exchange name")
    parser.add_argument("--start", type=str, help="start date", default=None)
    parser.add_argument("--end", type=str, help="end date", default=None)
    args = parser.parse_args()

    job_manager = JobManager()
    await job_manager.run(args.exchange, args.job, start=args.start, end=args.end)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
