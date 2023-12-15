import argparse
import asyncio

from pipeline.jobs import BinanceJob


class JobManager(object):
    def __init__(self):
        self.jobs = {BinanceJob.NAME: BinanceJob()}

    async def run(self, job_name):
        return await self.jobs[job_name].run()


async def main():
    parser = argparse.ArgumentParser("Job Manager")
    parser.add_argument("--job", type=str, help="job name")
    args = parser.parse_args()
    job_manager = JobManager()
    await job_manager.run(args.job)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
