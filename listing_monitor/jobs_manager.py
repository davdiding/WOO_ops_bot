import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from main import main as monitor_main
from updater import main as updater_main

if __name__ == "__main__":
    scheduler = AsyncIOScheduler()
    scheduler.add_job(updater_main, "interval", minutes=30)
    scheduler.add_job(monitor_main, "interval", minutes=30)
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
