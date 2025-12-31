from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.ingestion.run_all import run_national_gas
from app.utils.logger import logger


def start_scheduler():
    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        func=run_national_gas,
        trigger=IntervalTrigger(minutes=1),
        id="national_gas_ingestion",
        name="Hourly National Gas Ingestion",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    logger.info("Scheduler started: National Gas ingestion every hour")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped gracefully")
