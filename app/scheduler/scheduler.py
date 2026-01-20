from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from app.scheduler.jobs import hourly_ingest, daily_ingest
from app.utils.logger import logger

scheduler = BackgroundScheduler()


def start_scheduler(run_hourly: bool = True, run_daily: bool = True):
    logger.info("🚀 Starting ingestion scheduler")

    if run_hourly:
        scheduler.add_job(
            hourly_ingest,
            trigger=IntervalTrigger(hours=1),
            id="hourly_ingest",
            replace_existing=True,
        )
        logger.info("⏰ Hourly ingestion enabled")

    if run_daily:
        scheduler.add_job(
            daily_ingest,
            trigger=CronTrigger(hour=0, minute=0),
            id="daily_ingest",
            replace_existing=True,
        )
        logger.info("🌙 Daily ingestion enabled (12:00 AM UTC)")

    scheduler.start()
