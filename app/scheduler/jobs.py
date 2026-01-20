from datetime import datetime, timedelta
from app.ingestion.run_all import ingest_dataset
from app.utils.logger import logger


# -----------------------------
# HOURLY INGESTION
# -----------------------------
def hourly_ingest():
    logger.info("⏰ Running hourly ingestion job")

    to_date = datetime.utcnow().date().isoformat()
    from_date = (datetime.utcnow() - timedelta(hours=1)).date().isoformat()

    ingest_dataset(
        dataset_id="GAS_QUALITY",
        from_date=from_date,
        to_date=to_date,
        site_ids=[77],   # can later be dynamic
    )


# -----------------------------
# DAILY INGESTION @ 12:00 AM
# -----------------------------
def daily_ingest():
    logger.info("🌙 Running daily ingestion job")

    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)

    ingest_dataset(
        dataset_id="GAS_QUALITY",
        from_date=yesterday.isoformat(),
        to_date=today.isoformat(),
        site_ids=[77],   # can later be dynamic
    )

