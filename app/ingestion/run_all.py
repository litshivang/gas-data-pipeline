from app.ingestion.national_gas_client import NationalGasClient
from app.ingestion.series_autoregister import register_series_from_df
from app.ingestion.transformer import (
    transform_gas_quality_rest,
    transform_entsog_rest,
)
from app.ingestion.loader import upsert_observations
from app.utils.logger import logger
from app.ingestion.raw_ingestor import ingest_raw_df
from app.ingestion.field_discovery import discover_fields


def ingest_dataset(dataset_id: str, lookback_days: int = 30):
    logger.info(f"Ingesting dataset={dataset_id}, lookback_days={lookback_days}")

    client = NationalGasClient()
    df = client.fetch_last_days(dataset_id, lookback_days)

    if df.empty:
        logger.warning(f"No data returned for dataset={dataset_id}")
        return

    # 🧱 RAW
    ingest_raw_df(df, dataset_id)

    # 🧠 DISCOVERY
    discover_fields(dataset_id)

    # 🔥 SERIES
    series_map = register_series_from_df(df, dataset_id)

    if not series_map:
        logger.warning(f"No series registered for dataset={dataset_id}")
        return

    # 🔄 TRANSFORM + LOAD
    total_inserted = 0

    for _, series_id in series_map.items():

        if dataset_id == "GAS_QUALITY":
            records = transform_gas_quality_rest(df, series_id)

        elif dataset_id == "ENTSOG":
            records = transform_entsog_rest(df, series_id)

        else:
            logger.warning(f"No transformer for dataset={dataset_id}")
            continue

        if not records:
            logger.warning(f"No transformed records for series_id={series_id}")
            continue

        upsert_observations(records)
        total_inserted += len(records)

    logger.info(
        f"Completed ingestion for dataset={dataset_id}. "
        f"Total observations upserted={total_inserted}"
    )
