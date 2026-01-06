from app.ingestion.national_gas_client import NationalGasClient
from app.ingestion.series_autoregister import register_series_from_df
from app.ingestion.transformer import transform_ng_csv
from app.ingestion.loader import upsert_observations
from app.utils.logger import logger
from app.ingestion.raw_ingestor import ingest_raw_df
from app.ingestion.field_discovery import discover_fields


def ingest_dataset(dataset_id: str, lookback_days: int = 30):
    logger.info(f"Ingesting National Gas dataset={dataset_id}, lookback_days={lookback_days}")

    # 🛰️ Fetch from source
    client = NationalGasClient()
    df = client.fetch_last_days(dataset_id, lookback_days)

    if df.empty:
        logger.warning(f"No data returned for dataset={dataset_id}")
        return

    # 🧱 Phase-2 Step-1: RAW INGESTION (ZERO-LOSS, ground truth)
    ingest_raw_df(df, dataset_id)

    # 🧠 Phase-2 Step-2: FIELD DISCOVERY (enumerate every column & type)
    discover_fields(dataset_id)

    # 🔥 Auto-register logical series derived from data
    series_map = register_series_from_df(df, dataset_id)

    if not series_map:
        logger.warning(f"No series registered for dataset={dataset_id}")
        return

    # 🔄 Structured ingestion into analytical model
    for data_item, series_id in series_map.items():
        subset = df[df["Data Item"] == data_item]

        if subset.empty:
            logger.warning(f"No rows for data_item={data_item} in dataset={dataset_id}")
            continue

        records = transform_ng_csv(subset, series_id)

        if not records:
            logger.warning(f"No transformed records for series_id={series_id}")
            continue

        upsert_observations(records)

    logger.info(f"Completed ingestion for dataset={dataset_id}")
