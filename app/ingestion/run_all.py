from app.ingestion.national_gas_client import NationalGasClient
from app.ingestion.series_autoregister import register_series_from_df
from app.ingestion.transformer import transform_ng_csv
from app.ingestion.loader import upsert_observations
from app.utils.logger import logger


def ingest_dataset(dataset_id: str, lookback_days: int = 14):
    logger.info(f"Ingesting National Gas dataset={dataset_id}")

    client = NationalGasClient()
    df = client.fetch_last_days(dataset_id, lookback_days)

    # 🔥 Auto-register series
    series_map = register_series_from_df(df, dataset_id)

    # 🔥 Split + ingest per series
    for data_item, series_id in series_map.items():
        subset = df[df["Data Item"] == data_item]
        records = transform_ng_csv(subset, series_id)
        upsert_observations(records)
