from app.ingestion.series_registry import get_active_series
from app.ingestion.national_gas_client import NationalGasClient
from app.ingestion.transformer import transform_demand_csv
from app.ingestion.loader import upsert_observations
from app.utils.logger import logger


def run_national_gas():
    client = NationalGasClient()
    series_list = get_active_series("NATIONAL_GAS")

    for series_id, dataset_id, lookback_days in series_list:
        logger.info(
            f"Ingesting series={series_id}, "
            f"dataset={dataset_id}, "
            f"lookback_days={lookback_days}"
        )

        df = client.fetch_last_days(dataset_id, lookback_days)
        records = transform_demand_csv(df, series_id)
        upsert_observations(records)

