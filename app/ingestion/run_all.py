from app.ingestion.national_gas_client import NationalGasClient
from app.ingestion.series_autoregister import register_series_from_df
from app.ingestion.transformer import (
    transform_gas_quality_rest,
    transform_entsog_rest,
    transform_instantaneous_flow,
)
from app.ingestion.loader import upsert_observations
from app.utils.logger import logger
from app.ingestion.raw_ingestor import ingest_raw_df
from app.ingestion.field_discovery import discover_fields


def ingest_dataset(
    dataset_id: str,
    from_date: str | None = None,
    to_date: str | None = None,
    site_ids: list[int] | None = None,
    operator_keys: list[str] | None = None,
    point_keys: list[str] | None = None,
    direction_keys: list[str] | None = None,
    indicators: list[str] | None = None,
    limit: int | None = None,
):
    logger.info(
        f"Ingesting dataset={dataset_id}, from={from_date}, to={to_date}, "
        f"sites={site_ids}, operators={operator_keys}, points={point_keys}, "
        f"directions={direction_keys}, indicators={indicators}, limit={limit}"
    )

    client = NationalGasClient()

    # ---------------- GAS QUALITY ----------------
    if dataset_id == "GAS_QUALITY":
        df = client.fetch_gas_quality(
            from_date=from_date,
            to_date=to_date,
            site_ids=site_ids
        )

    # ---------------- ENTSOG ----------------
    elif dataset_id == "ENTSOG":
        df = client.fetch_entsog(
            from_date=from_date,
            to_date=to_date,
            operator_keys=operator_keys,
            point_keys=point_keys,
            direction_keys=direction_keys,
            indicators=indicators,
            limit=limit,
        )
    
    # ---------------- INSTANTANEOUS FLOW ----------------        
    elif dataset_id == "INSTANTANEOUS_FLOW":
        df = client.fetch_instantaneous_flow(
            from_date=from_date,
            to_date=to_date,
        )

    else:
        raise ValueError(f"Unsupported dataset_id for API ingestion: {dataset_id}")

    if df.empty:
        logger.warning(f"No data returned for dataset={dataset_id}")
        return

    # 🧱 RAW (zero-loss)
    ingest_raw_df(df, dataset_id)

    # 🧠 DISCOVERY (auto schema)
    discover_fields(dataset_id)

    # 🔥 SERIES (auto-register)
    series_map = register_series_from_df(df, dataset_id)
    if not series_map:
        logger.warning(f"No series registered for dataset={dataset_id}")
        return

    # 🔄 TRANSFORM + LOAD
    for _, series_id in series_map.items():

        if dataset_id == "GAS_QUALITY":
            records = transform_gas_quality_rest(df, series_id)

        elif dataset_id == "ENTSOG":
            records = transform_entsog_rest(
                df,
                series_id,
                from_date=from_date,
                to_date=to_date,
            )
            
        elif dataset_id == "INSTANTANEOUS_FLOW":
            records = transform_instantaneous_flow(df, series_id)            

        else:
            logger.warning(f"No transformer for dataset={dataset_id}")
            continue

        if not records:
            logger.warning(f"No transformed records for series_id={series_id}")
            continue

        upsert_observations(records)

    logger.info(f"Completed ingestion for dataset={dataset_id}")
