from sqlalchemy import insert
from app.db.connection import engine
from app.utils.logger import logger
from datetime import datetime
import pandas as pd


def sanitize_value(v):
    # Preserve complex JSON structures (zero-loss)
    if isinstance(v, (list, dict)):
        return v

    # Safely handle NaN / NaT
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    return v


def ingest_raw_df(
    df: pd.DataFrame,
    dataset_id: str,
    source: str = "NATIONAL_GAS"
):
    records = []

    for _, row in df.iterrows():
        payload = row.to_dict()

        # 🔥 FIX: JSON-safe sanitization
        payload = {k: sanitize_value(v) for k, v in payload.items()}

        records.append({
            "source": source,
            "dataset_id": dataset_id,
            "series_hint": payload.get("Data Item"),
            "event_time": None,           # parsed later
            "raw_payload": payload,
            "ingested_at": datetime.utcnow(),
        })

    if not records:
        logger.warning("No raw rows to ingest")
        return

    stmt = insert(insert_raw_events()).values(records)

    with engine.begin() as conn:
        conn.execute(stmt)

    logger.info(f"Raw-ingested {len(records)} rows for {dataset_id}")


def insert_raw_events():
    from sqlalchemy import table, column
    from sqlalchemy.dialects.postgresql import JSONB

    return table(
        "raw_events",
        column("source"),
        column("dataset_id"),
        column("series_hint"),
        column("event_time"),
        column("raw_payload", JSONB),
        column("ingested_at"),
    )
