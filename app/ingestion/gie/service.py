from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import bindparam
from app.db.connection import engine
from app.ingestion.gie.client import GIEClient
from app.ingestion.gie.transformer import transform
from app.ingestion.gie.series_builder import get_or_create_asset, get_or_create_series
from app.ingestion.gie.constants import DELETE_LOOKBACK_DAYS


def ingest_gie(dataset: str, source: str, country: str | None = None):

    client = GIEClient()
    raw_json = client.fetch(dataset, country)

    # Store raw JSON
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw_events (source, dataset_id, raw_payload, ingested_at)
                VALUES (:source, :dataset, :payload, NOW())
            """).bindparams(
                bindparam("payload", type_=JSONB)
            ),
            {
                "source": source,
                "dataset": dataset,
                "payload": raw_json,
            }
        )

    rows = transform(dataset, raw_json)

    cutoff = datetime.utcnow().date() - timedelta(days=DELETE_LOOKBACK_DAYS)

    with engine.begin() as conn:

        # Delete last 10 days
        conn.execute(
            text("""
                DELETE FROM energy.daily d
                USING meta.series s
                WHERE d.series_id = s.series_id
                AND s.source = :source
            """),
            {"source": source}
        )



        for r in rows:
            asset_id = get_or_create_asset(
                name=r["country"],
                level="Country",
                quality=r["quality"]
            )

            series_id = get_or_create_series(
                asset_id=asset_id,
                variable=r["variable"],
                source=source
            )

            conn.execute(
                text("""
                    INSERT INTO energy.daily (value_date, value, series_id, asset_id)
                    VALUES (:date, :value, :series_id, :asset_id)
                """),
                {
                    "date": r["date"],
                    "value": r["value"],
                    "series_id": series_id,
                    "asset_id": asset_id,
                }
            )
