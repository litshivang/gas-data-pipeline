from fastapi import APIRouter, Query
from datetime import datetime, timezone, timedelta
import pandas as pd
from sqlalchemy import text
from app.db.connection import engine

router = APIRouter(prefix="/data", tags=["data"])

@router.get("/{series_id}")
def get_data(
    series_id: str,
    last_days: int | None = Query(None, gt=0, le=365),
    start: str | None = None,
    end: str | None = None,
):
    if last_days is None and (start is None or end is None):
        return {
            "error": "Provide either last_days or start & end"
        }

    if last_days is not None:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=last_days)
    else:
        start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)

    query = text("""
        SELECT
            observation_time,
            value
        FROM data_observations
        WHERE series_id = :series_id
          AND observation_time BETWEEN :start_dt AND :end_dt
        ORDER BY observation_time
    """)

    with engine.begin() as conn:
        rows = conn.execute(
            query,
            {
                "series_id": series_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
            },
        ).fetchall()

    return [
        {"timestamp": r[0], "value": r[1]}
        for r in rows
    ]
