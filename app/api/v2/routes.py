from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from collections import defaultdict

from app.db.connection import get_db_session
from app.api.v2.schemas import SeriesResponse, DataPoint
from app.api.v2.queries import DATA_QUERY

router = APIRouter(prefix="/v2", tags=["v2"])


@router.get("/data", response_model=list[SeriesResponse])
def get_data(
    series_id: str | None = None,
    dataset_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = Query(1000, le=5000),
    offset: int = 0,
    include_raw: bool = False,
    db: Session = Depends(get_db_session),
):
    rows = db.execute(
        DATA_QUERY,
        {
            "series_id": series_id,
            "dataset_id": dataset_id,
            "start": start,
            "end": end,
            "limit": limit,
            "offset": offset,
        },
    ).fetchall()

    grouped = defaultdict(lambda: {
        "points": []
    })

    for r in rows:
        key = r.series_id
        grouped[key].update({
            "series_id": r.series_id,
            "dataset_id": r.dataset_id,
            "description": r.description,
            "unit": r.unit,
            "frequency": r.frequency,
        })

        grouped[key]["points"].append(
            DataPoint(
                timestamp=r.observation_time,
                value=r.value,
                quality_flag=r.quality_flag,
                raw_payload=r.raw_payload if include_raw else None,
            )
        )

    return list(grouped.values())
