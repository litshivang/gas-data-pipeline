from fastapi import APIRouter, Query, HTTPException, BackgroundTasks
from app.ingestion.run_all import ingest_dataset
from datetime import datetime
from typing import List, Optional


router = APIRouter(prefix="/v2/ingest", tags=["Ingestion"])

@router.post("/gas")
def ingest_gas_quality(
    background_tasks: BackgroundTasks,
    from_date: str = Query(..., description="YYYY-MM-DD"),
    to_date: str = Query(..., description="YYYY-MM-DD"),
    site_ids: Optional[List[int]] = Query(
        default=None,
        description="Optional site filter. Omit to ingest all sites.",
    ),
):
    # ---------------- VALIDATION ----------------
    try:
        f = datetime.strptime(from_date, "%Y-%m-%d")
        t = datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    if t < f:
        raise HTTPException(status_code=400, detail="to_date must be >= from_date")

    # ---------------- BACKGROUND INGEST ----------------
    background_tasks.add_task(
        ingest_dataset,
        dataset_id="GAS_QUALITY",
        from_date=from_date,
        to_date=to_date,
        site_ids=site_ids,   # None = all sites
    )

    # ---------------- IMMEDIATE RESPONSE ----------------
    return {
        "status": "accepted",
        "message": "Ingestion started in background",
        "dataset": "GAS_QUALITY",
        "from": from_date,
        "to": to_date,
        "site_ids": site_ids,   # Will be null if not provided
    }


@router.post("/entsog")
def ingest_entsog(
    background_tasks: BackgroundTasks,
    from_date: str = Query(...),
    to_date: str = Query(...),
    operator_keys: list[str] = Query(None),
    point_keys: list[str] = Query(None),
    direction_keys: list[str] = Query(None),
    indicators: list[str] = Query(None),
    limit: int = Query(1000),
):
    background_tasks.add_task(
        ingest_dataset,
        dataset_id="ENTSOG",
        from_date=from_date,
        to_date=to_date,
        operator_keys=operator_keys,
        point_keys=point_keys,
        direction_keys=direction_keys,
        indicators=indicators,
        limit=limit,
    )

    return {
        "status": "accepted",
        "dataset": "ENTSOG",
        "from": from_date,
        "to": to_date,
        "filters": {
            "operator_keys": operator_keys,
            "point_keys": point_keys,
            "direction_keys": direction_keys,
            "indicators": indicators,
        }
    }
