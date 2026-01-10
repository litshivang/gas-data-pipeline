from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from sqlalchemy import text
from app.db.connection import engine
from fastapi.responses import StreamingResponse
import pandas as pd
import io


router = APIRouter(prefix="/v2/export", tags=["Export"])


@router.get("/raw/json")
def export_raw_json(
    dataset_id: str,
    limit: int = Query(1000, ge=1, le=50000),
):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT raw_payload
                FROM raw_events
                WHERE dataset_id = :dataset_id
                ORDER BY ingested_at DESC
                LIMIT :limit
            """),
            {"dataset_id": dataset_id, "limit": limit}
        ).fetchall()

    data = [r[0] for r in rows]
    return JSONResponse(content=data)



@router.get("/raw/csv")
def export_raw_csv(
    dataset_id: str,
    limit: int = Query(1000, ge=1, le=50000),
):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT raw_payload
                FROM raw_events
                WHERE dataset_id = :dataset_id
                ORDER BY ingested_at DESC
                LIMIT :limit
            """),
            {"dataset_id": dataset_id, "limit": limit}
        ).fetchall()

    df = pd.json_normalize([r[0] for r in rows])

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={dataset_id}_raw.csv"},
    )
