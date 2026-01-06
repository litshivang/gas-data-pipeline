from fastapi import APIRouter, Query
from sqlalchemy import text
from app.db.connection import engine

router = APIRouter(prefix="/v2/discovery", tags=["Discovery"])


@router.get("/datasets")
def list_datasets():
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT dataset_id FROM raw_events ORDER BY dataset_id")
        ).fetchall()
    return [r[0] for r in rows]


@router.get("/fields")
def list_fields(dataset_id: str):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT field_name, inferred_type, nullable, example_value
                FROM field_catalog
                WHERE dataset_id = :dataset_id
                ORDER BY field_name
            """),
            {"dataset_id": dataset_id}
        ).fetchall()

    return [
        {
            "field": r[0],
            "type": r[1],
            "nullable": r[2],
            "example": r[3],
        }
        for r in rows
    ]


@router.get("/sample")
def sample_data(dataset_id: str, limit: int = Query(5, le=50)):
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

    return [r[0] for r in rows]
