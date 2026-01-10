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



@router.get("/raw")
def raw_preview(
    dataset_id: str,
    limit: int = Query(20, ge=1, le=500),
    site_id: int | None = None,
):
    """
    Return raw payload with optional filters (still zero-loss).
    Filters apply to JSON keys using PostgreSQL JSONB operators.
    """
    where = ["dataset_id = :dataset_id"]
    params = {"dataset_id": dataset_id, "limit": limit}

    if site_id is not None:
        where.append("(raw_payload ->> 'siteId')::int = :site_id")
        params["site_id"] = site_id

    sql = f"""
        SELECT raw_payload
        FROM raw_events
        WHERE {' AND '.join(where)}
        ORDER BY ingested_at DESC
        LIMIT :limit
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    return [r[0] for r in rows]