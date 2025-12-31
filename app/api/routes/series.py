from fastapi import APIRouter
from sqlalchemy import text
from app.db.connection import engine

router = APIRouter(prefix="/series", tags=["series"])

@router.get("")
def list_series():
    query = text("""
        SELECT
            series_id,
            description,
            unit,
            frequency,
            is_active
        FROM meta_series
        WHERE is_active = true
        ORDER BY series_id
    """)
    with engine.begin() as conn:
        rows = conn.execute(query).mappings().all()
    return rows
