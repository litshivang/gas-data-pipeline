from sqlalchemy import text
from app.db.connection import engine

def get_active_series(source_type: str):
    query = text("""
        SELECT
            series_id,
            dataset_id,
            lookback_days
        FROM meta_series
        WHERE is_active = true
          AND source_type = :source_type
          AND dataset_id IS NOT NULL
    """)
    with engine.begin() as conn:
        return conn.execute(query, {"source_type": source_type}).fetchall()
