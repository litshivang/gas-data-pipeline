from sqlalchemy.dialects.postgresql import insert
from app.db.connection import engine
from app.db.models import DataObservation
from app.utils.logger import logger


def upsert_observations(records: list[dict]) -> None:
    if not records:
        logger.warning("No records to insert.")
        return

    # 🔥 FIX: Deduplicate by unique constraint
    unique = {}
    for r in records:
        key = (r["series_id"], r["observation_time"])
        unique[key] = r   # last write wins

    deduped_records = list(unique.values())

    stmt = insert(DataObservation).values(deduped_records)

    stmt = stmt.on_conflict_do_update(
        index_elements=["series_id", "observation_time"],
        set_={
            "value": stmt.excluded.value,
            "ingestion_time": stmt.excluded.ingestion_time,
            "quality_flag": stmt.excluded.quality_flag,
            "raw_payload": stmt.excluded.raw_payload,
        },
    )

    with engine.begin() as conn:
        conn.execute(stmt)

    logger.info(f"Upserted {len(deduped_records)} observations.")
