"""
Centralized delete policy. Config-driven.
Core applies before insert. Adapters do nothing.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from sqlalchemy import text

from app.utils.logger import logger


def apply(
    dataset_id: str,
    time_field: str,
    engine: Any,
    config: Dict[str, Any],
) -> int:
    """
    Apply delete policy for dataset. Returns rows_deleted.
    Config: delete_strategy (e.g. last_n_days), delete_window_days (e.g. 10).
    """
    strategy = config.get("delete_strategy")
    window_days = config.get("delete_window_days")

    if not strategy or strategy != "last_n_days" or not window_days:
        return 0

    try:
        window = int(window_days)
    except (TypeError, ValueError):
        logger.warning("delete_window_days must be an integer; skipping delete")
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=window)

    # Delete observations older than cutoff for this dataset's series.
    # time_field from adapter.get_time_field() (e.g. observation_time).
    if time_field != "observation_time":
        logger.warning("delete_policy uses observation_time in data_observations")
    sql = text("""
        WITH series_to_delete AS (
            SELECT series_id FROM meta_series WHERE dataset_id = :dataset_id
        )
        DELETE FROM data_observations
        WHERE series_id IN (SELECT series_id FROM series_to_delete)
        AND observation_time < :cutoff
    """)
    with engine.begin() as conn:
        result = conn.execute(sql, {"dataset_id": dataset_id, "cutoff": cutoff})
        rows_deleted = result.rowcount if result.rowcount is not None else 0

    logger.info(
        "delete_policy: dataset_id=%s, strategy=%s, window_days=%s, deleted=%s",
        dataset_id, strategy, window, rows_deleted,
    )
    return rows_deleted
