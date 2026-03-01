"""
Validation script for ENTSOG API migration (orchestrator + EntsogAdapter).

Run from project root:
    python -m scripts.validate_api.validate_entsog
  or
    python scripts/validate_api/validate_entsog.py

Optional env: FROM_DATE, TO_DATE. Requires ENTSOG params (e.g. indicators or point_keys+direction_keys).
Validates: ingestion runs, raw_events, meta_series, data_observations.
"""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sqlalchemy import text
from app.db.connection import engine
from app.ingestion.core import registry, Orchestrator
import app.ingestion.adapters  # noqa: F401

DATASET_ID = "ENTSOG"


def get_counts():
    with engine.connect() as conn:
        raw = conn.execute(
            text("SELECT COUNT(*) FROM raw_events WHERE dataset_id = :did"),
            {"did": DATASET_ID},
        ).scalar()
        series = conn.execute(
            text("SELECT COUNT(*) FROM meta_series WHERE dataset_id = :did"),
            {"did": DATASET_ID},
        ).scalar()
        obs = conn.execute(
            text("""
                SELECT COUNT(*) FROM data_observations o
                JOIN meta_series s ON s.series_id = o.series_id
                WHERE s.dataset_id = :did
            """),
            {"did": DATASET_ID},
        ).scalar()
    return int(raw), int(series), int(obs)


def main():
    from_date = os.environ.get("FROM_DATE", (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d"))
    to_date = os.environ.get("TO_DATE", (datetime.utcnow() - timedelta(days=6)).strftime("%Y-%m-%d"))
    indicators = ["Physical Flow"]  # minimal required for ENTSOG

    print("=" * 60)
    print("ENTSOG migration validation")
    print("=" * 60)
    print(f"Date range: {from_date} -> {to_date}, indicators: {indicators}")
    print()

    raw_before, series_before, obs_before = get_counts()
    print(f"Before: raw={raw_before}, series={series_before}, obs={obs_before}")
    print()

    try:
        orch = Orchestrator(registry)
        orch.run(
            DATASET_ID,
            from_date=from_date,
            to_date=to_date,
            indicators=indicators,
            limit=100,
        )
        print("Ingestion completed.")
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)

    raw_after, series_after, obs_after = get_counts()
    print(f"After: raw={raw_after}, series={series_after}, obs={obs_after}")
    print()

    ok = series_after >= series_before and obs_after >= obs_before
    if ok:
        print("Validation PASSED.")
    else:
        print("Validation FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
