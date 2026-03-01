"""
Validation script for INSTANTANEOUS_FLOW API migration (orchestrator + adapter).

Run from project root:
    python -m scripts.validate_api.validate_instantaneous_flow
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sqlalchemy import text
from app.db.connection import engine
from app.ingestion.core import registry, Orchestrator
import app.ingestion.adapters  # noqa: F401

DATASET_ID = "INSTANTANEOUS_FLOW"


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
    print("=" * 60)
    print("INSTANTANEOUS_FLOW migration validation")
    print("=" * 60)

    raw_before, series_before, obs_before = get_counts()
    print(f"Before: raw={raw_before}, series={series_before}, obs={obs_before}")
    print()

    try:
        orch = Orchestrator(registry)
        orch.run("INSTANTANEOUS_FLOW")
        print("Ingestion completed.")
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)

    raw_after, series_after, obs_after = get_counts()
    print(f"After: raw={raw_after}, series={series_after}, obs={obs_after}")
    print()

    if raw_after >= raw_before and series_after >= series_before and obs_after >= obs_before:
        print("Validation PASSED.")
    else:
        print("Validation FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
