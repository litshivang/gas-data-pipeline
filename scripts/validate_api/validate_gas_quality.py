"""
Validation script for GAS_QUALITY API migration (orchestrator + NationalGasAdapter).

Run from project root:
    python -m scripts.validate_api.validate_gas_quality
  or
    python scripts/validate_api/validate_gas_quality.py

Optional env: FROM_DATE, TO_DATE (YYYY-MM-DD). Default: 7–6 days ago.

Requires: .env with DB settings; optional FROM_DATE, TO_DATE (default: 1-day range).
Validates: ingestion runs, raw_events, meta_series, data_observations.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

# Project root on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sqlalchemy import text

from app.db.connection import engine
from app.ingestion.core import registry, Orchestrator
import app.ingestion.adapters  # noqa: F401 — register GAS_QUALITY


DATASET_ID = "GAS_QUALITY"


def get_counts():
    """Return (raw_count, series_count, obs_count) for GAS_QUALITY."""
    with engine.connect() as conn:
        raw = conn.execute(
            text(
                "SELECT COUNT(*) FROM raw_events WHERE dataset_id = :did"
            ),
            {"did": DATASET_ID},
        ).scalar()
        series = conn.execute(
            text(
                "SELECT COUNT(*) FROM meta_series WHERE dataset_id = :did"
            ),
            {"did": DATASET_ID},
        ).scalar()
        obs = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM data_observations o
                JOIN meta_series s ON s.series_id = o.series_id
                WHERE s.dataset_id = :did
                """
            ),
            {"did": DATASET_ID},
        ).scalar()
    return int(raw), int(series), int(obs)


def run_ingestion(from_date: str, to_date: str, site_ids=None):
    """Run GAS_QUALITY ingestion via orchestrator."""
    orch = Orchestrator(registry)
    orch.run(
        DATASET_ID,
        from_date=from_date,
        to_date=to_date,
        site_ids=site_ids,
    )


def main():
    from_date = os.environ.get(
        "FROM_DATE",
        (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d"),
    )
    to_date = os.environ.get(
        "TO_DATE",
        (datetime.now(timezone.utc) - timedelta(days=6)).strftime("%Y-%m-%d"),
    )

    print("=" * 60)
    print("GAS_QUALITY migration validation")
    print("=" * 60)
    print(f"Date range: {from_date} -> {to_date}")
    print()

    raw_before, series_before, obs_before = get_counts()
    print(f"Before ingestion:")
    print(f"  raw_events (GAS_QUALITY): {raw_before}")
    print(f"  meta_series (GAS_QUALITY): {series_before}")
    print(f"  data_observations (GAS_QUALITY series): {obs_before}")
    print()

    try:
        print("Running ingestion (orchestrator + NationalGasAdapter)...")
        run_ingestion(from_date, to_date)
        print("Ingestion completed.")
    except Exception as e:
        print(f"FAIL: Ingestion raised: {e}")
        sys.exit(1)

    raw_after, series_after, obs_after = get_counts()
    print()
    print(f"After ingestion:")
    print(f"  raw_events (GAS_QUALITY): {raw_after}")
    print(f"  meta_series (GAS_QUALITY): {series_after}")
    print(f"  data_observations (GAS_QUALITY series): {obs_after}")
    print()

    # Validation
    ok = True
    if raw_after <= raw_before and raw_before > 0:
        print("  WARN: raw_events count did not increase (may be no data for range).")
    if series_after < series_before:
        print("  FAIL: meta_series count decreased.")
        ok = False
    if obs_after < obs_before:
        print("  FAIL: data_observations count decreased.")
        ok = False

    # Sanity: we expect some raw rows if API returned data
    with engine.connect() as conn:
        sample_raw = conn.execute(
            text(
                "SELECT raw_payload FROM raw_events WHERE dataset_id = :did LIMIT 1"
            ),
            {"did": DATASET_ID},
        ).fetchone()
        sample_series = conn.execute(
            text(
                "SELECT series_id, dataset_id FROM meta_series WHERE dataset_id = :did LIMIT 1"
            ),
            {"did": DATASET_ID},
        ).fetchone()
        sample_obs = conn.execute(
            text(
                """
                SELECT o.series_id, o.observation_time, o.value
                FROM data_observations o
                JOIN meta_series s ON s.series_id = o.series_id
                WHERE s.dataset_id = :did
                LIMIT 1
                """
            ),
            {"did": DATASET_ID},
        ).fetchone()

    print("Sample checks:")
    print(f"  raw_events has rows: {sample_raw is not None}")
    print(f"  meta_series has GAS_QUALITY: {sample_series is not None}")
    print(f"  data_observations has GAS_QUALITY series: {sample_obs is not None}")

    if not sample_series and series_after > 0:
        print("  WARN: meta_series count > 0 but sample query returned none.")
    if not sample_raw and raw_after > 0:
        print("  WARN: raw_events count > 0 but sample query returned none.")

    print()
    if ok:
        print("Validation PASSED. Safe to proceed to next API migration.")
    else:
        print("Validation FAILED. Fix before next step.")
        sys.exit(1)


if __name__ == "__main__":
    main()
