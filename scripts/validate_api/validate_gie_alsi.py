"""
Validation script for GIE ALSI API migration (orchestrator + GieAlsiAdapter).

Run from project root:
    python -m scripts.validate_api.validate_gie_alsi
  or
    python scripts/validate_api/validate_gie_alsi.py

Optional env: COUNTRY. Validates: raw_events (ALSI), energy.daily, meta.series.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sqlalchemy import text
from app.db.connection import engine
from app.ingestion.core import registry, Orchestrator
import app.ingestion.adapters  # noqa: F401

DATASET_ID = "ALSI"
SOURCE = "GIE_ALSI"


def get_counts():
    with engine.connect() as conn:
        raw = conn.execute(
            text("SELECT COUNT(*) FROM raw_events WHERE dataset_id = :did"),
            {"did": DATASET_ID},
        ).scalar()
        daily = conn.execute(
            text("""
                SELECT COUNT(*) FROM energy.daily d
                JOIN meta.series s ON d.series_id = s.series_id
                WHERE s.source = :source
            """),
            {"source": SOURCE},
        ).scalar()
    return int(raw), int(daily)


def main():
    country = os.environ.get("COUNTRY")

    print("=" * 60)
    print("GIE ALSI migration validation")
    print("=" * 60)
    print(f"country={country}")
    print()

    raw_before, daily_before = get_counts()
    print(f"Before: raw_events={raw_before}, energy.daily(ALSI)={daily_before}")
    print()

    try:
        orch = Orchestrator(registry)
        orch.run("ALSI", country=country)
        print("Ingestion completed.")
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)

    raw_after, daily_after = get_counts()
    print(f"After: raw_events={raw_after}, energy.daily(ALSI)={daily_after}")
    print()

    if raw_after >= raw_before and daily_after >= daily_before:
        print("Validation PASSED.")
    else:
        print("Validation FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
