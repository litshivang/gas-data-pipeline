"""
Single entry point for ingestion: registry-driven orchestrator only.
No dataset conditionals. All datasets go through orchestrator + adapter.
"""

from datetime import datetime, timedelta

from app.utils.logger import logger
from app.ingestion.core import registry, Orchestrator
import app.ingestion.adapters  # noqa: F401 — register all adapters


def run_national_gas() -> None:
    """Scheduled job: run GAS_QUALITY for last 2 days. Used by scheduler."""
    to_d = datetime.utcnow().date()
    from_d = (to_d - timedelta(days=2)).isoformat()
    to_d_str = to_d.isoformat()
    ingest_dataset("GAS_QUALITY", from_date=from_d, to_date=to_d_str)


def ingest_dataset(
    dataset_id: str,
    from_date: str | None = None,
    to_date: str | None = None,
    site_ids: list[int] | None = None,
    operator_keys: list[str] | None = None,
    point_keys: list[str] | None = None,
    direction_keys: list[str] | None = None,
    indicators: list[str] | None = None,
    limit: int | None = None,
    publication_ids: list[str] | None = None,
    country: str | None = None,
):
    """Run ingestion for dataset_id via orchestrator. No adapter → ValueError."""
    logger.info(
        "Ingesting dataset=%s, from=%s, to=%s, sites=%s, operators=%s, points=%s, "
        "directions=%s, indicators=%s, limit=%s, publication_ids=%s, country=%s",
        dataset_id, from_date, to_date, site_ids, operator_keys, point_keys,
        direction_keys, indicators, limit, publication_ids, country,
    )

    if not registry.get(dataset_id):
        raise ValueError(
            f"No adapter registered for dataset_id={dataset_id!r}. "
            f"Available: {registry.list_datasets()}"
        )

    orch = Orchestrator(registry)
    orch.run(
        dataset_id,
        from_date=from_date,
        to_date=to_date,
        site_ids=site_ids,
        operator_keys=operator_keys,
        point_keys=point_keys,
        direction_keys=direction_keys,
        indicators=indicators,
        limit=limit,
        publication_ids=publication_ids,
        country=country,
    )
    logger.info("Completed ingestion for dataset=%s", dataset_id)
