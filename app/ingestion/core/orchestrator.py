"""
Strict ingestion lifecycle. Order must never change.
Adapters cannot alter it.
"""

import time
import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from app.db.connection import engine
from app.db.models import MetaSeries
from app.ingestion.common.delete_policy import apply as apply_delete_policy
from app.ingestion.core.base_adapter import BaseAdapter
from app.ingestion.core.registry import AdapterRegistry
from app.ingestion.core.validation import ValidationError, validate as run_validation
from app.ingestion.field_discovery import discover_fields
from app.ingestion.gie.service import delete_gie_by_source, insert_gie_rows
from app.ingestion.loader import upsert_observations
from app.ingestion.raw_ingestor import ingest_raw_df, ingest_raw_json
from app.utils.logger import logger

GIE_SOURCE = {"AGSI": "GIE_AGSI", "ALSI": "GIE_ALSI"}


# Centralized retry for fetch (orchestrator-owned, not adapter).
def _fetch_with_retry(adapter: BaseAdapter, max_attempts: int = 3, **kwargs: Any) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            return adapter.fetch(**kwargs)
        except Exception as e:
            last_error = e
            logger.warning(
                "Fetch retry %s/%s for %s: %s",
                attempt + 1,
                max_attempts,
                type(adapter).__name__,
                e,
            )
            if attempt < max_attempts - 1:
                backoff = 2**attempt  # 1, 2, 4 seconds
                time.sleep(backoff)
            else:
                raise
    raise last_error  # type: ignore[misc]


class Orchestrator:
    """
    Enforces the 12-step ingestion lifecycle.
    Adapters only provide fetch/parse/normalize/define_series/get_time_field.
    """

    def __init__(self, registry: AdapterRegistry) -> None:
        self._registry = registry

    def run(self, dataset_id: str, **kwargs: Any) -> None:
        adapter_cls = self._registry.get(dataset_id)
        if not adapter_cls:
            raise ValueError(f"No adapter registered for dataset_id={dataset_id!r}")

        adapter = adapter_cls()
        run_id: Optional[str] = None

        rows_fetched = 0
        rows_inserted = 0
        rows_deleted = 0

        try:
            # --- ORDER MUST NEVER CHANGE ---

            # 1. Create ingestion_run
            run_id = self._create_ingestion_run(dataset_id, kwargs)

            # 2. Load dataset config
            config = self._load_dataset_config(dataset_id)

            # 3. Fetch (with centralized retry)
            raw = _fetch_with_retry(adapter, **kwargs)

            # 4. Store raw payload
            self._store_raw_payload(dataset_id, raw, config, run_id)

            # 5. Parse (contract: returns list)
            records = adapter.parse(raw)
            rows_fetched = len(records) if records else 0

            # 6. Normalize
            normalized = self._normalize_all(adapter, records)
            rows_inserted = len(normalized) if normalized else 0

            # 7. Validate (if fails → mark run failed)
            run_validation(normalized, adapter, config)

            # 8. Apply delete policy (if configured)
            rows_deleted = self._apply_delete_policy(
                dataset_id, adapter, config, run_id
            )

            # 9. Register canonical series
            series_meta = self._register_canonical_series(
                adapter, dataset_id, normalized
            )

            # 10. Bulk insert observations
            self._bulk_insert_observations(
                dataset_id, normalized, config, run_id, series_meta
            )

            # 11. Finalize ingestion_run
            self._finalize_ingestion_run(
                dataset_id,
                run_id,
                success=True,
                rows_fetched=rows_fetched,
                rows_inserted=rows_inserted,
                rows_deleted=rows_deleted,
            )

            # 12. Emit metrics
            self._emit_metrics(dataset_id, run_id)

        except ValidationError as e:
            if run_id is not None:
                self._finalize_ingestion_run(
                    dataset_id,
                    run_id,
                    success=False,
                    error=str(e),
                    rows_fetched=rows_fetched,
                    rows_inserted=rows_inserted,
                    rows_deleted=rows_deleted,
                )
            raise
        except Exception as e:
            if run_id is not None:
                self._finalize_ingestion_run(
                    dataset_id, run_id, success=False, error=str(e)
                )
            raise

    def _create_ingestion_run(self, dataset_id: str, kwargs: dict) -> str:
        """1. Create ingestion_run (tracking). Returns run_id."""
        run_id = uuid.uuid4()
        started = datetime.now(timezone.utc)
        sql = text("""
            INSERT INTO ingestion_runs (run_id, dataset_id, started_at, status, created_at)
            VALUES (:run_id, :dataset_id, :started_at, 'RUNNING', :created_at)
        """)
        with engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "started_at": started,
                    "created_at": started,
                },
            )
        return str(run_id)

    def _load_dataset_config(self, dataset_id: str) -> dict:
        """2. Load dataset config. Stub until configs wired."""
        return {}

    def _store_raw_payload(
        self, dataset_id: str, raw: Any, config: dict, run_id: Optional[str]
    ) -> None:
        """4. Store raw payload. Uses existing raw_ingestor + field_discovery."""
        if isinstance(raw, pd.DataFrame) and not raw.empty:
            ingest_raw_df(raw, dataset_id, run_id=run_id)
            discover_fields(dataset_id)
        elif isinstance(raw, dict) and dataset_id in GIE_SOURCE:
            ingest_raw_json(
                raw, dataset_id, source=GIE_SOURCE[dataset_id], run_id=run_id
            )

    def _normalize_all(self, adapter: BaseAdapter, records: List[Any]) -> List[Any]:
        """6. Normalize all records via adapter. Flatten when normalize returns a list."""
        normalized = []
        for r in records:
            n = adapter.normalize(r)
            if isinstance(n, list):
                normalized.extend(n)
            else:
                normalized.append(n)
        return normalized

    def _apply_delete_policy(
        self,
        dataset_id: str,
        adapter: BaseAdapter,
        config: dict,
        run_id: Optional[str],
    ) -> int:
        """8. Apply delete policy. GIE: delete by source; else config-driven."""
        if dataset_id in GIE_SOURCE:
            return delete_gie_by_source(GIE_SOURCE[dataset_id])
        time_field = adapter.get_time_field()
        return apply_delete_policy(dataset_id, time_field, engine, config)

    def _register_canonical_series(
        self, adapter: BaseAdapter, dataset_id: str, normalized: List[Any]
    ) -> List[dict]:
        """9. Register canonical series. GIE uses meta.series on insert; else MetaSeries."""
        series_meta = adapter.define_series(normalized)
        if dataset_id in GIE_SOURCE:
            return series_meta
        for record in series_meta:
            if not isinstance(record, dict) or "series_id" not in record:
                continue
            stmt = insert(MetaSeries).values(record)
            stmt = stmt.on_conflict_do_nothing(index_elements=["series_id"])
            with engine.begin() as conn:
                conn.execute(stmt)
        return series_meta

    def _bulk_insert_observations(
        self,
        dataset_id: str,
        normalized: List[Any],
        config: dict,
        run_id: Optional[str],
        series_meta: List[dict],
    ) -> None:
        """10. Bulk insert. GIE: insert_gie_rows; else upsert_observations."""
        if not normalized:
            return
        if dataset_id in GIE_SOURCE:
            insert_gie_rows(GIE_SOURCE[dataset_id], normalized)
        else:
            upsert_observations(normalized, run_id=run_id)

    def _finalize_ingestion_run(
        self,
        dataset_id: str,
        run_id: Optional[str],
        success: bool = True,
        error: Optional[str] = None,
        rows_fetched: int = 0,
        rows_inserted: int = 0,
        rows_deleted: int = 0,
    ) -> None:
        """11. Finalize ingestion_run: set finished_at, status, counts, error_message."""
        if not run_id:
            return
        status = "SUCCESS" if success else "FAILED"
        sql = text("""
            UPDATE ingestion_runs
            SET finished_at = :finished_at, status = :status,
                rows_fetched = :rows_fetched, rows_inserted = :rows_inserted,
                rows_deleted = :rows_deleted, error_message = :error_message
            WHERE run_id = :run_id
        """)
        with engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "run_id": run_id,
                    "finished_at": datetime.now(timezone.utc),
                    "status": status,
                    "rows_fetched": rows_fetched,
                    "rows_inserted": rows_inserted,
                    "rows_deleted": rows_deleted,
                    "error_message": error,
                },
            )

    def _emit_metrics(self, dataset_id: str, run_id: Optional[str]) -> None:
        """12. Emit metrics. Stub until metrics wired."""
        pass
