"""
Validation layer. Executed in core.
Adapter defines rules (required fields, min row count, date range); core runs checks.
On failure → mark ingestion_run failed (caller raises).
"""

from typing import Any, List

from app.ingestion.core.base_adapter import BaseAdapter
from app.utils.logger import logger


class ValidationError(Exception):
    """Raised when validation fails. Orchestrator marks run failed."""

    pass


def get_validation_config(adapter: BaseAdapter) -> dict:
    """Return adapter's validation config or empty dict. Optional method on adapter."""
    if hasattr(adapter, "get_validation_config") and callable(
        getattr(adapter, "get_validation_config")
    ):
        return adapter.get_validation_config() or {}
    return {}


def validate(
    normalized: List[Any],
    adapter: BaseAdapter,
    config: dict,
) -> None:
    """
    Run validation rules from adapter. Raises ValidationError if any check fails.
    Config from adapter.get_validation_config(): required_fields, min_row_count, date_range.
    """
    rules = get_validation_config(adapter)
    if not rules:
        return

    # Min row count
    min_count = rules.get("min_row_count")
    if min_count is not None:
        try:
            min_count = int(min_count)
        except (TypeError, ValueError):
            min_count = None
        if min_count is not None and len(normalized) < min_count:
            raise ValidationError(
                f"min_row_count={min_count} but got {len(normalized)} normalized records"
            )

    # Required fields (each normalized record must have these keys)
    required_fields = rules.get("required_fields")
    if isinstance(required_fields, list) and required_fields and normalized:
        for i, rec in enumerate(normalized):
            if not isinstance(rec, dict):
                raise ValidationError(
                    f"Normalized record at index {i} is not a dict"
                )
            missing = [f for f in required_fields if f not in rec or rec[f] is None]
            if missing:
                raise ValidationError(
                    f"Record at index {i} missing required fields: {missing}"
                )

    # Date range (optional: min_date, max_date - time field must be in range)
    date_range = rules.get("date_range")
    if isinstance(date_range, dict) and date_range and normalized:
        import pandas as pd
        time_field = adapter.get_time_field()
        min_date = date_range.get("min_date")
        max_date = date_range.get("max_date")
        if min_date or max_date:
            min_ts = pd.Timestamp(min_date) if min_date else None
            max_ts = pd.Timestamp(max_date) if max_date else None
            for i, rec in enumerate(normalized):
                if not isinstance(rec, dict) or time_field not in rec:
                    continue
                ts = rec[time_field]
                if ts is None:
                    continue
                ts = pd.Timestamp(ts)
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                if min_ts is not None and ts < min_ts:
                    raise ValidationError(
                        f"Record at index {i}: {time_field} {ts} before min_date {min_date}"
                    )
                if max_ts is not None and ts > max_ts:
                    raise ValidationError(
                        f"Record at index {i}: {time_field} {ts} after max_date {max_date}"
                    )

    logger.debug("Validation passed for %s records", len(normalized))
