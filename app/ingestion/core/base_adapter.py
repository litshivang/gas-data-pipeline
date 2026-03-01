"""
Base adapter contract for ingestion plugins.

Adapters MUST NOT: write to DB, delete records, retry requests,
log ingestion metrics, or control lifecycle.

Adapters ONLY: fetch, parse, normalize, define_series, get_time_field.
"""

from abc import ABC, abstractmethod
from typing import Any, List


class BaseAdapter(ABC):
    """Contract for all dataset ingestion adapters."""

    @abstractmethod
    def fetch(self, **kwargs) -> Any:
        """Fetch raw data from source. No DB, no retries, no lifecycle."""
        pass

    @abstractmethod
    def parse(self, raw: Any) -> List[Any]:
        """Parse raw response into a list of records."""
        pass

    @abstractmethod
    def normalize(self, record: Any) -> Any:
        """Normalize a single record to internal shape."""
        pass

    @abstractmethod
    def define_series(self, normalized_records: List[Any]) -> List[dict]:
        """Describe series metadata from normalized records. Returns list of series meta dicts."""
        pass

    @abstractmethod
    def get_time_field(self) -> str:
        """Return name of datetime field for delete policy (e.g. observation_time, value_date)."""
        pass

    def get_validation_config(self) -> dict:
        """
        Optional. Return validation rules for core to run: required_fields, min_row_count, date_range.
        Example: {"required_fields": ["series_id", "observation_time", "value"], "min_row_count": 0}.
        """
        return {}
