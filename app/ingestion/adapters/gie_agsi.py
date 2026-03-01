"""
GIE AGSI adapter. Fetch/parse/normalize from GIE client + transformer.
No DB, no retries. Storage handled by orchestrator (energy.daily, meta.series).
"""

from typing import Any, List

from app.config.settings import settings
from app.ingestion.core.base_adapter import BaseAdapter
from app.ingestion.gie.transformer import transform
from app.utils.logger import logger

import requests


DATASET_ID = "AGSI"
SOURCE = "GIE_AGSI"
BASE_URL = "https://agsi.gie.eu/api"


class GieAgsiAdapter(BaseAdapter):
    """Adapter for AGSI. No DB writes."""

    def fetch(self, country: str | None = None, **kwargs: Any) -> dict:
        params = {}
        if country:
            params["country"] = country
        logger.info("Fetching AGSI params=%s", params)
        response = requests.get(
            BASE_URL,
            headers={"x-key": settings.GIE_API_KEY},
            params=params,
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def parse(self, raw: Any) -> List[Any]:
        if not isinstance(raw, dict):
            raise TypeError("AGSI adapter expects dict from fetch")
        return transform(DATASET_ID, raw)

    def normalize(self, record: Any) -> List[dict]:
        """Each parsed row is one record; return as list of one."""
        if not isinstance(record, dict):
            return []
        if "country" not in record or "date" not in record or "variable" not in record:
            return []
        return [record]

    def define_series(self, normalized_records: List[Any]) -> List[dict]:
        """GIE series are created during insert (meta.series). Return empty."""
        return []

    def get_time_field(self) -> str:
        return "date"
