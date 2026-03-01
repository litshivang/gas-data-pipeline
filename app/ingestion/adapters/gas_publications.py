"""
Gas Publications adapter. Logic from national_gas_client + transformer + series_autoregister.
No DB, no retries, no lifecycle.
"""

from typing import Any, List

import pandas as pd
import requests

from app.ingestion.core.base_adapter import BaseAdapter
from app.ingestion.transformer import clean_json_payload
from app.utils.logger import logger


GAS_PUBLICATIONS_URL = (
    "https://api.nationalgas.com/operationaldata/v1/publications/gasday"
)
DATASET_ID = "GAS_PUBLICATIONS"


def _make_series_id(dataset_id: str, *parts: Any) -> str:
    slug = "_".join(
        str(p).upper().replace(",", "").replace("(", "").replace(")", "").replace(" ", "_")
        for p in parts if p is not None and p != ""
    )
    return f"NG_{dataset_id}_{slug}"


class GasPublicationsAdapter(BaseAdapter):
    """Adapter for GAS_PUBLICATIONS. No DB, no retries."""

    def fetch(
        self,
        from_date: str,
        to_date: str,
        publication_ids: list[str],
        **kwargs: Any,
    ) -> pd.DataFrame:
        payload = {
            "fromDate": from_date,
            "toDate": to_date,
            "publicationIds": publication_ids,
            "latestValue": "Y",
        }
        logger.info("Fetching GAS_PUBLICATIONS: %s", payload)
        response = requests.post(GAS_PUBLICATIONS_URL, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()
        rows = []
        for pub in data:
            pub_id = pub.get("publicationId")
            pub_name = pub.get("publicationName")
            for entry in pub.get("publications", []):
                rows.append({
                    "publicationId": pub_id,
                    "publicationName": pub_name,
                    "applicableFor": entry.get("applicableFor"),
                    "value": entry.get("value"),
                    "qualityIndicator": entry.get("qualityIndicator"),
                    "generatedTimeStamp": entry.get("generatedTimeStamp"),
                })
        return pd.DataFrame(rows)

    def parse(self, raw: Any) -> List[Any]:
        if not isinstance(raw, pd.DataFrame):
            raise TypeError("GAS_PUBLICATIONS adapter expects DataFrame from fetch")
        if raw.empty:
            return []
        return [row.to_dict() for _, row in raw.iterrows()]

    def normalize(self, record: Any) -> List[dict]:
        if not isinstance(record, dict):
            return []
        pub_id = record.get("publicationId")
        applicable_for = record.get("applicableFor")
        value = record.get("value")
        if value in (None, "", " "):
            return []
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            return []
        if pub_id is None or applicable_for is None:
            return []
        series_id = _make_series_id(DATASET_ID, pub_id)
        return [{
            "series_id": series_id,
            "observation_time": pd.to_datetime(applicable_for, utc=True),
            "value": numeric_value,
            "quality_flag": record.get("qualityIndicator"),
            "raw_payload": clean_json_payload(record),
        }]

    def define_series(self, normalized_records: List[Any]) -> List[dict]:
        seen = set()
        out = []
        for r in normalized_records:
            if not isinstance(r, dict):
                continue
            sid = r.get("series_id")
            if not sid or sid in seen:
                continue
            seen.add(sid)
            parts = sid.split("_")
            if len(parts) < 3:
                continue
            pub_id = parts[-1]
            out.append({
                "series_id": sid,
                "source": "NATIONAL_GAS",
                "dataset_id": DATASET_ID,
                "data_item": pub_id,
                "description": f"Publication {pub_id}",
                "unit": "UNKNOWN",
                "frequency": "daily",
                "timezone_source": "UTC",
                "is_active": True,
            })
        return out

    def get_time_field(self) -> str:
        return "observation_time"
