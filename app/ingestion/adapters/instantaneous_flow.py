"""
Instantaneous Flow adapter. Logic from national_gas_client + transformer + series_autoregister.
No DB, no retries, no lifecycle.
"""

from typing import Any, List

import pandas as pd
import requests

from app.ingestion.core.base_adapter import BaseAdapter
from app.ingestion.transformer import clean_json_payload
from app.utils.logger import logger


INSTANTANEOUS_FLOW_URL = (
    "https://api.nationalgas.com/operationaldata/v1/instantaneousflow/sites"
)
DATASET_ID = "INSTANTANEOUS_FLOW"
PREFIX = "NG_INSTANTANEOUS_FLOW_"


def _make_series_id(dataset_id: str, *parts: Any) -> str:
    slug = "_".join(
        str(p).upper().replace(",", "").replace("(", "").replace(")", "").replace(" ", "_")
        for p in parts if p is not None and p != ""
    )
    return f"NG_{dataset_id}_{slug}"


class InstantaneousFlowAdapter(BaseAdapter):
    """Adapter for INSTANTANEOUS_FLOW. No DB, no retries."""

    def fetch(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        site_names: list[str] | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        logger.info("Fetching INSTANTANEOUS_FLOW from %s", INSTANTANEOUS_FLOW_URL)
        response = requests.get(INSTANTANEOUS_FLOW_URL, timeout=60)
        response.raise_for_status()
        data = response.json()
        rows = []
        for block in data.get("instantaneousFlow", []):
            for site in block.get("sites", []):
                site_name = site.get("siteName")
                for detail in site.get("siteGasDetail", []):
                    rows.append({
                        "siteName": site_name,
                        "applicableAt": detail.get("applicableAt"),
                        "flowRate": detail.get("flowRate"),
                        "qualityIndicator": detail.get("qualityIndicator"),
                        "scheduleTime": detail.get("scheduleTime"),
                    })
        return pd.DataFrame(rows)

    def parse(self, raw: Any) -> List[Any]:
        if not isinstance(raw, pd.DataFrame):
            raise TypeError("INSTANTANEOUS_FLOW adapter expects DataFrame from fetch")
        if raw.empty:
            return []
        return [row.to_dict() for _, row in raw.iterrows()]

    def normalize(self, record: Any) -> List[dict]:
        if not isinstance(record, dict):
            return []
        site_name = record.get("siteName")
        applicable_at = record.get("applicableAt")
        flow_rate = record.get("flowRate")
        if site_name is None or applicable_at is None or flow_rate is None:
            return []
        series_id = _make_series_id(DATASET_ID, site_name, "FLOWRATE")
        return [{
            "series_id": series_id,
            "observation_time": pd.to_datetime(applicable_at, utc=True),
            "value": float(flow_rate),
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
            if not sid.startswith(PREFIX) or not sid.endswith("_FLOWRATE"):
                continue
            site = sid[len(PREFIX):].rsplit("_FLOWRATE", 1)[0]
            out.append({
                "series_id": sid,
                "source": "NATIONAL_GAS",
                "dataset_id": DATASET_ID,
                "data_item": "flowRate",
                "description": f"Instantaneous Flow at {site}",
                "unit": "UNKNOWN",
                "frequency": "intraday",
                "timezone_source": "Europe/London",
                "is_active": True,
            })
        return out

    def get_time_field(self) -> str:
        return "observation_time"
