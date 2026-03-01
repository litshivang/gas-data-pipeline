"""
ENTSOG adapter. Logic from national_gas_client.fetch_entsog + transformer + series_autoregister.
No DB, no retries, no lifecycle.
"""

from typing import Any, List

import pandas as pd
import requests

from app.ingestion.core.base_adapter import BaseAdapter
from app.ingestion.transformer import clean_json_payload
from app.utils.logger import logger


ENTSOG_URL = "https://transparency.entsog.eu/api/v1/operationaldatas"
DATASET_ID = "ENTSOG"
_REQUIRED_KEYS = {"indicator", "pointKey", "directionKey"}


def _make_series_id(dataset_id: str, *parts: Any) -> str:
    slug = "_".join(
        str(p).upper()
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
        .replace(" ", "_")
        for p in parts if p is not None and p != ""
    )
    return f"NG_{dataset_id}_{slug}"


class EntsogAdapter(BaseAdapter):
    """Adapter for ENTSOG. No DB, no retries, no lifecycle."""

    def fetch(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        operator_keys: list[str] | None = None,
        point_keys: list[str] | None = None,
        direction_keys: list[str] | None = None,
        indicators: list[str] | None = None,
        limit: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        if not indicators and not (point_keys and direction_keys):
            raise ValueError(
                "ENTSOG requires at least one of:\n"
                "1) indicator\n"
                "2) pointKey + directionKey"
            )

        params = {"periodType": "day"}
        if from_date:
            params["periodFrom"] = from_date
        if to_date:
            params["periodTo"] = to_date
        if operator_keys:
            params["operatorKey"] = ",".join(operator_keys)
        if point_keys:
            params["pointKey"] = ",".join(point_keys)
        if direction_keys:
            params["directionKey"] = ",".join(direction_keys)
        if indicators:
            indicators = [i.replace(" ", "") for i in indicators]
            params["indicator"] = ",".join(indicators)
        if limit:
            params["limit"] = limit

        logger.info("Fetching ENTSOG with params: %s", params)
        response = requests.get(ENTSOG_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict):
            if "operationaldatas" not in data:
                raise ValueError(f"Invalid ENTSOG response keys: {list(data.keys())}")
            records = data["operationaldatas"]
        elif isinstance(data, list):
            records = data
        else:
            raise ValueError(f"Unexpected ENTSOG response type: {type(data)}")

        if not records:
            return pd.DataFrame()
        return pd.json_normalize(records)

    def parse(self, raw: Any) -> List[Any]:
        if not isinstance(raw, pd.DataFrame):
            raise TypeError("ENTSOG adapter expects DataFrame from fetch")
        if raw.empty:
            return []
        return [row.to_dict() for _, row in raw.iterrows()]

    def normalize(self, record: Any) -> List[dict]:
        if not isinstance(record, dict):
            return []
        if not _REQUIRED_KEYS.issubset(record):
            return []

        indicator = record.get("indicator")
        point = record.get("pointKey")
        direction = record.get("directionKey")
        value = record.get("value")
        ts = record.get("periodFrom")

        if value in (None, "", " ") or ts is None:
            return []

        try:
            value = float(value)
        except ValueError:
            return []

        series_id = _make_series_id(DATASET_ID, indicator, point, direction)
        return [{
            "series_id": series_id,
            "observation_time": pd.to_datetime(ts, utc=True),
            "value": value,
            "quality_flag": record.get("flowStatus"),
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
            if len(parts) < 5:
                continue
            indicator = " ".join(parts[2:-2]).replace("_", " ")
            point = parts[-2]
            direction = parts[-1]
            out.append({
                "series_id": sid,
                "source": "ENTSOG",
                "dataset_id": DATASET_ID,
                "data_item": indicator,
                "description": f"{indicator} at {point} ({direction})",
                "unit": "UNKNOWN",
                "frequency": "daily",
                "timezone_source": "Europe/Brussels",
                "is_active": True,
            })
        return out

    def get_time_field(self) -> str:
        return "observation_time"
