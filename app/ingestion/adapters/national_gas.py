"""
National Gas adapter — GAS_QUALITY dataset only.
Logic copied from national_gas_client + transformer + series_autoregister; no changes.
"""

import time
from datetime import datetime, timedelta
from typing import Any, List

import pandas as pd
import requests

from app.ingestion.core.base_adapter import BaseAdapter
from app.ingestion.transformer import clean_json_payload
from app.utils.logger import logger


# Endpoint (same as national_gas_client)
GAS_QUALITY_HISTORIC = (
    "https://api.nationalgas.com/operationaldata/v1/gasquality/historicdata"
)


def _make_series_id(dataset_id: str, *parts: Any) -> str:
    """Same as series_autoregister.make_series_id."""
    slug = "_".join(
        str(p).upper()
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
        .replace(" ", "_")
        for p in parts if p is not None and p != ""
    )
    return f"NG_{dataset_id}_{slug}"


# Numeric metric columns only (exclude identifiers and time)
_GAS_QUALITY_KEY_COLUMNS = {"siteId", "areaName", "siteName", "publishedTime"}


class NationalGasAdapter(BaseAdapter):
    """Adapter for GAS_QUALITY. No DB, no retries, no lifecycle."""

    DATASET_ID = "GAS_QUALITY"

    def fetch(
        self,
        from_date: str,
        to_date: str,
        site_ids: List[int] | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Fetch GAS_QUALITY from National Gas API. No retry (orchestrator does that)."""
        start = datetime.fromisoformat(from_date)
        end = datetime.fromisoformat(to_date)
        all_rows: List[dict] = []

        session = requests.Session()
        for frm, to in self._daterange_chunks(start, end, days=2):
            payload = {
                "fromDate": frm.date().isoformat(),
                "toDate": to.date().isoformat(),
            }
            if site_ids:
                payload["siteIds"] = site_ids

            logger.info("Fetching GAS_QUALITY chunk: %s", payload)

            response = session.post(
                GAS_QUALITY_HISTORIC,
                json=payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=60,
            )

            if response.status_code == 429:
                logger.warning("Rate limited. Sleeping 15 seconds...")
                time.sleep(15)
                response = session.post(
                    GAS_QUALITY_HISTORIC, json=payload, timeout=60
                )

            response.raise_for_status()
            data = response.json()

            for site in data:
                base = {
                    "siteId": site.get("siteId"),
                    "areaName": site.get("areaName"),
                    "siteName": site.get("siteName"),
                }
                for point in site.get("siteGasQualityDetail", []):
                    row = {**base, **point}
                    all_rows.append(row)

            time.sleep(1.5)

        return pd.DataFrame(all_rows)

    def _daterange_chunks(
        self, start: datetime, end: datetime, days: int = 2
    ):
        cur = start
        while cur < end:
            nxt = min(cur + timedelta(days=days), end)
            yield cur, nxt
            cur = nxt

    def parse(self, raw: Any) -> List[Any]:
        """DataFrame -> list of row dicts."""
        if not isinstance(raw, pd.DataFrame):
            raise TypeError("GAS_QUALITY adapter expects DataFrame from fetch")
        if raw.empty:
            return []
        return [row.to_dict() for _, row in raw.iterrows()]

    def normalize(self, record: Any) -> List[dict]:
        """One raw row -> list of observation records (one per metric). Same logic as transform_gas_quality_rest."""
        if not isinstance(record, dict):
            return []

        metric_cols = [
            k
            for k in record
            if k not in _GAS_QUALITY_KEY_COLUMNS
            and isinstance(record.get(k), (int, float))
            and not (isinstance(record.get(k), float) and pd.isna(record.get(k)))
        ]

        site_id = record.get("siteId")
        ts = record.get("publishedTime")
        if site_id is None or ts is None:
            return []

        result = []
        for col in metric_cols:
            value = record.get(col)
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            series_id = _make_series_id(self.DATASET_ID, int(site_id), col.upper())
            result.append({
                "series_id": series_id,
                "observation_time": pd.to_datetime(ts, utc=True),
                "value": float(value),
                "quality_flag": None,
                "raw_payload": clean_json_payload(record),
            })
        return result

    def define_series(self, normalized_records: List[Any]) -> List[dict]:
        """From normalized observations, return list of series meta dicts (same shape as MetaSeries)."""
        seen: set[str] = set()
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
            site_id = parts[-2]
            data_item = parts[-1]
            out.append({
                "series_id": sid,
                "source": "NATIONAL_GAS",
                "dataset_id": self.DATASET_ID,
                "data_item": data_item,
                "description": f"{data_item} at site {site_id}",
                "unit": "UNKNOWN",
                "frequency": "intraday",
                "timezone_source": "UTC",
                "is_active": True,
            })
        return out

    def get_time_field(self) -> str:
        return "observation_time"
