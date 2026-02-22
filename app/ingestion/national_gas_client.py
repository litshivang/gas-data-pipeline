import time
import requests
import pandas as pd
from app.utils.logger import logger
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DATASET_ENDPOINTS = {
    "GAS_QUALITY_LATEST": "https://api.nationalgas.com/operationaldata/v1/gasquality/latestdata",
    "GAS_QUALITY_HISTORIC": "https://api.nationalgas.com/operationaldata/v1/gasquality/historicdata",
    "ENTSOG": "https://transparency.entsog.eu/api/v1/operationaldatas",
    "INSTANTANEOUS_FLOW": "https://api.nationalgas.com/operationaldata/v1/instantaneousflow/sites"
}


class NationalGasClient:
    
    def _build_session(self):
        retry = Retry(
            total=5,
            backoff_factor=2,               # exponential backoff
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )

        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        return session
    
    def fetch_last_days(self, dataset_id: str, last_days: int) -> pd.DataFrame:
        if dataset_id not in DATASET_ENDPOINTS:
            raise ValueError(f"Unknown dataset_id: {dataset_id}")

        url = DATASET_ENDPOINTS[dataset_id]
        logger.info(f"Fetching dataset={dataset_id} via {url}")

        if dataset_id == "GAS_QUALITY":
            if last_days != 1:
                logger.warning(
                    "GAS_QUALITY API only supports 'latest' snapshot. "
                    "lookback_days is ignored. Historical data is not available "
                    "from this endpoint."
                )
            return self._fetch_gas_quality(url)

        if dataset_id == "ENTSOG":
            return self._fetch_entsog(url, last_days)

        raise ValueError(f"No handler for dataset_id={dataset_id}")


    # -------------------- NATIONAL GAS --------------------


    def _daterange_chunks(self, start, end, days=2):
        cur = start
        while cur < end:
            nxt = min(cur + timedelta(days=days), end)
            yield cur, nxt
            cur = nxt


    def fetch_gas_quality(self, from_date=None, to_date=None, site_ids=None) -> pd.DataFrame:

        url = DATASET_ENDPOINTS["GAS_QUALITY_HISTORIC"]

        start = datetime.fromisoformat(from_date)
        end = datetime.fromisoformat(to_date)

        session = self._build_session()
        all_rows = []

        for frm, to in self._daterange_chunks(start, end, days=2):

            payload = {
                "fromDate": frm.date().isoformat(),
                "toDate": to.date().isoformat(),
            }
            if site_ids:
                payload["siteIds"] = site_ids

            logger.info(f"Fetching GAS_QUALITY chunk: {payload}")

            response = session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=60,
            )

            # 🔥 HARD STOP if still blocked
            if response.status_code == 429:
                logger.warning("Rate limited. Sleeping 15 seconds...")
                time.sleep(15)
                response = session.post(url, json=payload, timeout=60)

            response.raise_for_status()
            data = response.json()

            for site in data:
                base = {
                    "siteId": site.get("siteId"),
                    "areaName": site.get("areaName"),
                    "siteName": site.get("siteName"),
                }

                for point in site.get("siteGasQualityDetail", []):
                    row = base.copy()
                    row.update(point)
                    all_rows.append(row)

            time.sleep(1.5)   # 🔒 throttle between chunks

        return pd.DataFrame(all_rows)

    

    # -------------------- ENTSOG --------------------
    def fetch_entsog(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        operator_keys: list[str] | None = None,
        point_keys: list[str] | None = None,
        direction_keys: list[str] | None = None,
        indicators: list[str] | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:

        url = DATASET_ENDPOINTS["ENTSOG"]

        # 🔥 HARD VALIDATION — prevent ENTSOG 500s
        if not indicators and not (point_keys and direction_keys):
            raise ValueError(
                "ENTSOG requires at least one of:\n"
                "1) indicator\n"
                "2) pointKey + directionKey\n"
                "operatorKey alone is NOT sufficient."
            )

        params = {
            "periodType": "day",
        }

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
            # 🔥 Normalize: "Physical Flow" → "PhysicalFlow"
            indicators = [i.replace(" ", "") for i in indicators]
            params["indicator"] = ",".join(indicators)
        if limit:
            params["limit"] = limit

        logger.info(f"Fetching ENTSOG with params: {params}")

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        # ENTSOG wraps records
        if isinstance(data, dict):
            if "operationaldatas" not in data:
                raise ValueError(f"Invalid ENTSOG response keys: {data.keys()}")
            records = data["operationaldatas"]
        elif isinstance(data, list):
            records = data
        else:
            raise ValueError(f"Unexpected ENTSOG response type: {type(data)}")

        if not records:
            logger.warning("ENTSOG API returned empty dataset.")
            return pd.DataFrame()

        return pd.json_normalize(records)
    
    
    # -------------------- Instantaneous --------------------
    def fetch_instantaneous_flow(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        site_names: list[str] | None = None,
    ) -> pd.DataFrame:

        url = DATASET_ENDPOINTS["INSTANTANEOUS_FLOW"]

        session = self._build_session()
        response = session.get(url, timeout=60)
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
