import requests
import pandas as pd
from app.utils.logger import logger

DATASET_ENDPOINTS = {
    "GAS_QUALITY": "https://api.nationalgas.com/operationaldata/v1/gasquality/latestdata",
    "ENTSOG": "https://transparency.entsog.eu/api/v1/operationaldatas"
}


class NationalGasClient:
    def fetch_last_days(self, dataset_id: str, last_days: int) -> pd.DataFrame:
        if dataset_id not in DATASET_ENDPOINTS:
            raise ValueError(f"Unknown dataset_id: {dataset_id}")

        url = DATASET_ENDPOINTS[dataset_id]
        logger.info(f"Fetching dataset={dataset_id} via {url}")

        if dataset_id == "GAS_QUALITY":
            return self._fetch_gas_quality(url)

        if dataset_id == "ENTSOG":
            return self._fetch_entsog(url, last_days)

        raise ValueError(f"No handler for dataset_id={dataset_id}")

    # -------------------- NATIONAL GAS --------------------
    def _fetch_gas_quality(self, url: str) -> pd.DataFrame:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "gasQualityData" not in data:
            raise ValueError(f"Invalid GAS_QUALITY response keys: {data.keys()}")

        return pd.json_normalize(data["gasQualityData"])

    # -------------------- ENTSOG --------------------
    def _fetch_entsog(self, url: str, last_days: int) -> pd.DataFrame:
        params = {
            "periodType": "day",
            "limit": 100
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "operationaldatas" not in data:
            raise ValueError(f"Invalid ENTSOG response keys: {data.keys()}")

        return pd.json_normalize(data["operationaldatas"])
