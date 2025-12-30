import requests
import pandas as pd
from io import StringIO
from datetime import date, timedelta
from app.utils.logger import logger


BASE_URL = "https://data.nationalgas.com/api/find-gas-data-download"


class NationalGasClient:
    def fetch_last_days(self, dataset_id: str, last_days: int) -> pd.DataFrame:
        end_date = date.today()
        start_date = end_date - timedelta(days=last_days)

        params = {
            "applicableFor": "Y",
            "dateFrom": start_date.isoformat(),
            "dateTo": end_date.isoformat(),
            "dateType": "GASDAY",
            "latestFlag": "Y",
            "ids": dataset_id,
            "type": "CSV",
        }

        logger.info(
            f"Fetching National Gas data: {dataset_id} "
            f"({start_date} → {end_date})"
        )

        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()

        return pd.read_csv(StringIO(response.text))
