import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from app.config.settings import settings


class GIEClient:

    BASE_URL_AGSI = "https://agsi.gie.eu/api"
    BASE_URL_ALSI = "https://alsi.gie.eu/api"

    def __init__(self):
        self.session = self._build_session()

    def _build_session(self):
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        return session

    def fetch(self, dataset: str, country: str | None = None):
        if dataset == "AGSI":
            url = self.BASE_URL_AGSI
        elif dataset == "ALSI":
            url = self.BASE_URL_ALSI
        else:
            raise ValueError("Invalid GIE dataset")

        params = {}
        if country:
            params["country"] = country

        response = self.session.get(
            url,
            headers={"x-key": settings.GIE_API_KEY},
            params=params,
            timeout=60,
        )
        response.raise_for_status()
        return response.json()
