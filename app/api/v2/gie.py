from fastapi import APIRouter
from app.ingestion.gie.service import ingest_gie
from app.ingestion.gie.constants import DATASET_AGSI, DATASET_ALSI, SOURCE_AGSI, SOURCE_ALSI


router = APIRouter(prefix="/v2/gie", tags=["GIE"])

@router.post("/agsi")
def ingest_agsi(country: str | None = None):
    ingest_gie(DATASET_AGSI, SOURCE_AGSI, country)
    return {"status": "completed", "dataset": "AGSI", "country": country}


@router.post("/alsi")
def ingest_alsi(country: str | None = None):
    ingest_gie(DATASET_ALSI, SOURCE_ALSI, country)
    return {"status": "completed", "dataset": "ALSI", "country": country}
