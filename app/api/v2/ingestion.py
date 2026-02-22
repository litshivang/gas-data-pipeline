from fastapi import APIRouter, Query, HTTPException, BackgroundTasks
from app.ingestion.national_gas_client import NationalGasClient
from app.ingestion.run_all import ingest_dataset
from datetime import datetime
from typing import List, Optional
from fastapi.responses import JSONResponse
from app.api.v2.schemas import GasPublicationRequest



router = APIRouter(prefix="/v2/ingest", tags=["Ingestion"])

@router.post("/gas")
def ingest_gas_quality(
    background_tasks: BackgroundTasks,
    from_date: str = Query(..., description="YYYY-MM-DD"),
    to_date: str = Query(..., description="YYYY-MM-DD"),
    site_ids: Optional[List[int]] = Query(
        default=None,
        description="Optional site filter. Omit to ingest all sites.",
    ),
):
    # ---------------- VALIDATION ----------------
    try:
        f = datetime.strptime(from_date, "%Y-%m-%d")
        t = datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    if t < f:
        raise HTTPException(status_code=400, detail="to_date must be >= from_date")

    # ---------------- BACKGROUND INGEST ----------------
    background_tasks.add_task(
        ingest_dataset,
        dataset_id="GAS_QUALITY",
        from_date=from_date,
        to_date=to_date,
        site_ids=site_ids,   # None = all sites
    )

    # ---------------- IMMEDIATE RESPONSE ----------------
    return {
        "status": "accepted",
        "message": "Ingestion started in background",
        "dataset": "GAS_QUALITY",
        "from": from_date,
        "to": to_date,
        "site_ids": site_ids,   # Will be null if not provided
    }


@router.post("/entsog")
def ingest_entsog(
    background_tasks: BackgroundTasks,
    from_date: str = Query(...),
    to_date: str = Query(...),
    operator_keys: list[str] = Query(None),
    point_keys: list[str] = Query(None),
    direction_keys: list[str] = Query(None),
    indicators: list[str] = Query(None),
    limit: int = Query(1000),
):
    background_tasks.add_task(
        ingest_dataset,
        dataset_id="ENTSOG",
        from_date=from_date,
        to_date=to_date,
        operator_keys=operator_keys,
        point_keys=point_keys,
        direction_keys=direction_keys,
        indicators=indicators,
        limit=limit,
    )

    return {
        "status": "accepted",
        "dataset": "ENTSOG",
        "from": from_date,
        "to": to_date,
        "filters": {
            "operator_keys": operator_keys,
            "point_keys": point_keys,
            "direction_keys": direction_keys,
            "indicators": indicators,
        }
    }


@router.post("/instantaneous")
def ingest_instantaneous_flow(background_tasks: BackgroundTasks):

    background_tasks.add_task(
        ingest_dataset,
        dataset_id="INSTANTANEOUS_FLOW"
    )

    return {
        "status": "accepted",
        "dataset": "INSTANTANEOUS_FLOW"
    }


@router.get("/publication-catalogue")
def get_publication_catalogue():
    """
    Returns simplified publication list for Swagger usability.
    """

    client = NationalGasClient()
    data = client.fetch_publication_catalogue()

    publications = []

    for group in data.get("data", []):
        for sub in group.get("subCategory", []):
            for entry in sub.get("catalogueEntries", []):

                pub_id = entry.get("publicationId")
                name = entry.get("name")

                if not pub_id:
                    continue

                publications.append({
                    "publicationId": pub_id,
                    "name": name,
                })

    return publications


@router.post("/gas-publications")
def ingest_gas_publications(
    background_tasks: BackgroundTasks,
    from_date: str = Query(..., example="2024-03-01"),
    to_date: str = Query(..., example="2024-03-05"),
    publication_ids: List[str] = Query(
        ...,
        description="List of publication IDs (e.g., PUBOB28)",
        example=["PUBOB28"]
    ),
):
    background_tasks.add_task(
        ingest_dataset,
        dataset_id="GAS_PUBLICATIONS",
        from_date=from_date,
        to_date=to_date,
        publication_ids=publication_ids,
    )

    return {
        "status": "accepted",
        "dataset": "GAS_PUBLICATIONS"
    }