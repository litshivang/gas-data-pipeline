from fastapi import APIRouter, Query
from sqlalchemy import text
from app.db.connection import engine
from app.ingestion.core import registry, Orchestrator
import app.ingestion.adapters  # noqa: F401 — register adapters

router = APIRouter(prefix="/v2/gie", tags=["GIE"])
_orchestrator = Orchestrator(registry)


@router.post("/agsi")
def ingest_agsi(country: str | None = None):
    _orchestrator.run("AGSI", country=country)
    return {"status": "completed", "dataset": "AGSI", "country": country}


@router.post("/alsi")
def ingest_alsi(country: str | None = None):
    _orchestrator.run("ALSI", country=country)
    return {"status": "completed", "dataset": "ALSI", "country": country}

@router.get("/data")
def get_gie_data(
    source: str,
    country: str | None = None,
    variable: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = Query(100, le=5000),
):

    where = ["s.source = :source"]
    params = {"source": source, "limit": limit}

    if country:
        where.append("a.name = :country")
        params["country"] = country

    if variable:
        where.append("s.variable = :variable")
        params["variable"] = variable

    if start_date:
        where.append("d.value_date >= :start_date")
        params["start_date"] = start_date

    if end_date:
        where.append("d.value_date <= :end_date")
        params["end_date"] = end_date

    sql = f"""
        SELECT d.value_date, d.value, s.variable, a.name AS country
        FROM energy.daily d
        JOIN meta.series s ON d.series_id = s.series_id
        JOIN meta.assets a ON d.asset_id = a.asset_id
        WHERE {' AND '.join(where)}
        ORDER BY d.value_date DESC
        LIMIT :limit
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    return [
        {
            "date": r[0],
            "value": float(r[1]) if r[1] is not None else None,
            "variable": r[2],
            "country": r[3],
        }
        for r in rows
    ]