from fastapi import FastAPI
from app.api.v2 import health
from app.api.v2.routes import router as v2_router
from app.api.v2.discovery import router as discovery_router
from app.api.v2.ingestion import router as ingestion_router
from app.api.v2.export import router as export_router


app = FastAPI(
    title="Gas Data Platform",
    version="0.1.0",
    description="Read-only API for National Gas data"
)

app.include_router(health.router)
app.include_router(v2_router)
app.include_router(discovery_router)
app.include_router(ingestion_router)
app.include_router(export_router)

