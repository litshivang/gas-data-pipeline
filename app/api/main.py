from fastapi import FastAPI
from app.api.routes import health, series, data

app = FastAPI(
    title="Gas Data Platform",
    version="0.1.0",
    description="Read-only API for National Gas data"
)

app.include_router(health.router)
app.include_router(series.router)
app.include_router(data.router)
