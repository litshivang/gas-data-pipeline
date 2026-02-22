from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional, Dict, Any


class DataPoint(BaseModel):
    timestamp: datetime   # 🔥 FIX
    value: float
    quality_flag: Optional[str] = "UNKNOWN"
    raw_payload: Optional[Dict[str, Any]] = None


class SeriesResponse(BaseModel):
    series_id: str
    dataset_id: str
    description: str
    unit: str
    frequency: str
    points: List[DataPoint]


class GasPublicationRequest(BaseModel):
    from_date: str
    to_date: str
    publication_ids: List[str]