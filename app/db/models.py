from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    Float,
    ForeignKey,
    Text,
)
from sqlalchemy.orm import declarative_base
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB

raw_payload = Column(JSONB)


Base = declarative_base()



class MetaSeries(Base):
    __tablename__ = "meta_series"

    series_id = Column(String, primary_key=True)
    source = Column(String, nullable=False)
    description = Column(Text)
    unit = Column(String, nullable=False)
    frequency = Column(String, nullable=False)
    timezone_source = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    dataset_id = Column(String, nullable=True)
    data_item = Column(Text, nullable=True)
    lookback_days = Column(Integer, default=30)


class DataObservation(Base):
    __tablename__ = "data_observations"

    series_id = Column(
        String,
        ForeignKey("meta_series.series_id"),
        primary_key=True,
        nullable=False,
    )

    observation_time = Column(
        DateTime(timezone=True),
        primary_key=True,
        nullable=False,
    )

    ingestion_time = Column(DateTime, default=datetime.utcnow)
    value = Column(Float, nullable=False)
    quality_flag = Column(String, default="UNKNOWN")

    raw_payload = Column(JSONB)  # 🔥 REQUIRED
