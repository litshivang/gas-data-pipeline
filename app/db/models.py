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
from sqlalchemy.dialects.postgresql import JSONB, UUID
import uuid
from sqlalchemy import UniqueConstraint


raw_payload = Column(JSONB)


Base = declarative_base()


class MetaSeries(Base):
    __tablename__ = "meta_series"

    series_id = Column(String, primary_key=True)
    dataset_id = Column(String, nullable=False)   # 🔥 should NOT be nullable
    source = Column(String, nullable=False)
    source_type = Column(String, default="NATIONAL_GAS")  # 🔥 add
    description = Column(Text)
    unit = Column(String, nullable=False)
    frequency = Column(String, nullable=False)
    timezone_source = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    lookback_days = Column(Integer, default=30)
    data_item = Column(Text)
    last_ingested_at = Column(DateTime)           # 🔥 add
    created_at = Column(DateTime, default=datetime.utcnow)


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


class RawEvent(Base):
    __tablename__ = "raw_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(Text, nullable=False)
    dataset_id = Column(Text, nullable=False)
    series_hint = Column(Text)
    event_time = Column(DateTime)
    ingested_at = Column(DateTime, default=datetime.utcnow)
    raw_payload = Column(JSONB, nullable=False)



class FieldCatalog(Base):
    __tablename__ = "field_catalog"
    __table_args__ = (
        UniqueConstraint("dataset_id", "field_name", name="uq_dataset_field"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(Text, nullable=False)
    field_name = Column(Text, nullable=False)
    inferred_type = Column(Text, nullable=False)
    nullable = Column(Boolean, nullable=False)
    example_value = Column(Text)
    first_seen_at = Column(DateTime, default=datetime.utcnow)