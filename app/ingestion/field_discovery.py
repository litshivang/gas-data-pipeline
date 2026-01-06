from sqlalchemy import text
from app.db.connection import engine
from datetime import datetime


def infer_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    return "string"


def discover_fields(dataset_id: str):
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT raw_payload
                FROM raw_events
                WHERE dataset_id = :dataset_id
            """),
            {"dataset_id": dataset_id}
        ).fetchall()

        stats = {}

        for (payload,) in rows:
            for k, v in payload.items():
                if k not in stats:
                    stats[k] = {
                        "types": set(),
                        "nulls": 0,
                        "example": v,
                    }

                if v is None:
                    stats[k]["nulls"] += 1
                else:
                    stats[k]["types"].add(infer_type(v))

        for field, meta in stats.items():
            inferred = ",".join(sorted(meta["types"])) or "null"
            nullable = meta["nulls"] > 0

            conn.execute(
                text("""
                    INSERT INTO field_catalog
                    (dataset_id, field_name, inferred_type, nullable, example_value)
                    VALUES (:dataset_id, :field, :type, :nullable, :example)
                    ON CONFLICT (dataset_id, field_name) DO NOTHING
                """),
                {
                    "dataset_id": dataset_id,
                    "field": field,
                    "type": inferred,
                    "nullable": nullable,
                    "example": str(meta["example"])[:200],
                }
            )
