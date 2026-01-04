import pandas as pd
from datetime import datetime, timedelta
import pytz
import pandas as pd
import math

UTC = pytz.UTC
LONDON = pytz.timezone("Europe/London")


def gas_day_to_utc(gas_day: pd.Timestamp) -> datetime:
    """
    Gas Day starts at 06:00 UK local time.
    We store the canonical start of Gas Day in UTC.
    """
    local_dt = LONDON.localize(
        datetime.combine(gas_day.date(), datetime.min.time())
        + timedelta(hours=6)
    )
    return local_dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def clean_value(v):
    if pd.isna(v):
        return None
    return v


def clean_json_payload(row: dict) -> dict:
    return {k: clean_value(v) for k, v in row.items()}


def transform_ng_csv(df, series_id):
    records = []

    for _, row in df.iterrows():
        # 🔥 Skip rows without numeric value
        if pd.isna(row["Value"]):
            continue

        raw = clean_json_payload(row.to_dict())

        records.append({
            "series_id": series_id,
            "observation_time": pd.to_datetime(
                row["Applicable For"], dayfirst=True
            ),
            "value": float(row["Value"]),
            "quality_flag": (
                None if pd.isna(row.get("Quality Indicator"))
                else str(row.get("Quality Indicator"))
            ),
            "raw_payload": raw,
        })

    return records


def transform_demand_csv(df: pd.DataFrame, series_id: str) -> list[dict]:
    REQUIRED_COLS = {"Applicable For", "Value"}
    missing = REQUIRED_COLS - set(df.columns)

    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    records = []

    for _, row in df.iterrows():
        gas_day = pd.to_datetime(row["Applicable For"], dayfirst=True)
        value = float(row["Value"])

        records.append(
            {
                "series_id": series_id,
                "observation_time": gas_day_to_utc(gas_day),
                "value": value,
                "quality_flag": "ACTUAL",
            }
        )

    return records
