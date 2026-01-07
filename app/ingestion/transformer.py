import pandas as pd
from datetime import datetime
import pytz


def clean_json_payload(row: dict) -> dict:
    """Convert NaN/NaT to None so PostgreSQL JSONB accepts it."""
    return {k: (None if pd.isna(v) else v) for k, v in row.items()}



UTC = pytz.UTC


# -----------------------------
# GAS QUALITY (NATIONAL GAS)
# -----------------------------
def transform_gas_quality_rest(df: pd.DataFrame, series_id: str):
    records = []

    # series_id format: NG_GAS_QUALITY_<SITEID>_<METRIC>
    parts = series_id.split("_")
    site_id = int(parts[-2])
    metric = parts[-1].lower()

    # Support both naming styles
    col1 = f"siteGasQualityDetail_{metric}"
    col2 = f"siteGasQualityDetail.{metric}"

    if col1 in df.columns:
        value_col = col1
    elif col2 in df.columns:
        value_col = col2
    else:
        return []

    for _, row in df[df["siteId"] == site_id].iterrows():
        value = row.get(value_col)

        if pd.isna(value):
            continue

        records.append({
            "series_id": series_id,
            # 🔥 National Gas "latestdata" has no timestamp → use ingestion time
            "observation_time": pd.Timestamp.utcnow(),
            "value": float(value),
            "quality_flag": None,
            "raw_payload": clean_json_payload(row.to_dict()),
        })

    return records


# -----------------------------
# ENTSOG
# -----------------------------
def transform_entsog_rest(df: pd.DataFrame, series_id: str):
    records = []

    parts = series_id.split("_")

    if len(parts) < 5:
        return []

    _, _, *rest = parts
    direction = rest[-1].lower()
    point = rest[-2]
    indicator = " ".join(rest[:-2]).replace("_", " ").lower()

    df_norm = df.copy()
    df_norm["indicator_norm"] = df_norm["indicator"].astype(str).str.lower().str.strip()
    df_norm["pointKey_norm"] = df_norm["pointKey"].astype(str).str.strip()
    df_norm["directionKey_norm"] = df_norm["directionKey"].astype(str).str.lower().str.strip()

    filtered = df_norm[
        (df_norm["indicator_norm"] == indicator) &
        (df_norm["pointKey_norm"] == point) &
        (df_norm["directionKey_norm"] == direction)
    ]

    for _, row in filtered.iterrows():
        value = row.get("value")
        if pd.isna(value):
            continue

        ts = row.get("periodFrom")

        records.append({
            "series_id": series_id,
            "observation_time": pd.to_datetime(ts, utc=True),
            "value": float(value),
            "quality_flag": row.get("flowStatus"),
            "raw_payload": clean_json_payload(row.to_dict()),  # 🔥 FIX
        })

    return records