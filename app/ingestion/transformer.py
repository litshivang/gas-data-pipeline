import pandas as pd
import pytz

UTC = pytz.UTC


def clean_json_payload(row: dict) -> dict:
    return {k: (None if pd.isna(v) else v) for k, v in row.items()}


# -----------------------------
# GAS QUALITY (REST)
# -----------------------------
def transform_gas_quality_rest(df: pd.DataFrame, series_id: str):
    records = []

    parts = series_id.split("_")
    site_id = int(parts[-2])
    metric = parts[-1].lower()

    if metric not in df.columns:
        return []

    for _, row in df[df["siteId"] == site_id].iterrows():
        value = row.get(metric)
        if pd.isna(value):
            continue

        ts = row.get("publishedTime")

        records.append({
            "series_id": series_id,
            "observation_time": pd.to_datetime(ts, utc=True),
            "value": float(value),
            "quality_flag": None,
            "raw_payload": clean_json_payload(row.to_dict()),
        })

    return records



# -----------------------------
# ENTSOG
# -----------------------------
def transform_entsog_rest(df: pd.DataFrame, series_id: str, from_date=None, to_date=None):
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

    # 🔥 DATE FILTER
    filtered["periodFrom_dt"] = pd.to_datetime(filtered["periodFrom"], errors="coerce")

    if from_date:
        filtered = filtered[filtered["periodFrom_dt"] >= pd.to_datetime(from_date)]
    if to_date:
        filtered = filtered[filtered["periodFrom_dt"] <= pd.to_datetime(to_date)]

    for _, row in filtered.iterrows():
        value = row.get("value")

        if value in (None, "", " "):
            continue

        try:
            value = float(value)
        except ValueError:
            continue

        ts = row.get("periodFrom")

        records.append({
            "series_id": series_id,
            "observation_time": pd.to_datetime(ts, utc=True),
            "value": value,
            "quality_flag": row.get("flowStatus"),
            "raw_payload": clean_json_payload(row.to_dict()),
        })

    return records


# -----------------------------
# INSTANTANEOUS FLOW
# -----------------------------

def transform_instantaneous_flow(df: pd.DataFrame, series_id: str):
    records = []

    # Remove prefix
    prefix = "NG_INSTANTANEOUS_FLOW_"
    if not series_id.startswith(prefix):
        return []

    # Remove prefix and suffix
    site = series_id[len(prefix):].rsplit("_FLOWRATE", 1)[0]

    filtered = df[df["siteName"].str.upper().str.replace(" ", "_") == site]

    for _, row in filtered.iterrows():
        value = row.get("flowRate")
        if value is None:
            continue

        records.append({
            "series_id": series_id,
            "observation_time": pd.to_datetime(row["applicableAt"], utc=True),
            "value": float(value),
            "quality_flag": row.get("qualityIndicator"),
            "raw_payload": clean_json_payload(row.to_dict()),
        })

    return records
