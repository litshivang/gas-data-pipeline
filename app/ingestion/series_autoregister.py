import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from app.db.connection import engine
from app.db.models import MetaSeries


def make_series_id(dataset_id: str, *parts) -> str:
    slug = "_".join(
        p.upper()
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
        .replace(" ", "_")
        for p in parts if p
    )
    return f"NG_{dataset_id}_{slug}"


def register_series_from_df(df, dataset_id: str):

    # ================= GAS QUALITY (REST) =================

    if dataset_id == "GAS_QUALITY" and "siteId" in df.columns:

        # 🔥 Only numeric metrics
        metric_cols = [
            c for c in df.columns
            if c not in {"siteId", "areaName", "siteName", "publishedTime"}
            and pd.api.types.is_numeric_dtype(df[c])
        ]

        if not metric_cols:
            return {}

        series_map = {}

        for site_id in df["siteId"].dropna().unique():
            for col in metric_cols:

                series_id = make_series_id(dataset_id, str(int(site_id)), col.upper())
                series_map[(site_id, col)] = series_id

                record = {
                    "series_id": series_id,
                    "source": "NATIONAL_GAS",
                    "dataset_id": dataset_id,
                    "data_item": col,
                    "description": f"{col} at site {int(site_id)}",
                    "unit": "UNKNOWN",
                    "frequency": "intraday",
                    "timezone_source": "UTC",
                    "is_active": True,
                }

                stmt = insert(MetaSeries).values(record)
                stmt = stmt.on_conflict_do_nothing(index_elements=["series_id"])

                with engine.begin() as conn:
                    conn.execute(stmt)

        return series_map

    # ================= ENTSOG =================
    REQUIRED = {"indicator", "pointKey", "directionKey"}
    if REQUIRED.issubset(df.columns):

        series_map = {}

        for _, row in (
            df[["indicator", "pointKey", "directionKey"]]
            .dropna()
            .drop_duplicates()
            .iterrows()
        ):
            indicator = row["indicator"]
            point = row["pointKey"]
            direction = row["directionKey"]

            series_id = make_series_id(dataset_id, indicator, point, direction)
            series_map[(indicator, point, direction)] = series_id

            record = {
                "series_id": series_id,
                "source": "ENTSOG",
                "dataset_id": dataset_id,
                "data_item": indicator,
                "description": f"{indicator} at {point} ({direction})",
                "unit": "UNKNOWN",
                "frequency": "daily",
                "timezone_source": "Europe/Brussels",
                "is_active": True,
            }

            stmt = insert(MetaSeries).values(record)
            stmt = stmt.on_conflict_do_nothing(index_elements=["series_id"])

            with engine.begin() as conn:
                conn.execute(stmt)

        return series_map


    # ================= INSTANTANEOUS FLOW =================
    if dataset_id == "INSTANTANEOUS_FLOW" and "siteName" in df.columns:

        series_map = {}

        for site in df["siteName"].dropna().unique():

            series_id = make_series_id(dataset_id, site, "FLOWRATE")
            series_map[(site, "flowRate")] = series_id

            record = {
                "series_id": series_id,
                "source": "NATIONAL_GAS",
                "dataset_id": dataset_id,
                "data_item": "flowRate",
                "description": f"Instantaneous Flow at {site}",
                "unit": "UNKNOWN",
                "frequency": "intraday",
                "timezone_source": "Europe/London",
                "is_active": True,
            }

            stmt = insert(MetaSeries).values(record)
            stmt = stmt.on_conflict_do_nothing(index_elements=["series_id"])

            with engine.begin() as conn:
                conn.execute(stmt)

        return series_map

    return {}
