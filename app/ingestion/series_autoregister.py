from sqlalchemy.dialects.postgresql import insert
from app.db.connection import engine
from app.db.models import MetaSeries

def make_series_id(dataset_id: str, data_item: str) -> str:
    slug = (
        data_item.upper()
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
        .replace(" ", "_")
    )
    return f"NG_{dataset_id}_{slug}"


def register_series_from_df(df, dataset_id: str):
    if "Data Item" not in df.columns:
        return {}

    series_map = {}

    for item in df["Data Item"].dropna().unique():
        series_id = make_series_id(dataset_id, item)
        series_map[item] = series_id

        record = {
            "series_id": series_id,
            "source": "NATIONAL_GAS",
            "dataset_id": dataset_id,
            "data_item": item,
            "description": item,
            "unit": "UNKNOWN",
            "frequency": "daily",
            "timezone_source": "Europe/London",
            "is_active": True,
        }

        stmt = insert(MetaSeries).values(record)
        stmt = stmt.on_conflict_do_nothing(index_elements=["series_id"])

        with engine.begin() as conn:
            conn.execute(stmt)

    return series_map
