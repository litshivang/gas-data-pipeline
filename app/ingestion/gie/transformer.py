from datetime import datetime
from app.ingestion.gie.constants import EXCLUDED_KEYS, NULL_LIKE_VALUES


def transform(dataset: str, raw_json: dict):
    rows = []

    for entry in raw_json.get("data", []):
        country = entry.get("name")
        gas_day = entry.get("gasDayStart")

        for key, value in entry.items():
            if key in EXCLUDED_KEYS:
                continue

            if value in NULL_LIKE_VALUES:
                numeric_value = None
            else:
                try:
                    numeric_value = float(value)
                except (ValueError, TypeError):
                    continue

            rows.append({
                "country": country,
                "date": datetime.strptime(gas_day, "%Y-%m-%d").date(),
                "variable": key,
                "value": numeric_value,
                "quality": entry.get("status"),
            })

    return rows
