from datetime import datetime
from app.ingestion.gie.constants import EXCLUDED_KEYS, NULL_LIKE_VALUES


def transform(dataset: str, raw_json: dict):
    rows = []

    for entry in raw_json.get("data", []):
        country = entry.get("name")
        gas_day = entry.get("gasDayStart")

        if not gas_day:
            continue

        parsed_date = datetime.strptime(gas_day, "%Y-%m-%d").date()

        for key, value in entry.items():

            if key in EXCLUDED_KEYS:
                continue

            # Skip lists (GIE often returns info: [])
            if isinstance(value, list):
                continue
            # -----------------------------
            # 🔥 Handle nested dicts (ALSI)
            # -----------------------------
            if isinstance(value, dict):
                for sub_k, sub_v in value.items():

                    if sub_v in NULL_LIKE_VALUES:
                        numeric_value = None
                    else:
                        try:
                            numeric_value = float(sub_v)
                        except (ValueError, TypeError):
                            continue

                    rows.append({
                        "country": country,
                        "date": parsed_date,
                        "variable": f"{key}_{sub_k}",
                        "value": numeric_value,
                        "quality": entry.get("status"),
                    })
                continue

            # -----------------------------
            # Standard numeric (AGSI)
            # -----------------------------
            if value in NULL_LIKE_VALUES:
                numeric_value = None
            else:
                try:
                    numeric_value = float(value)
                except (ValueError, TypeError):
                    continue

            rows.append({
                "country": country,
                "date": parsed_date,
                "variable": key,
                "value": numeric_value,
                "quality": entry.get("status"),
            })

    return rows
