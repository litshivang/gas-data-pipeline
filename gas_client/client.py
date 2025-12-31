import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy import text
from app.db.connection import engine


class GasClient:
    def get_history(
        self,
        series_id: str,
        last_days: int | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:

        if last_days is None and (start is None or end is None):
            raise ValueError("Provide either last_days or start & end")

        if last_days is not None:
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(days=last_days)
        else:
            start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
            end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)

        query = text("""
            SELECT
                observation_time,
                value
            FROM data_observations
            WHERE series_id = :series_id
              AND observation_time BETWEEN :start_dt AND :end_dt
            ORDER BY observation_time
        """)

        with engine.begin() as conn:
            rows = conn.execute(
                query,
                {
                    "series_id": series_id,
                    "start_dt": start_dt,
                    "end_dt": end_dt,
                },
            ).fetchall()

        df = pd.DataFrame(rows, columns=["observation_time", "value"])
        df.set_index("observation_time", inplace=True)
        return df
