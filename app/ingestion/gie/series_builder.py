from sqlalchemy import text
from app.db.connection import engine


def get_or_create_asset(name: str, level: str, quality: str | None):
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                SELECT asset_id FROM meta.assets
                WHERE name = :name
            """),
            {"name": name}
        ).fetchone()

        if result:
            return result[0]

        result = conn.execute(
            text("""
                INSERT INTO meta.assets (name, type, level, quality)
                VALUES (:name, 'Storage', :level, :quality)
                RETURNING asset_id
            """),
            {"name": name, "level": level, "quality": quality}
        ).fetchone()

        return result[0]


def get_or_create_series(asset_id: int, variable: str, source: str):
    unique_key = f"{asset_id}_{variable}_{source}"

    with engine.begin() as conn:
        result = conn.execute(
            text("""
                SELECT series_id FROM meta.series
                WHERE series_unique_concat = :uk
            """),
            {"uk": unique_key}
        ).fetchone()

        if result:
            return result[0]

        result = conn.execute(
            text("""
                INSERT INTO meta.series
                (series_name, asset_id, series_unique_concat, variable, source)
                VALUES (:name, :asset_id, :uk, :variable, :source)
                RETURNING series_id
            """),
            {
                "name": f"{variable}_{asset_id}",
                "asset_id": asset_id,
                "uk": unique_key,
                "variable": variable,
                "source": source,
            }
        ).fetchone()

        return result[0]
