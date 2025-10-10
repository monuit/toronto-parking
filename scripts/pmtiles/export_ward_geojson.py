"""Export minimal ward-level GeoJSON for PMTiles preprocessing.

This script fetches ward geometries and aggregated metrics from PostGIS,
clips them to the Greater Toronto Area envelope, and writes trimmed
GeoJSON files containing only the properties required for styling.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOTENV_PATH = PROJECT_ROOT / ".env"
EXPORT_DIR = PROJECT_ROOT / "pmtiles" / "exports"


AOI_BOUNDS = (-79.6393, 43.4032, -79.1169, 43.8554)


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    output_name: str


DATASETS: tuple[DatasetConfig, ...] = (
    DatasetConfig("red_light_locations", "red_light_ward_choropleth.geojson"),
    DatasetConfig("ase_locations", "ase_ward_choropleth.geojson"),
    DatasetConfig("cameras_combined", "cameras_combined_ward_choropleth.geojson"),
)


def load_env() -> None:
    if DOTENV_PATH.exists():
        load_dotenv(DOTENV_PATH)


def resolve_dsn() -> str:
    candidates = (
        os.getenv("DATABASE_PRIVATE_URL"),
        os.getenv("DATABASE_URL"),
        os.getenv("DATABASE_PUBLIC_URL"),
        os.getenv("POSTGRES_URL"),
    )
    for dsn in candidates:
        if not dsn:
            continue
        try:
            with psycopg.connect(dsn, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return dsn
        except Exception:
            continue
    raise RuntimeError("Unable to resolve a working Postgres DSN")


def fetch_dataset(conn: psycopg.Connection, dataset: DatasetConfig) -> dict:
    west, south, east, north = AOI_BOUNDS
    sql = """
        WITH aoi AS (
            SELECT ST_SetSRID(ST_MakeEnvelope(%s, %s, %s, %s), 4326) AS geom
        )
        SELECT
            wards.ward_code,
            wards.ward_name,
            totals.ticket_count,
            totals.total_revenue,
            ST_AsGeoJSON(ST_Intersection(wards.geom, aoi.geom)) AS geometry
        FROM city_wards AS wards
        CROSS JOIN aoi
        LEFT JOIN camera_ward_totals AS totals
          ON totals.ward_code = wards.ward_code
         AND totals.dataset = %s
        WHERE ST_Intersects(wards.geom, aoi.geom)
        ORDER BY wards.ward_code
    """
    features: list[dict] = []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (west, south, east, north, dataset.key))
        for row in cur:
            geometry = row.get("geometry")
            if not geometry:
                continue
            geom_obj = json.loads(geometry)
            if not geom_obj:
                continue
            properties = {
                "wardCode": row.get("ward_code"),
                "ticketCount": int(row.get("ticket_count" or 0)),
                "totalRevenue": float(row.get("total_revenue" or 0.0)),
            }
            features.append(
                {
                    "type": "Feature",
                    "geometry": geom_obj,
                    "properties": properties,
                }
            )
    return {
        "type": "FeatureCollection",
        "name": dataset.key,
        "features": features,
        "meta": {
            "dataset": dataset.key,
            "bounds": AOI_BOUNDS,
            "generated": os.getenv("SOURCE_GENERATED_AT"),
        },
    }


def write_geojson(payload: dict, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, separators=(",", ":")))


def run(datasets: Iterable[DatasetConfig]) -> None:
    load_env()
    dsn = resolve_dsn()
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(dsn) as conn:
        for dataset in datasets:
            geojson = fetch_dataset(conn, dataset)
            output_path = EXPORT_DIR / dataset.output_name
            write_geojson(geojson, output_path)
            print(f"wrote {output_path.relative_to(PROJECT_ROOT)} ({len(geojson['features'])} features)")


if __name__ == "__main__":
    run(DATASETS)
