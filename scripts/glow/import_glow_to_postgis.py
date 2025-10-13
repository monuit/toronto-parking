"""Load glow line GeoJSON artifacts into PostGIS and ensure tile function.

This script consolidates the glow line datasets (parking tickets, red light
camera, ASE camera) into a canonical ``public.glow_lines`` table and creates
the ``public.get_glow_tile`` function that generates mapbox vector tiles
directly from PostGIS.

Usage
-----

    python scripts/glow/import_glow_to_postgis.py [--database-url ...]

If ``--database-url`` is omitted the script falls back to the standard
database environment variables (``TILES_DB_URL`` / ``DATABASE_PRIVATE_URL`` /
``DATABASE_URL``).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

from shapely.geometry import LineString, MultiLineString, shape

import psycopg

try:  # Optional dependency; present in app environment
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - fallback when dotenv unavailable
    load_dotenv = None

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    path: Path
    year_base: int


DEFAULT_DATASETS = {
    "parking_tickets": DatasetConfig(
        name="parking_tickets",
        path=PROJECT_ROOT / "map-app" / "public" / "data" / "tickets_glow_lines.geojson",
        year_base=2008,
    ),
    "red_light_locations": DatasetConfig(
        name="red_light_locations",
        path=PROJECT_ROOT / "map-app" / "public" / "data" / "red_light_glow_lines.geojson",
        year_base=2010,
    ),
    "ase_locations": DatasetConfig(
        name="ase_locations",
        path=PROJECT_ROOT / "map-app" / "public" / "data" / "ase_glow_lines.geojson",
        year_base=2010,
    ),
}


@dataclass
class GlowFeature:
    dataset: str
    centreline_id: int
    count: int
    years_mask: int
    months_mask: int
    geometry_wkt: str

def load_environment() -> None:
    if load_dotenv is None:
        return

    candidate_paths = [
        PROJECT_ROOT / ".env",
        PROJECT_ROOT / ".env.local",
        PROJECT_ROOT / ".env.production",
        PROJECT_ROOT / "map-app" / ".env.local",
        PROJECT_ROOT / "map-app" / ".env",
    ]
    for path in candidate_paths:
        if path.exists():
            load_dotenv(path, override=False)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import glow line datasets into PostGIS")
    parser.add_argument(
        "--database-url",
        dest="database_url",
        help="Override database connection string (defaults to tile DB env vars)",
    )
    parser.add_argument(
        "--dataset",
        dest="datasets",
        action="append",
        default=None,
        choices=list(DEFAULT_DATASETS.keys()),
        help="Restrict import to specific dataset key (can be specified multiple times)",
    )
    return parser.parse_args(argv)


def resolve_connection_string(override: str | None) -> str:
    if override:
        return override
    candidates = [
        os.environ.get("TILES_DB_URL"),
        os.environ.get("POSTGIS_DATABASE_URL"),
        os.environ.get("DATABASE_PRIVATE_URL"),
        os.environ.get("DATABASE_URL"),
    ]
    for candidate in candidates:
        if candidate:
            return candidate
    raise RuntimeError(
        "Database URL not provided. Set --database-url or configure TILES_DB_URL / DATABASE_PRIVATE_URL / DATABASE_URL"
    )

def chunked(values: Sequence[GlowFeature], size: int) -> Iterable[Sequence[GlowFeature]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def load_dataset(config: DatasetConfig) -> List[GlowFeature]:
    if not config.path.exists():
        raise FileNotFoundError(f"Glow dataset not found: {config.path}")
    payload = json.loads(config.path.read_text(encoding="utf-8"))
    features = []
    for feature in payload.get("features", []):
        geometry = feature.get("geometry")
        if not geometry:
            continue
        geom = shape(geometry)
        if geom.is_empty:
            continue
        if isinstance(geom, LineString):
            geom = MultiLineString([geom])
        if not isinstance(geom, (LineString, MultiLineString)):
            continue

        properties = feature.get("properties", {})
        raw_centreline = properties.get("centreline_id") or properties.get("centrelineId")
        if raw_centreline is None:
            continue
        try:
            centreline_id = int(raw_centreline)
        except (TypeError, ValueError):  # pragma: no cover - defensive guard
            continue

        count = int(properties.get("count", 0) or 0)

        years_mask = 0
        years = properties.get("years") or []
        year_base = int(properties.get("year_base") or properties.get("yearBase") or config.year_base)
        for year in years:
            try:
                offset = int(year) - year_base
            except (TypeError, ValueError):
                continue
            if 0 <= offset < 63:
                years_mask |= 1 << offset

        months_mask = 0
        months = properties.get("months") or []
        for month in months:
            try:
                month_value = int(month)
            except (TypeError, ValueError):
                continue
            if 1 <= month_value <= 12:
                months_mask |= 1 << (month_value - 1)

        features.append(
            GlowFeature(
                dataset=config.name,
                centreline_id=centreline_id,
                count=count,
                years_mask=years_mask,
                months_mask=months_mask,
                geometry_wkt=geom.wkt,
            )
        )
    return features


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.glow_lines (
                dataset TEXT NOT NULL,
                centreline_id BIGINT NOT NULL,
                count INTEGER NOT NULL,
                years_mask INTEGER NOT NULL,
                months_mask INTEGER NOT NULL,
                geom geometry(MultiLineString, 4326) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (dataset, centreline_id)
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS glow_lines_geom_idx ON public.glow_lines USING GIST (geom);
            CREATE INDEX IF NOT EXISTS glow_lines_dataset_count_idx ON public.glow_lines (dataset, count DESC);
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION public.get_glow_tile(
                p_dataset TEXT,
                p_z INTEGER,
                p_x INTEGER,
                p_y INTEGER
            )
            RETURNS BYTEA
            LANGUAGE SQL
            STABLE
            AS $$
                WITH bounds AS (
                    SELECT ST_TileEnvelope(p_z, p_x, p_y) AS geom
                ), metrics AS (
                    SELECT
                        geom AS tile_geom,
                        (ST_XMax(geom) - ST_XMin(geom)) AS tile_width
                    FROM bounds
                ), buffered AS (
                    SELECT
                        tile_geom,
                        tile_width,
                        ST_Buffer(tile_geom, (tile_width / 4096.0) * 32.0) AS geom
                    FROM metrics
                ), source AS (
                    SELECT
                        gl.centreline_id,
                        gl.count,
                        gl.years_mask,
                        gl.months_mask,
                        CASE
                            WHEN p_z <= 9 THEN ST_SimplifyVW(gl.geom_3857, tile_width / 6.0)
                            WHEN p_z <= 11 THEN ST_SimplifyVW(gl.geom_3857, tile_width / 12.0)
                            WHEN p_z <= 13 THEN ST_SimplifyVW(gl.geom_3857, tile_width / 24.0)
                            WHEN p_z <= 15 THEN ST_SimplifyVW(gl.geom_3857, tile_width / 48.0)
                            ELSE gl.geom_3857
                        END AS geom_3857
                    FROM (
                        SELECT
                            centreline_id,
                            count,
                            years_mask,
                            months_mask,
                            ST_Transform(geom, 3857) AS geom_3857
                        FROM public.glow_lines
                        WHERE dataset = p_dataset
                    ) AS gl
                    CROSS JOIN buffered b
                    WHERE ST_Intersects(gl.geom_3857, b.geom)
                ), clipped AS (
                    SELECT
                        centreline_id,
                        count,
                        years_mask,
                        months_mask,
                        ST_AsMVTGeom(
                            geom_3857,
                            (SELECT tile_geom FROM metrics LIMIT 1),
                            4096,
                            32,
                            TRUE
                        ) AS geom
                    FROM source
                    WHERE geom_3857 IS NOT NULL
                )
                SELECT COALESCE(
                    (SELECT ST_AsMVT(clipped, 'glow_lines', 4096, 'geom') FROM clipped),
                    '\\x'::BYTEA
                );
            $$;
            """
        )
        cur.execute("ANALYZE public.glow_lines;")
        conn.commit()


def replace_data(conn: psycopg.Connection, rows: Iterable[GlowFeature]) -> None:
    rows = list(rows)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE public.glow_lines;")
        insert_sql = """
            INSERT INTO public.glow_lines
                (dataset, centreline_id, count, years_mask, months_mask, geom)
            VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326))
        """
        for batch in chunked(rows, 1000):
            cur.executemany(
                insert_sql,
                [
                    (
                        feature.dataset,
                        feature.centreline_id,
                        feature.count,
                        feature.years_mask,
                        feature.months_mask,
                        feature.geometry_wkt,
                    )
                    for feature in batch
                ],
            )
        cur.execute("ANALYZE public.glow_lines;")
        conn.commit()


def main(argv: Sequence[str] | None = None) -> None:
    load_environment()

    args = parse_args(argv)
    selected = set(args.datasets) if args.datasets else set(DEFAULT_DATASETS.keys())
    datasets = {key: DEFAULT_DATASETS[key] for key in selected}

    features: list[GlowFeature] = []
    for dataset, config in datasets.items():
        print(f"[glow] Loading {dataset} from {config.path}")
        dataset_features = load_dataset(config)
        print(f"[glow]   extracted {len(dataset_features):,} features")
        features.extend(dataset_features)

    if not features:
        print("[glow] No features extracted; aborting")
        return

    connection_string = resolve_connection_string(args.database_url)
    print("[glow] Connecting to database")
    with psycopg.connect(connection_string) as conn:
        ensure_schema(conn)
        replace_data(conn, features)
    print(f"[glow] Imported {len(features):,} features into public.glow_lines")


if __name__ == "__main__":  # pragma: no cover - script entry point
    main()
