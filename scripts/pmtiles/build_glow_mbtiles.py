"""Convert glow GeoJSON datasets into MBTiles archives without tippecanoe.

Each glow dataset is a centreline-derived line collection enriched with
ticket totals and temporal coverage.  To keep client-side rendering fast, we
pre-encode those features into vector tiles (zoom levels 9-16) and later
convert the MBTiles artifacts into PMTiles suitable for CDN distribution.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import mercantile
from mapbox_vector_tile import encode as encode_mvt
from shapely.geometry import LineString, MultiLineString, box, mapping, shape
from shapely.geometry.base import BaseGeometry


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "map-app" / "public" / "data"
EXPORT_DIR = PROJECT_ROOT / "pmtiles" / "exports"
MBTILES_DIR = PROJECT_ROOT / "pmtiles" / "mbtiles"

DEFAULT_BUFFER = 32


def _simplification_tolerance(zoom: int) -> float:
    if zoom <= 9:
        return 0.0035
    if zoom <= 11:
        return 0.0015
    if zoom <= 13:
        return 0.0006
    if zoom <= 15:
        return 0.00025
    return 0.00008


def simplify_geometry(geom: BaseGeometry, zoom: int) -> BaseGeometry:
    tolerance = _simplification_tolerance(zoom)
    if tolerance <= 0:
        return geom
    simplified = geom.simplify(tolerance, preserve_topology=False)
    return simplified if not simplified.is_empty else geom


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    source_path: Path
    mbtiles_name: str
    min_zoom: int
    max_zoom: int
    year_base: int


DATASETS: tuple[DatasetConfig, ...] = (
    DatasetConfig(
        "parking_tickets",
        DATA_DIR / "tickets_glow_lines.geojson",
        "parking_glow_lines.mbtiles",
        9,
        16,
        2008,
    ),
    DatasetConfig(
        "red_light_locations",
        DATA_DIR / "red_light_glow_lines.geojson",
        "red_light_glow_lines.mbtiles",
        9,
        15,
        2010,
    ),
    DatasetConfig(
        "ase_locations",
        DATA_DIR / "ase_glow_lines.geojson",
        "ase_glow_lines.mbtiles",
        9,
        15,
        2010,
    ),
)


def ensure_directories() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    MBTILES_DIR.mkdir(parents=True, exist_ok=True)


def load_features(dataset: DatasetConfig) -> list[tuple[BaseGeometry, dict]]:
    raw_path = dataset.source_path
    if not raw_path.exists():
        raise FileNotFoundError(f"Glow dataset not found: {raw_path}")

    payload = json.loads(raw_path.read_text())
    features: list[tuple[BaseGeometry, dict]] = []

    for feature in payload.get("features", []):
        geometry = feature.get("geometry")
        properties = feature.get("properties") or {}
        if not geometry:
            continue

        geom = shape(geometry)
        if geom.is_empty:
            continue
        if not isinstance(geom, (LineString, MultiLineString)):
            continue

        centreline_id = properties.get("centreline_id") or properties.get("centrelineId")
        if centreline_id is None:
            continue

        years = properties.get("years") or []
        months = properties.get("months") or []
        count = int(properties.get("count", 0) or 0)

        years_mask = 0
        for year in years:
            try:
                offset = int(year) - dataset.year_base
            except (TypeError, ValueError):
                continue
            if 0 <= offset < 53:
                years_mask |= 1 << offset

        months_mask = 0
        for month in months:
            try:
                month_val = int(month)
            except (TypeError, ValueError):
                continue
            if 1 <= month_val <= 12:
                months_mask |= 1 << (month_val - 1)

        feature_props = {
            "centreline_id": int(centreline_id),
            "count": count,
            "years_mask": years_mask,
            "months_mask": months_mask,
        }
        features.append((geom, feature_props))

    if not features:
        raise RuntimeError(f"No usable features found in {raw_path}")

    return features


def init_mbtiles(path: Path) -> sqlite3.Connection:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE metadata (name TEXT, value TEXT)")
    conn.execute(
        """
        CREATE TABLE tiles (
            zoom_level INTEGER,
            tile_column INTEGER,
            tile_row INTEGER,
            tile_data BLOB
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX tiles_zxy ON tiles (zoom_level, tile_column, tile_row)")
    return conn


def write_metadata(conn: sqlite3.Connection, dataset: DatasetConfig, bounds: mercantile.LngLatBbox) -> None:
    layer_description = f"{dataset.key.replace('_', ' ')} glow lines".strip().title()
    metadata_json = {
        "vector_layers": [
            {
                "id": "glow_lines",
                "description": layer_description,
                "minzoom": dataset.min_zoom,
                "maxzoom": dataset.max_zoom,
                "fields": {
                    "centreline_id": "Number",
                    "count": "Number",
                    "years_mask": "Number",
                    "months_mask": "Number",
                },
            }
        ],
        "year_base": dataset.year_base,
    }
    metadata = {
        "name": dataset.key,
        "type": "overlay",
        "version": "1.1",
        "description": f"Glow line tiles for {dataset.key}",
        "format": "pbf",
        "minzoom": str(dataset.min_zoom),
        "maxzoom": str(dataset.max_zoom),
        "bounds": ",".join(str(round(value, 6)) for value in (bounds.west, bounds.south, bounds.east, bounds.north)),
        "center": ",".join(
            [
                str(round((bounds.west + bounds.east) / 2, 6)),
                str(round((bounds.south + bounds.north) / 2, 6)),
                str((dataset.min_zoom + dataset.max_zoom) // 2),
            ]
        ),
        "json": json.dumps(metadata_json, ensure_ascii=False),
    }
    conn.executemany("INSERT INTO metadata (name, value) VALUES (?, ?)", metadata.items())


def clip_and_encode(features: list[tuple[BaseGeometry, dict]], tile_bounds: mercantile.LngLatBbox, zoom: int) -> bytes | None:
    width = tile_bounds.east - tile_bounds.west
    height = tile_bounds.north - tile_bounds.south
    padding_x = max(width * 0.02, 1e-6)
    padding_y = max(height * 0.02, 1e-6)
    expanded_bbox = box(
        tile_bounds.west - padding_x,
        tile_bounds.south - padding_y,
        tile_bounds.east + padding_x,
        tile_bounds.north + padding_y,
    )
    tile_features = []

    for geom, props in features:
        if not geom.intersects(expanded_bbox):
            continue
        clipped = geom.intersection(expanded_bbox)
        if clipped.is_empty:
            continue
        simplified = simplify_geometry(clipped, zoom)
        if simplified.is_empty:
            continue
        tile_features.append({
            "geometry": mapping(simplified),
            "properties": props,
        })

    if not tile_features:
        return None

    layer = {
        "name": "glow_lines",
        "features": tile_features,
        "extent": 4096,
        "version": 2,
    }

    options = {
        "quantize_bounds": (tile_bounds.west, tile_bounds.south, tile_bounds.east, tile_bounds.north),
        "y_coord_down": True,
        "buffer": DEFAULT_BUFFER,
    }
    return encode_mvt([layer], default_options=options)


def build_mbtiles(dataset: DatasetConfig) -> Path:
    features = load_features(dataset)

    total_bounds = None
    for geom, _ in features:
        minx, miny, maxx, maxy = geom.bounds
        if total_bounds is None:
            total_bounds = (minx, miny, maxx, maxy)
        else:
            total_bounds = (
                min(total_bounds[0], minx),
                min(total_bounds[1], miny),
                max(total_bounds[2], maxx),
                max(total_bounds[3], maxy),
            )

    if total_bounds is None:
        raise RuntimeError("Unable to derive bounds for glow dataset")

    bounds = mercantile.LngLatBbox(*total_bounds)
    mbtiles_path = MBTILES_DIR / dataset.mbtiles_name
    conn = init_mbtiles(mbtiles_path)
    write_metadata(conn, dataset, bounds)

    tile_count = 0
    for zoom in range(dataset.min_zoom, dataset.max_zoom + 1):
        tiles = mercantile.tiles(bounds.west, bounds.south, bounds.east, bounds.north, [zoom])
        for tile in tiles:
            tile_bounds = mercantile.bounds(tile)
            encoded = clip_and_encode(features, tile_bounds, zoom)
            if not encoded:
                continue
            tms_y = (2 ** zoom - 1) - tile.y
            conn.execute(
                "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                (zoom, tile.x, tms_y, sqlite3.Binary(encoded)),
            )
            tile_count += 1

    conn.commit()
    conn.close()
    print(f"wrote {mbtiles_path.relative_to(PROJECT_ROOT)} ({tile_count} tiles)")
    return mbtiles_path


def run(datasets: Iterable[DatasetConfig]) -> None:
    ensure_directories()
    for dataset in datasets:
        build_mbtiles(dataset)


if __name__ == "__main__":
    run(DATASETS)
