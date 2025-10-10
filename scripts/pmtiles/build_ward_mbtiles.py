"""Build MBTiles archives for ward choropleth datasets without tippecanoe.

This script consumes the trimmed GeoJSON exports produced by
``export_ward_geojson.py`` and rasterises them into vector tiles for zoom
levels 8-12 using Shapely for clipping and ``mapbox-vector-tile`` for MVT
encoding.  The resulting MBTiles files are stored alongside the exports.
"""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import mercantile
from mapbox_vector_tile import encode as encode_mvt
from shapely.geometry import box, shape, mapping
from shapely.geometry.base import BaseGeometry


PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPORT_DIR = PROJECT_ROOT / "pmtiles" / "exports"
MBTILES_DIR = PROJECT_ROOT / "pmtiles" / "mbtiles"


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    geojson_name: str
    mbtiles_name: str
    min_zoom: int = 8
    max_zoom: int = 12


DATASETS: tuple[DatasetConfig, ...] = (
    DatasetConfig("red_light_locations", "red_light_ward_choropleth.geojson", "red_light_ward_choropleth.mbtiles"),
    DatasetConfig("ase_locations", "ase_ward_choropleth.geojson", "ase_ward_choropleth.mbtiles"),
    DatasetConfig("cameras_combined", "cameras_combined_ward_choropleth.geojson", "cameras_combined_ward_choropleth.mbtiles"),
)


def load_geojson(path: Path) -> list[tuple[BaseGeometry, dict]]:
    payload = json.loads(path.read_text())
    features: list[tuple[BaseGeometry, dict]] = []
    for feature in payload.get("features", []):
        geometry = feature.get("geometry")
        props = feature.get("properties") or {}
        if not geometry:
            continue
        geom = shape(geometry)
        if geom.is_empty:
            continue
        features.append((geom, props))
    return features


def simplify_geometry(geom: BaseGeometry, zoom: int) -> BaseGeometry:
    # Rough heuristic: drop detail progressively for lower zooms.
    tolerance = 0.0005 * (2 ** max(0, 10 - zoom))
    simplified = geom.simplify(tolerance, preserve_topology=True)
    return simplified if not simplified.is_empty else geom


def encode_tile(features: list[tuple[BaseGeometry, dict]], bounds: mercantile.LngLatBbox, zoom: int) -> bytes | None:
    tile_bbox = box(bounds.west, bounds.south, bounds.east, bounds.north)
    layer_features = []
    for geom, props in features:
        if not geom.intersects(tile_bbox):
            continue
        clipped = geom.intersection(tile_bbox)
        if clipped.is_empty:
            continue
        simplified = simplify_geometry(clipped, zoom)
        if simplified.is_empty:
            continue
        layer_features.append({
            "geometry": mapping(simplified),
            "properties": {
                "wardCode": props.get("wardCode"),
                "ticketCount": props.get("ticketCount", 0),
                "totalRevenue": props.get("totalRevenue", 0.0),
            },
        })
    if not layer_features:
        return None
    layer = {
        "name": "ward_polygons",
        "features": layer_features,
        "extent": 4096,
        "version": 2,
    }
    default_options = {
        "quantize_bounds": (bounds.west, bounds.south, bounds.east, bounds.north),
        "y_coord_down": True,
    }
    return encode_mvt([layer], default_options=default_options)


def ensure_metadata(conn: sqlite3.Connection, dataset: DatasetConfig, bounds: mercantile.LngLatBbox) -> None:
    metadata = {
        "name": dataset.key,
        "type": "overlay",
        "version": "1.1",
        "description": f"Ward choropleth tiles for {dataset.key}",
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
    }
    conn.executemany("INSERT INTO metadata (name, value) VALUES (?, ?)", metadata.items())


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


def build_mbtiles(dataset: DatasetConfig) -> Path:
    geojson_path = EXPORT_DIR / dataset.geojson_name
    if not geojson_path.exists():
        raise FileNotFoundError(f"Missing GeoJSON export: {geojson_path}")

    features = load_geojson(geojson_path)
    if not features:
        raise RuntimeError(f"No features found in {geojson_path}")

    bounds: mercantile.LngLatBbox | None = None
    total_bounds = None
    for geom, _ in features:
        geom_bounds = geom.bounds
        if total_bounds is None:
            total_bounds = geom_bounds
        else:
            total_bounds = (
                min(total_bounds[0], geom_bounds[0]),
                min(total_bounds[1], geom_bounds[1]),
                max(total_bounds[2], geom_bounds[2]),
                max(total_bounds[3], geom_bounds[3]),
            )
    if total_bounds is None:
        raise RuntimeError("Unable to derive bounds for features")
    bounds = mercantile.LngLatBbox(*total_bounds)

    MBTILES_DIR.mkdir(parents=True, exist_ok=True)
    mbtiles_path = MBTILES_DIR / dataset.mbtiles_name
    conn = init_mbtiles(mbtiles_path)

    ensure_metadata(conn, dataset, bounds)

    tile_count = 0
    for zoom in range(dataset.min_zoom, dataset.max_zoom + 1):
        tiles = mercantile.tiles(bounds.west, bounds.south, bounds.east, bounds.north, [zoom])
        for tile in tiles:
            tile_bounds = mercantile.bounds(tile)
            tile_data = encode_tile(features, tile_bounds, zoom)
            if not tile_data:
                continue
            tms_y = (2 ** zoom - 1) - tile.y
            conn.execute(
                "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                (zoom, tile.x, tms_y, sqlite3.Binary(tile_data)),
            )
            tile_count += 1

    conn.commit()
    conn.close()
    print(f"wrote {mbtiles_path.relative_to(PROJECT_ROOT)} ({tile_count} tiles)")
    return mbtiles_path


def run(datasets: Iterable[DatasetConfig]) -> None:
    for dataset in datasets:
        build_mbtiles(dataset)


if __name__ == "__main__":
    run(DATASETS)
