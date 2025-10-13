"""Convert MBTiles outputs into PMTiles archives."""

from __future__ import annotations

import gzip
import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import write as pmtiles_write


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MBTILES_DIR = PROJECT_ROOT / "pmtiles" / "mbtiles"
PMTILES_DIR = PROJECT_ROOT / "pmtiles" / "artifacts"


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    mbtiles_name: str
    pmtiles_name: str


DATASETS: tuple[DatasetConfig, ...] = (
    DatasetConfig("red_light_locations", "red_light_ward_choropleth.mbtiles", "red_light_ward_choropleth.pmtiles"),
    DatasetConfig("ase_locations", "ase_ward_choropleth.mbtiles", "ase_ward_choropleth.pmtiles"),
    DatasetConfig("cameras_combined", "cameras_combined_ward_choropleth.mbtiles", "cameras_combined_ward_choropleth.pmtiles"),
    DatasetConfig("parking_tickets_glow", "parking_glow_lines.mbtiles", "parking_glow_lines.pmtiles"),
    DatasetConfig("red_light_glow", "red_light_glow_lines.mbtiles", "red_light_glow_lines.pmtiles"),
    DatasetConfig("ase_glow", "ase_glow_lines.mbtiles", "ase_glow_lines.pmtiles"),
)


DEFAULT_VECTOR_LAYER_FIELDS = {
    "glow_lines": {
        "centreline_id": "Number",
        "count": "Number",
        "years_mask": "Number",
        "months_mask": "Number",
    },
    "ward_polygons": {
        "wardCode": "Number",
        "ticketCount": "Number",
        "totalRevenue": "Number",
    },
}


def _default_layer_id(dataset_key: str) -> str:
    return "glow_lines" if dataset_key.endswith("_glow") else "ward_polygons"


def _sanitize_fields(raw_fields, fallback_id: str) -> dict:
    if isinstance(raw_fields, dict) and raw_fields:
        sanitized = {}
        for key, value in raw_fields.items():
            if not isinstance(key, str):
                continue
            sanitized[key] = value if isinstance(value, str) else str(value)
        if sanitized:
            return sanitized
    fallback = DEFAULT_VECTOR_LAYER_FIELDS.get(fallback_id, {})
    return dict(fallback)


def parse_metadata(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT name, value FROM metadata").fetchall()
    return {name: value for name, value in rows}


def build_header(meta: dict[str, str]) -> dict[str, int]:
    bounds_str = meta.get("bounds", "-180,-85,180,85")
    bounds_parts = [float(part) for part in bounds_str.split(",")]
    if len(bounds_parts) != 4:
        bounds_parts = [-180.0, -85.0, 180.0, 85.0]
    west, south, east, north = bounds_parts

    center_str = meta.get("center")
    if center_str:
        center_parts = center_str.split(",")
    else:
        center_parts = [str((west + east) / 2), str((south + north) / 2), "10"]
    while len(center_parts) < 3:
        center_parts.append("10")
    center_lng = float(center_parts[0])
    center_lat = float(center_parts[1])
    center_zoom = int(float(center_parts[2]))

    header = {
        "tile_type": TileType.MVT,
        "tile_compression": Compression.GZIP,
        "min_lon_e7": int(west * 10_000_000),
        "min_lat_e7": int(south * 10_000_000),
        "max_lon_e7": int(east * 10_000_000),
        "max_lat_e7": int(north * 10_000_000),
        "center_lon_e7": int(center_lng * 10_000_000),
        "center_lat_e7": int(center_lat * 10_000_000),
        "center_zoom": center_zoom,
    }
    return header


def build_metadata(dataset: DatasetConfig, meta: dict[str, str]) -> dict:
    json_blob = meta.get("json")
    extra: dict | None = None
    if json_blob:
        try:
            extra = json.loads(json_blob)
        except json.JSONDecodeError:
            extra = None

    name = meta.get("name", dataset.key)
    description = meta.get("description", f"Vector tiles for {dataset.key}")
    minzoom = int(float(meta.get("minzoom", 8)))
    maxzoom = int(float(meta.get("maxzoom", 12)))
    bounds = meta.get("bounds", "-180,-85,180,85")
    center = meta.get("center")
    year_base = None
    vector_layers_meta: list[dict] = []

    if isinstance(extra, dict):
        year_base = extra.get("year_base")
        if extra.get("description"):
            description = extra["description"]
        raw_layers = extra.get("vector_layers")
        if isinstance(raw_layers, list):
            for entry in raw_layers:
                if not isinstance(entry, dict):
                    continue
                layer_id = str(entry.get("id") or _default_layer_id(dataset.key))
                entry_minzoom = entry.get("minzoom")
                entry_maxzoom = entry.get("maxzoom")
                vector_layers_meta.append(
                    {
                        "id": layer_id,
                        "description": (entry.get("description") or layer_id.replace("_", " ").title()),
                        "minzoom": int(float(entry_minzoom if entry_minzoom is not None else minzoom)),
                        "maxzoom": int(float(entry_maxzoom if entry_maxzoom is not None else maxzoom)),
                        "fields": _sanitize_fields(entry.get("fields"), layer_id),
                    }
                )

    if not vector_layers_meta:
        fallback_id = _default_layer_id(dataset.key)
        vector_layers_meta.append(
            {
                "id": fallback_id,
                "description": fallback_id.replace("_", " ").title(),
                "minzoom": minzoom,
                "maxzoom": maxzoom,
                "fields": dict(DEFAULT_VECTOR_LAYER_FIELDS.get(fallback_id, {})),
            }
        )

    metadata = {
        "name": name,
        "description": description,
        "version": meta.get("version", "1.0.0"),
        "type": meta.get("type", "overlay"),
        "format": "pbf",
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "bounds": bounds,
        "center": center,
        "vector_layers": vector_layers_meta,
    }

    if year_base is not None:
        metadata["year_base"] = year_base

    return metadata


def convert_dataset(dataset: DatasetConfig) -> Path:
    mbtiles_path = MBTILES_DIR / dataset.mbtiles_name
    if not mbtiles_path.exists():
        raise FileNotFoundError(f"Missing MBTiles artifact: {mbtiles_path}")

    PMTILES_DIR.mkdir(parents=True, exist_ok=True)
    pmtiles_path = PMTILES_DIR / dataset.pmtiles_name

    with sqlite3.connect(mbtiles_path) as conn:
        meta = parse_metadata(conn)
        header = build_header(meta)
        metadata = build_metadata(dataset, meta)

        with pmtiles_write(pmtiles_path) as writer:
            cur = conn.execute("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles")
            tile_count = 0
            for zoom, column, row, tile_data in cur:
                # Convert TMS row back to XYZ schema used by PMTiles.
                xyz_row = (2 ** zoom - 1) - row
                tileid = zxy_to_tileid(zoom, column, xyz_row)
                compressed = gzip.compress(tile_data)
                writer.write_tile(tileid, compressed)
                tile_count += 1

            writer.finalize(header, metadata)

    print(f"wrote {pmtiles_path.relative_to(PROJECT_ROOT)}")
    return pmtiles_path


def run() -> None:
    for dataset in DATASETS:
        convert_dataset(dataset)


if __name__ == "__main__":
    run()
