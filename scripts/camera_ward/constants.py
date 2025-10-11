"""Shared constants and paths used by camera ward dataset builders."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

WARD_GEOJSON_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/5e7a8234-f805-43ac-820f-03d7c360b588/resource/"
    "737b29e0-8329-4260-b6af-21555ab24f28/download/city-wards-data-4326.geojson"
)

OUTPUT_DIR = REPO_ROOT / "map-app" / "public" / "data"
WARD_CACHE_PATH = REPO_ROOT / "output" / "etl" / "static" / "city_wards.geojson"
STATE_FILE = REPO_ROOT / "output" / "etl" / "camera_ward_state.json"
STATE_VERSION = 2

SUMMARY_PATHS = {
    "ase_locations": OUTPUT_DIR / "ase_ward_summary.json",
    "red_light_locations": OUTPUT_DIR / "red_light_ward_summary.json",
    "cameras_combined": OUTPUT_DIR / "cameras_combined_ward_summary.json",
}

GEOJSON_PATHS = {
    "ase_locations": OUTPUT_DIR / "ase_ward_choropleth.geojson",
    "red_light_locations": OUTPUT_DIR / "red_light_ward_choropleth.geojson",
    "cameras_combined": OUTPUT_DIR / "cameras_combined_ward_choropleth.geojson",
}

REDIS_KEYS = {
    "ase_locations": {
        "geojson": "toronto:map-data:ase:wards:geojson:v1",
        "summary": "toronto:map-data:ase:wards:summary:v1",
    },
    "red_light_locations": {
        "geojson": "toronto:map-data:red_light:wards:geojson:v1",
        "summary": "toronto:map-data:red_light:wards:summary:v1",
    },
    "cameras_combined": {
        "geojson": "toronto:map-data:cameras:wards:geojson:v1",
        "summary": "toronto:map-data:cameras:wards:summary:v1",
    },
}
