# Dataset Integration Status

## Automated ETL Sources

| Dataset | Resource Type | Raw Location | Processing Notes |
| --- | --- | --- | --- |
| Parking tickets (2008–2024) | CKAN ZIP archives (multiple CSVs) | Downloaded automatically into `output/etl/raw/parking_tickets/` during ETL runs | Loader extracts ZIPs, normalizes dates/times, geocodes against `centreline_segments`, and stores records in PostGIS table `parking_tickets`. Aggregated GeoJSON lives at `map-app/public/data/tickets_aggregated.geojson` and is pushed to Redis via `scripts/push_tickets_to_redis.py`. |
| Red light cameras | CKAN CSV (locations) + XLSX (annual charges) | `output/etl/raw/red_light_locations/` | Loader merges the CSV location data with the XLSX charge totals (code offset handled) and writes enriched records to `red_light_camera_locations` (PostGIS). |
| Automated Speed Enforcement (ASE) | CKAN CSV (locations) + XLSX (monthly charges) | `output/etl/raw/ase_locations/` | Loader joins location and charge workbooks by site code, producing aggregated metrics stored in `ase_camera_locations` (PostGIS). |

### Storage Targets

- **PostGIS tables**
  - `centreline_segments`
  - `parking_tickets`
  - `red_light_camera_locations`
  - `ase_camera_locations`

- **Redis keys**
  - `toronto:map-data:tickets:aggregated:v1` – gzipped/base64 GeoJSON for pre-aggregated parking ticket points. TTL ~24h.
  - Tile cache keys (e.g., `toronto:tiles:tiles:red_light_locations:13:2290:2989`) are created lazily by `/api/tiles` and expire per `REDIS_DEFAULT_TTL`.

## Tile Generation & UI Integration

- Vector tiles for **red_light_locations** and **ase_locations** are generated on demand by `api/tiles.py`, pulling directly from PostGIS (`src/tiles/service.py`).
- Redis caching for tiles remains automatic—no manual pre-warm required. The first request stores the tile binary using the dataset/z/x/y composite key.
- Map UI components (`PointsLayer`, `StatsSummary`, `DatasetToggle`, etc.) already consume the dataset parameter and parse Postgres-style array/JSON fields, so red-light and ASE data render with the same tooling as parking tickets.
- Parking ticket points are still driven by the aggregated GeoJSON (now cached in Redis); red-light and ASE use live PostGIS-backed tiles.

## Remaining Actions

1. Issue tile/summary requests in the deployed environment to warm Redis tile caches if desired (optional because caching is lazy).
2. Continue running the parking-ticket batch uploader (`scripts/run_parking_tickets_batches.py`) during low-traffic windows when full historical reloads are needed.
