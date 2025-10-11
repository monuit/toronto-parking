"""Standalone geocoder for existing parking ticket records.

This script looks for tickets missing `street_normalized` or `geom`, applies the
same geocoding logic as the ETL pipeline, and updates the table in-place.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import dotenv
import pandas as pd
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from geocoding.centreline_geocoder import CentrelineGeocoder, GeocodeResult


DEFAULT_BATCH_SIZE = 10_000
LOCATION_LOOKUP_ENV = "PARKING_TICKETS_LOCATION_LOOKUP"
SKIP_GEOCODER_ENV = "PARKING_TICKETS_SKIP_GEOCODE"


def load_env() -> None:
    dotenv.load_dotenv(REPO_ROOT / ".env")


def resolve_dsn(cli_dsn: Optional[str]) -> str:
    if cli_dsn:
        return cli_dsn
    for key in ("POSTGIS_DATABASE_URL", "DATABASE_URL", "POSTGRES_URL"):
        value = os.getenv(key)
        if value:
            return value
    raise RuntimeError("Database URL not provided; set --database-url or DATABASE_URL")


def normalize_location(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text or text.lower() == "nan":
        return None
    return text


def load_location_lookup() -> Dict[str, Dict[str, float | None]]:
    lookup: Dict[str, Dict[str, float | None]] = {}
    env_path = os.getenv(LOCATION_LOOKUP_ENV)
    if env_path:
        path = Path(env_path)
    else:
        repo_root = Path(__file__).resolve().parents[1]
        path = repo_root / "map-app" / "public" / "data" / "tickets_aggregated.geojson"
    if not path.exists():
        return lookup
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return lookup
    for feature in payload.get("features", []):
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coords = geometry.get("coordinates") if isinstance(geometry, dict) else None
        location = normalize_location(properties.get("location"))
        if not location:
            continue
        if not isinstance(coords, (list, tuple)) or len(coords) < 2:
            continue
        longitude = coords[0]
        latitude = coords[1]
        if longitude is None or latitude is None:
            continue
        lookup[location] = {
            "street_normalized": location,
            "latitude": float(latitude),
            "longitude": float(longitude),
            "centreline_id": None,
        }
    return lookup


@dataclass
class GeocodeResultPayload:
    street_normalized: Optional[str]
    centreline_id: Optional[int]
    latitude: Optional[float]
    longitude: Optional[float]


class TicketGeocoder:
    def __init__(self, conn: psycopg.Connection, *, skip_geocoder: bool = False) -> None:
        self._conn = conn
        self._skip_geocoder = skip_geocoder
        self._location_lookup = load_location_lookup()
        self._geocode_cache: Dict[Tuple[str, str, str, str], Optional[GeocodeResultPayload]] = {}
        self._geocoder: Optional[CentrelineGeocoder] = None

    def geocode_row(self, record: Dict[str, Optional[str]]) -> Optional[GeocodeResultPayload]:
        key = tuple((record.get(f"location{i}") or "").strip().upper() for i in range(1, 5))
        cached = self._geocode_cache.get(key)
        if cached is not None:
            return cached

        lookup_result = self._lookup_precomputed_location(record)
        if lookup_result is not None:
            payload = GeocodeResultPayload(
                street_normalized=lookup_result.get("street_normalized"),
                centreline_id=lookup_result.get("centreline_id"),
                latitude=lookup_result.get("latitude"),
                longitude=lookup_result.get("longitude"),
            )
            self._geocode_cache[key] = payload
            return payload

        if self._skip_geocoder:
            self._geocode_cache[key] = None
            return None

        address = " ".join(part for part in key if part).strip()
        if not address:
            self._geocode_cache[key] = None
            return None

        geocoder = self._get_geocoder()
        try:
            result = geocoder.geocode(address)
        except Exception:
            result = None

        if isinstance(result, GeocodeResult):
            payload = GeocodeResultPayload(
                street_normalized=result.street_normalized,
                centreline_id=result.centreline_id,
                latitude=result.latitude,
                longitude=result.longitude,
            )
        else:
            payload = None

        self._geocode_cache[key] = payload
        return payload

    def _lookup_precomputed_location(self, record: Dict[str, Optional[str]]) -> Optional[Dict[str, float | None]]:
        candidates = [
            normalize_location(record.get("location2")),
            normalize_location(record.get("location1")),
        ]
        for candidate in candidates:
            if candidate and candidate in self._location_lookup:
                return self._location_lookup[candidate]
        return None

    def _get_geocoder(self) -> CentrelineGeocoder:
        if self._geocoder is not None:
            return self._geocoder
        df = pd.read_sql_query(
            """
            SELECT
                centreline_id AS "CENTRELINE_ID",
                linear_name AS "LINEAR_NAME",
                linear_name_type AS "LINEAR_NAME_TYPE",
                linear_name_dir AS "LINEAR_NAME_DIR",
                linear_name_full AS "LINEAR_NAME_FULL",
                linear_name_label AS "LINEAR_NAME_LABEL",
                parity_left AS "PARITY_L",
                parity_right AS "PARITY_R",
                low_num_even AS "LOW_NUM_EVEN",
                high_num_even AS "HIGH_NUM_EVEN",
                low_num_odd AS "LOW_NUM_ODD",
                high_num_odd AS "HIGH_NUM_ODD",
                feature_code AS "FEATURE_CODE",
                feature_code_desc AS "FEATURE_CODE_DESC",
                jurisdiction AS "JURISDICTION",
                ST_AsGeoJSON(geom) AS "geometry"
            FROM centreline_segments
            """,
            self._conn,
        )
        self._geocoder = CentrelineGeocoder(df)
        return self._geocoder


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill parking ticket geocoding")
    parser.add_argument("--database-url", dest="database_url", help="Postgres connection string override")
    parser.add_argument("--limit", type=int, default=None, help="Maximum rows to process")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows to fetch per pass")
    parser.add_argument("--skip-geocoder", action="store_true", help="Only use precomputed lookup (no fuzzy geocoder)")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist updates, only report")
    return parser.parse_args()


def main() -> None:
    load_env()
    os.environ[SKIP_GEOCODER_ENV] = "0"
    os.environ["PARKING_TICKETS_DISABLE_GEOCODER"] = "0"
    args = parse_args()
    dsn = resolve_dsn(args.database_url)

    with psycopg.connect(dsn) as conn:
        conn.autocommit = False
        geocoder = TicketGeocoder(conn, skip_geocoder=args.skip_geocoder or os.getenv(SKIP_GEOCODER_ENV, "0").lower() in {"1", "true", "yes"})

        total_processed = 0
        total_updated = 0

        while True:
            remaining = args.limit - total_processed if args.limit is not None else args.batch_size
            if remaining <= 0:
                break
            chunk_size = min(args.batch_size, remaining) if args.limit is not None else args.batch_size
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ticket_hash, location1, location2, location3, location4
                    FROM parking_tickets
                    WHERE street_normalized IS NULL OR geom IS NULL
                    ORDER BY date_of_infraction NULLS LAST, ticket_hash
                    LIMIT %s OFFSET %s
                    """,
                    (chunk_size, total_processed),
                )
                rows = cur.fetchall()

            if not rows:
                break

            updates = []
            for ticket_hash, loc1, loc2, loc3, loc4 in rows:
                record = {
                    "ticket_hash": ticket_hash,
                    "location1": loc1,
                    "location2": loc2,
                    "location3": loc3,
                    "location4": loc4,
                }
                payload = geocoder.geocode_row(record)
                if payload is None:
                    continue
                updates.append(
                    (
                        payload.street_normalized,
                        payload.centreline_id,
                        payload.longitude,
                        payload.latitude,
                        payload.longitude,
                        payload.latitude,
                        ticket_hash,
                    )
                )

            total_processed += len(rows)

            if not updates:
                conn.rollback()
                print(f"Processed {total_processed} rows; no geocodable updates found in this batch.")
                if len(rows) < chunk_size:
                    break
                continue

            if args.dry_run:
                conn.rollback()
                total_updated += len(updates)
                print(f"[dry-run] Would update {len(updates)} tickets (processed {total_processed}).")
                if len(rows) < chunk_size:
                    break
                continue

            with conn.cursor() as cur:
                cur.executemany(
                    """
                    UPDATE parking_tickets
                    SET
                        street_normalized = COALESCE(%s, street_normalized),
                        centreline_id = COALESCE(%s, centreline_id),
                        geom = CASE
                            WHEN %s IS NOT NULL AND %s IS NOT NULL THEN ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                            ELSE geom
                        END,
                        updated_at = NOW()
                    WHERE ticket_hash = %s
                    """,
                    updates,
                )
            conn.commit()
            total_updated += len(updates)
            print(f"Updated {len(updates)} tickets (processed {total_processed}).")

            if len(rows) < chunk_size:
                break

        print(f"Geocoding complete. Processed {total_processed} rows, updated {total_updated}.")


if __name__ == "__main__":
    main()
