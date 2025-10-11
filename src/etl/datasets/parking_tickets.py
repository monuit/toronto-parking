
"""ETL pipeline for Toronto parking ticket records with geocoding."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd
import logging

from geocoding.centreline_geocoder import CentrelineGeocoder, GeocodeResult

from ..state import DatasetState
from ..utils import sha1sum
from .base import DatasetETL, ExtractionResult

STAGING_COLUMNS = (
    "ticket_hash",
    "ticket_number",
    "date_of_infraction",
    "time_of_infraction",
    "infraction_code",
    "infraction_description",
    "set_fine_amount",
    "location1",
    "location2",
    "location3",
    "location4",
    "street_normalized",
    "centreline_id",
    "latitude",
    "longitude",
)

_GEOCODE_MISS = object()


def build_ticket_hash(
    ticket_number: Optional[str],
    parsed_date: Optional[str],
    time_value: Optional[str],
    infraction_code: Optional[str],
    infraction_description: Optional[str],
    set_fine: Optional[str],
    location1: Optional[str],
    location2: Optional[str],
    location3: Optional[str],
    location4: Optional[str],
) -> str:
    components = (
        ticket_number or "",
        parsed_date or "",
        time_value or "",
        infraction_code or "",
        infraction_description or "",
        set_fine or "",
        location1 or "",
        location2 or "",
        location3 or "",
        location4 or "",
    )
    raw = "|".join(components)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _safe_decimal(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        from decimal import Decimal

        return str(Decimal(str(value)))
    except Exception:
        return None


def _normalise_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _normalise_time(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if len(value) == 4 and value.isdigit():
        return f"{value[:2]}:{value[2:]}"
    return value


logger = logging.getLogger(__name__)


class ParkingTicketsETL(DatasetETL):
    """Loader for the full Toronto parking ticket archive."""

    COPY_PROGRESS_INTERVAL = max(1, int(os.getenv("PARKING_TICKETS_COPY_PROGRESS", "50000")))

    def __init__(self, config, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self._geocoder: Optional[CentrelineGeocoder] = None
        self._geocode_cache: Dict[Tuple[str, str, str, str], Any] = {}
        self._location_lookup = self._load_location_lookup()
        disable_geo_env = os.getenv("PARKING_TICKETS_DISABLE_GEOCODER")
        if disable_geo_env is None:
            disable_geo_env = os.getenv("PARKING_TICKETS_SKIP_GEOCODE", "0")
        self._disable_geocoder = disable_geo_env.lower() in {"1", "true", "yes"}
        years_env = os.getenv("PARKING_TICKETS_YEARS")
        if years_env:
            parsed: List[int] = []
            for token in years_env.split(","):
                token = token.strip()
                if token.isdigit():
                    parsed.append(int(token))
            self._year_filter = set(parsed) if parsed else None
        else:
            self._year_filter = None

    def extract(self, state: DatasetState) -> ExtractionResult | None:
        previous_resources = ((state.metadata or {}).get("resources", {}) if state else {})
        resource_paths: Dict[str, Path] = {}
        resource_hashes: Dict[str, str] = {}
        resource_metadata: Dict[str, Dict[str, Any]] = {}
        has_changes = False

        for name, resource_cfg in self.config.resources.items():
            resource_info = self.get_package_resource(resource_cfg)
            year = self._extract_year(name, resource_info.get("name"))
            if self._year_filter is not None and year not in self._year_filter:
                continue

            suffix = self.infer_suffix(resource_info, resource_cfg)
            path = self.store.raw_path(self.config.slug, resource_cfg.resource_id, suffix)
            manifest_entry = previous_resources.get(name, {})
            last_modified = resource_info.get("last_modified")

            changed = False
            if last_modified and manifest_entry.get("last_modified") == last_modified and path.exists():
                sha1 = manifest_entry.get("sha1") or sha1sum(path)
            else:
                path = self.download_resource(resource_cfg, suffix=suffix)
                sha1 = sha1sum(path)
                changed = True

            if not changed and manifest_entry.get("sha1") and manifest_entry.get("sha1") != sha1:
                changed = True

            resource_metadata[name] = {
                "resource_id": resource_cfg.resource_id,
                "last_modified": last_modified,
                "format": resource_info.get("format"),
                "sha1": sha1,
                "year": year,
            }
            resource_hashes[name] = sha1

            if changed:
                resource_paths[name] = path
                has_changes = True

        if not has_changes:
            return None

        return ExtractionResult(
            resource_paths=resource_paths,
            resource_hashes=resource_hashes,
            resource_metadata=resource_metadata,
        )

    def transform(self, extraction: ExtractionResult, state: DatasetState) -> Dict[str, Any]:
        resources: List[Dict[str, Any]] = []
        for name, path in extraction.resource_paths.items():
            meta = extraction.resource_metadata.get(name, {})
            resources.append(
                {
                    "name": name,
                    "path": path,
                    "year": meta.get("year"),
                }
            )
        return {
            "resources": resources,
            "resource_metadata": extraction.resource_metadata,
            "resource_hashes": extraction.resource_hashes,
        }

    def load(self, payload: Dict[str, Any], state: DatasetState) -> None:
        resources: List[Dict[str, Any]] = payload.get("resources", [])
        if not resources:
            return

        self._ensure_tables()
        self.pg.ensure_extensions()

        existing_metadata = (state.metadata or {}).get("resources", {}) if state else {}
        updated_metadata: Dict[str, Dict[str, Any]] = {}
        total_rows = 0

        for descriptor in resources:
            name = descriptor["name"]
            path: Path = descriptor["path"]
            year = descriptor.get("year")
            rows_written = self._load_resource_year(year, path)
            total_rows += rows_written
            logger.info("[parking_tickets] loaded %s rows for year %s", rows_written, year)

            meta = dict(payload["resource_metadata"].get(name, {}))
            meta["row_count"] = rows_written
            meta["year"] = year
            updated_metadata[name] = meta

        for key, value in existing_metadata.items():
            if key not in updated_metadata:
                updated_metadata[key] = value

        metadata = {
            "row_count": total_rows,
            "resources": updated_metadata,
        }

        self.state_store.upsert(
            self.config.slug,
            last_synced_at=datetime.utcnow(),
            last_resource_hash="|".join(sorted(payload["resource_hashes"].values())),
            metadata=metadata,
        )

    # Internal helpers -------------------------------------------------
    def _extract_year(self, resource_name: str, fallback: Optional[str]) -> Optional[int]:
        candidates = [resource_name, fallback or ""]
        for candidate in candidates:
            if not candidate:
                continue
            for token in candidate.replace("-", "_").split("_"):
                if token.isdigit() and len(token) == 4:
                    return int(token)
        return None

    def _load_resource_year(self, year: Optional[int], archive_path: Path) -> int:
        if year is None:
            raise RuntimeError("Unable to determine year for parking tickets resource")

        start = date(year, 1, 1)
        end = date(year + 1, 1, 1)
        self._geocode_cache.clear()

        rows_written = 0
        column_list = ",".join(STAGING_COLUMNS)
        copy_sql = f"COPY parking_tickets_staging ({column_list}) FROM STDIN WITH (FORMAT text)"

        with self.pg.connect() as conn:
            conn.execute("SET LOCAL synchronous_commit TO OFF")
            conn.execute(
                "DELETE FROM parking_tickets WHERE date_of_infraction >= %s AND date_of_infraction < %s",
                (start.isoformat(), end.isoformat()),
            )
            conn.execute("TRUNCATE parking_tickets_staging")

            with conn.cursor().copy(copy_sql) as copy:
                for row in self._iter_archive_rows(archive_path):
                    copy.write_row(row)
                    rows_written += 1
                    if rows_written % self.COPY_PROGRESS_INTERVAL == 0:
                        logger.info(
                            "[parking_tickets] streamed %s rows for year %s", rows_written, year,
                        )

            conn.execute(
                """
                INSERT INTO parking_tickets AS target (
                    ticket_hash,
                    ticket_number,
                    date_of_infraction,
                    time_of_infraction,
                    infraction_code,
                    infraction_description,
                    set_fine_amount,
                    location1,
                    location2,
                    location3,
                    location4,
                    street_normalized,
                    centreline_id,
                    geom
                )
                SELECT DISTINCT ON (ticket_hash)
                    ticket_hash,
                    ticket_number,
                    NULLIF(date_of_infraction, '')::DATE,
                    time_of_infraction,
                    infraction_code,
                    infraction_description,
                    NULLIF(set_fine_amount, '')::NUMERIC,
                    location1,
                    location2,
                    location3,
                    location4,
                    street_normalized,
                    centreline_id,
                    CASE
                        WHEN longitude IS NULL OR latitude IS NULL THEN NULL
                        ELSE ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
                    END
                FROM parking_tickets_staging
                ORDER BY ticket_hash, NULLIF(date_of_infraction, '')::DATE DESC, time_of_infraction DESC
                ON CONFLICT (ticket_hash) DO UPDATE SET
                    ticket_number = EXCLUDED.ticket_number,
                    date_of_infraction = EXCLUDED.date_of_infraction,
                    time_of_infraction = EXCLUDED.time_of_infraction,
                    infraction_code = EXCLUDED.infraction_code,
                    infraction_description = EXCLUDED.infraction_description,
                    set_fine_amount = EXCLUDED.set_fine_amount,
                    location1 = EXCLUDED.location1,
                    location2 = EXCLUDED.location2,
                    location3 = EXCLUDED.location3,
                    location4 = EXCLUDED.location4,
                    street_normalized = COALESCE(EXCLUDED.street_normalized, target.street_normalized),
                    centreline_id = COALESCE(EXCLUDED.centreline_id, target.centreline_id),
                    geom = COALESCE(EXCLUDED.geom, target.geom),
                    updated_at = NOW()
                """
            )
            conn.execute("TRUNCATE parking_tickets_staging")
            conn.commit()

        return rows_written

    def _iter_archive_rows(self, archive_path: Path) -> Iterator[Tuple[Any, ...]]:
        with zipfile.ZipFile(archive_path) as archive:
            csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))
            for member in sorted(archive.namelist()):
                name_lower = member.lower()
                if member.endswith("/"):
                    continue
                if "parking_tags" not in name_lower:
                    continue
                encoding = self._detect_member_encoding(archive, member)
                with archive.open(member) as handle:
                    wrapper = io.TextIOWrapper(
                        handle,
                        encoding=encoding,
                        errors="ignore",
                        newline="",
                    )
                    reader = csv.DictReader(wrapper)
                    for record in reader:
                        prepared = self._prepare_row(record)
                        if prepared is not None:
                            yield prepared

    @staticmethod
    def _detect_member_encoding(archive: zipfile.ZipFile, member: str) -> str:
        try:
            with archive.open(member) as sample:
                prefix = sample.read(4)
        except KeyError:
            return "utf-8"

        if prefix.startswith(b"\xff\xfe"):
            return "utf-16-le"
        if prefix.startswith(b"\xfe\xff"):
            return "utf-16-be"
        if prefix.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"
        if b"\x00" in prefix:
            return "utf-16-le"
        return "utf-8"

    def _prepare_row(self, record: Dict[str, Any]) -> Optional[Tuple[Any, ...]]:
        if record:
            record = {
                (key.lstrip("\ufeff") if isinstance(key, str) else key): value
                for key, value in record.items()
            }
        ticket_number = (
            record.get("ticket_number")
            or record.get("tag_number_masked")
            or record.get("tagnumbermasked")
        )
        if not ticket_number:
            return None

        parsed_date = _normalise_date(
            record.get("date_of_infraction")
            or record.get("dateofinfraction")
        )
        if not parsed_date:
            return None

        time_value = _normalise_time(
            record.get("time_of_infraction") or record.get("timeofinfraction")
        )
        geocode_result = self._geocode_record(record)

        set_fine = _safe_decimal(record.get("set_fine_amount") or record.get("setfineamount"))
        infraction_code = record.get("infraction_code") or record.get("infractioncode")
        infraction_description = (
            record.get("infraction_description") or record.get("infractiondescription")
        )
        location1 = record.get("location1")
        location2 = record.get("location2")
        location3 = record.get("location3")
        location4 = record.get("location4")

        ticket_hash = build_ticket_hash(
            ticket_number,
            parsed_date,
            time_value,
            infraction_code,
            infraction_description,
            set_fine,
            location1,
            location2,
            location3,
            location4,
        )

        street_normalized: Optional[str] = None
        centreline_id: Optional[int] = None
        latitude: Optional[float] = None
        longitude: Optional[float] = None

        if isinstance(geocode_result, GeocodeResult):
            street_normalized = geocode_result.street_normalized
            centreline_id = geocode_result.centreline_id
            latitude = geocode_result.latitude
            longitude = geocode_result.longitude
        elif isinstance(geocode_result, dict):
            street_normalized = geocode_result.get("street_normalized")
            centreline_id = geocode_result.get("centreline_id")
            latitude = geocode_result.get("latitude")
            longitude = geocode_result.get("longitude")

        return (
            ticket_hash,
            ticket_number,
            parsed_date,
            time_value,
            infraction_code,
            infraction_description,
            set_fine,
            location1,
            location2,
            location3,
            location4,
            street_normalized,
            centreline_id,
            latitude,
            longitude,
        )

    def _geocode_record(self, record: Dict[str, Any]) -> Optional[Any]:
        address_parts = [
            record.get("location1"),
            record.get("location2"),
            record.get("location3"),
            record.get("location4"),
        ]
        key = tuple((part or "").strip().upper() for part in address_parts)
        cached = self._geocode_cache.get(key)
        if cached is _GEOCODE_MISS:
            return None
        if isinstance(cached, (GeocodeResult, dict)):
            return cached

        lookup_result = self._lookup_precomputed_location(record)
        if lookup_result is not None:
            self._geocode_cache[key] = lookup_result
            return lookup_result

        if self._disable_geocoder:
            self._geocode_cache[key] = _GEOCODE_MISS
            return None

        address = " ".join(part for part in key if part)
        if not address:
            self._geocode_cache[key] = _GEOCODE_MISS
            return None

        geocoder = self._get_geocoder()
        try:
            result = geocoder.geocode(address)
        except Exception:
            result = None
        self._geocode_cache[key] = result if result is not None else _GEOCODE_MISS
        return result

    def _get_geocoder(self) -> CentrelineGeocoder:
        if self._geocoder is not None:
            return self._geocoder
        with self.pg.connect() as conn:
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
                conn,
            )
        self._geocoder = CentrelineGeocoder(df)
        return self._geocoder

    # Lookup helpers -------------------------------------------------
    @staticmethod
    def _normalize_location(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip().upper()
        if not text or text.lower() == "nan":
            return None
        return text

    def _load_location_lookup(self) -> Dict[str, Dict[str, Any]]:
        lookup: Dict[str, Dict[str, Any]] = {}
        env_path = os.getenv("PARKING_TICKETS_LOCATION_LOOKUP")
        if env_path:
            path = Path(env_path)
        else:
            repo_root = Path(__file__).resolve().parents[3]
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
            location = self._normalize_location(properties.get("location"))
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

    def _lookup_precomputed_location(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        candidates = [
            self._normalize_location(record.get("location2")),
            self._normalize_location(record.get("location1")),
        ]
        for candidate in candidates:
            if candidate and candidate in self._location_lookup:
                return self._location_lookup[candidate]
        return None

    def _ensure_tables(self) -> None:
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS parking_tickets (
                ticket_hash TEXT PRIMARY KEY,
                ticket_number TEXT,
                date_of_infraction DATE,
                time_of_infraction TEXT,
                infraction_code TEXT,
                infraction_description TEXT,
                set_fine_amount NUMERIC,
                location1 TEXT,
                location2 TEXT,
                location3 TEXT,
                location4 TEXT,
                street_normalized TEXT,
                centreline_id BIGINT,
                geom geometry(POINT, 4326),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS parking_tickets_staging (
                ticket_hash TEXT,
                ticket_number TEXT,
                date_of_infraction TEXT,
                time_of_infraction TEXT,
                infraction_code TEXT,
                infraction_description TEXT,
                set_fine_amount TEXT,
                location1 TEXT,
                location2 TEXT,
                location3 TEXT,
                location4 TEXT,
                street_normalized TEXT,
                centreline_id BIGINT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION
            )
            """
        )
        self.pg.execute("ALTER TABLE parking_tickets_staging SET UNLOGGED")
        self.pg.execute(
            """
            ALTER TABLE parking_tickets
            ADD COLUMN IF NOT EXISTS ticket_hash TEXT
            """
        )
        self.pg.execute(
            """
            ALTER TABLE parking_tickets_staging
            ADD COLUMN IF NOT EXISTS ticket_hash TEXT
            """
        )
        self.pg.execute(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'public'
                      AND table_name = 'parking_tickets'
                      AND constraint_type = 'PRIMARY KEY'
                ) THEN
                    EXECUTE 'ALTER TABLE parking_tickets DROP CONSTRAINT parking_tickets_pkey';
                END IF;
            END
            $$
            """
        )
        self.pg.execute(
            """
            UPDATE parking_tickets
            SET ticket_hash = md5(
                COALESCE(ticket_number, '') || '|' ||
                COALESCE(date_of_infraction::TEXT, '') || '|' ||
                COALESCE(time_of_infraction, '') || '|' ||
                COALESCE(infraction_code, '') || '|' ||
                COALESCE(infraction_description, '') || '|' ||
                COALESCE(set_fine_amount::TEXT, '') || '|' ||
                COALESCE(location1, '') || '|' ||
                COALESCE(location2, '') || '|' ||
                COALESCE(location3, '') || '|' ||
                COALESCE(location4, '')
            )
            WHERE ticket_hash IS NULL
            """
        )
        self.pg.execute("ALTER TABLE parking_tickets ALTER COLUMN ticket_hash SET NOT NULL")
        self.pg.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'public'
                      AND table_name = 'parking_tickets'
                      AND constraint_type = 'PRIMARY KEY'
                ) THEN
                    EXECUTE 'ALTER TABLE parking_tickets ADD CONSTRAINT parking_tickets_pkey PRIMARY KEY (ticket_hash)';
                END IF;
            END
            $$
            """
        )

