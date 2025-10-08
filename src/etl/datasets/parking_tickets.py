
"""ETL pipeline for Toronto parking ticket records with geocoding."""

from __future__ import annotations

import csv
import io
import os
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd

from geocoding.centreline_geocoder import CentrelineGeocoder, GeocodeResult

from ..state import DatasetState
from ..utils import sha1sum
from .base import DatasetETL, ExtractionResult

STAGING_COLUMNS = (
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


class ParkingTicketsETL(DatasetETL):
    """Loader for the full Toronto parking ticket archive."""

    BATCH_SIZE = 5000

    def __init__(self, config, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self._geocoder: Optional[CentrelineGeocoder] = None
        self._geocode_cache: Dict[Tuple[str, str, str, str], object | GeocodeResult] = {}
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
        self.pg.execute(
            "DELETE FROM parking_tickets WHERE date_of_infraction >= %s AND date_of_infraction < %s",
            (start.isoformat(), end.isoformat()),
        )

        self._geocode_cache.clear()

        batch: List[Tuple[Any, ...]] = []
        rows_written = 0

        for row in self._iter_archive_rows(archive_path):
            batch.append(row)
            if len(batch) >= self.BATCH_SIZE:
                self._load_rows(batch)
                rows_written += len(batch)
                batch.clear()

        if batch:
            self._load_rows(batch)
            rows_written += len(batch)

        return rows_written

    def _iter_archive_rows(self, archive_path: Path) -> Iterator[Tuple[Any, ...]]:
        with zipfile.ZipFile(archive_path) as archive:
            for member in sorted(archive.namelist()):
                if not member.lower().endswith(".csv"):
                    continue
                with archive.open(member) as handle:
                    csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))
                    reader = csv.DictReader(
                        io.TextIOWrapper(handle, encoding="utf-8", errors="ignore")
                    )
                    for record in reader:
                        prepared = self._prepare_row(record)
                        if prepared is not None:
                            yield prepared

    def _prepare_row(self, record: Dict[str, Any]) -> Optional[Tuple[Any, ...]]:
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

        return (
            ticket_number,
            parsed_date,
            time_value,
            record.get("infraction_code") or record.get("infractioncode"),
            record.get("infraction_description") or record.get("infractiondescription"),
            set_fine,
            record.get("location1"),
            record.get("location2"),
            record.get("location3"),
            record.get("location4"),
            geocode_result.street_normalized if geocode_result else None,
            geocode_result.centreline_id if geocode_result else None,
            geocode_result.latitude if geocode_result else None,
            geocode_result.longitude if geocode_result else None,
        )

    def _geocode_record(self, record: Dict[str, Any]) -> Optional[GeocodeResult]:
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
        if isinstance(cached, GeocodeResult):
            return cached

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

    def _ensure_tables(self) -> None:
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS parking_tickets (
                ticket_number TEXT PRIMARY KEY,
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

    def _load_rows(self, rows: Iterable[Tuple[Any, ...]]) -> None:
        rows_list = list(rows)
        if not rows_list:
            return
        self.pg.execute("TRUNCATE parking_tickets_staging")
        self.pg.copy_rows("parking_tickets_staging", STAGING_COLUMNS, rows_list)
        self.pg.execute(
            """
            INSERT INTO parking_tickets AS target (
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
            SELECT DISTINCT ON (ticket_number)
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
            ORDER BY ticket_number, NULLIF(date_of_infraction, '')::DATE DESC, time_of_infraction DESC
            ON CONFLICT (ticket_number) DO UPDATE SET
                date_of_infraction = EXCLUDED.date_of_infraction,
                time_of_infraction = EXCLUDED.time_of_infraction,
                infraction_code = EXCLUDED.infraction_code,
                infraction_description = EXCLUDED.infraction_description,
                set_fine_amount = EXCLUDED.set_fine_amount,
                location1 = EXCLUDED.location1,
                location2 = EXCLUDED.location2,
                location3 = EXCLUDED.location3,
                location4 = EXCLUDED.location4,
                street_normalized = EXCLUDED.street_normalized,
                centreline_id = EXCLUDED.centreline_id,
                geom = COALESCE(EXCLUDED.geom, target.geom),
                updated_at = NOW()
            """
        )
        self.pg.execute("TRUNCATE parking_tickets_staging")
