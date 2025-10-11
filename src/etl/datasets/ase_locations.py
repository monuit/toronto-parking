"""ETL pipeline for Automated Speed Enforcement (ASE) locations."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from geocoding.centreline_geocoder import CentrelineGeocoder, GeocodeResult

from ..state import DatasetState
from ..utils import iter_csv, sha1sum
from .base import DatasetETL, ExtractionResult


ASE_FINE_ESTIMATE = Decimal(os.getenv("ASE_FINE_AVG", "50"))


STAGING_COLUMNS = (
    "location_code",
    "ward",
    "status",
    "location",
    "ticket_count",
    "total_fine_amount",
    "years",
    "months",
    "yearly_counts_json",
    "monthly_counts_json",
    "geometry_geojson",
)


def _format_pg_array(values: Sequence[int]) -> str:
    if not values:
        return "{}"
    return "{" + ",".join(str(int(v)) for v in values) + "}"


def _load_charges_summary(path: Path | None) -> Dict[str, Dict[str, Any]]:
    if path is None or not path.exists():
        return {}

    dataframe = pd.read_excel(path)
    if dataframe.empty:
        return {}

    dataframe["Site Code"] = dataframe.get("Site Code").astype(str).str.strip()
    dataframe = dataframe.dropna(subset=["Site Code"])

    month_columns: List[Any] = []
    for column in dataframe.columns:
        if isinstance(column, (pd.Timestamp, datetime)):
            month_columns.append(column)

    summary: Dict[str, Dict[str, Any]] = {}
    for _, row in dataframe.iterrows():
        site_code = str(row.get("Site Code") or "").strip()
        if not site_code or site_code.lower() == "nan":
            continue

        monthly_counts: Dict[str, int] = {}
        yearly_counts: Dict[str, int] = {}
        years: List[int] = []
        months: List[int] = []
        total_tickets = 0

        for column in month_columns:
            raw_value = row.get(column)
            value = pd.to_numeric(raw_value, errors="coerce")
            if pd.isna(value) or value <= 0:
                continue
            ticket_total = int(round(float(value)))
            if ticket_total <= 0:
                continue
            key = f"{column.year:04d}-{column.month:02d}"
            monthly_counts[key] = ticket_total
            years.append(column.year)
            months.append(column.month)
            total_tickets += ticket_total

            year_key = f"{column.year:04d}"
            yearly_counts[year_key] = yearly_counts.get(year_key, 0) + ticket_total

        ward_value = row.get("Ward")
        ward_name = None
        if ward_value is not None and not (isinstance(ward_value, float) and pd.isna(ward_value)):
            ward_name = str(ward_value).strip()

        fine_amount = (ASE_FINE_ESTIMATE * Decimal(total_tickets)).quantize(Decimal("0.01"))

        existing = summary.get(site_code)
        if existing is None:
            summary[site_code] = {
                "location": str(row.get("Location*") or row.get("Location") or ""),
                "ward": ward_name,
                "ticket_count": total_tickets,
                "total_fine_amount": fine_amount,
                "years": sorted(set(years)),
                "months": sorted(set(months)),
                "monthly_counts": dict(monthly_counts),
                "yearly_counts": dict(yearly_counts),
            }
        else:
            existing["ticket_count"] += total_tickets
            existing["total_fine_amount"] = (Decimal(existing["total_fine_amount"]) + fine_amount).quantize(Decimal("0.01"))
            if ward_name and not existing.get("ward"):
                existing["ward"] = ward_name
            if existing.get("location") in (None, "") and row.get("Location*"):
                existing["location"] = str(row.get("Location*"))
            existing_years = set(existing.get("years", []))
            existing_months = set(existing.get("months", []))
            existing_years.update(years)
            existing_months.update(months)
            existing["years"] = sorted(existing_years)
            existing["months"] = sorted(existing_months)
            merged_counts = existing.get("monthly_counts", {})
            for key, value in monthly_counts.items():
                merged_counts[key] = merged_counts.get(key, 0) + value
            existing["monthly_counts"] = merged_counts
            merged_yearly = existing.get("yearly_counts", {})
            for year_key, value in yearly_counts.items():
                merged_yearly[year_key] = merged_yearly.get(year_key, 0) + value
            existing["yearly_counts"] = merged_yearly

    return summary


def _clean_location_code(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"none", "null", "nan"}:
        return ""
    return text


def _normalise_location_key(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


class ASELocationsETL(DatasetETL):
    def __init__(self, config, **kwargs: Any) -> None:  # noqa: D401
        super().__init__(config, **kwargs)
        self._geocoder: Optional[CentrelineGeocoder] = None
        disable_env = os.getenv("ASE_LOCATIONS_DISABLE_GEOCODER")
        self._disable_geocoder = bool(disable_env and disable_env.lower() in {"1", "true", "yes"})

    def extract(self, state: DatasetState) -> ExtractionResult | None:
        previous_resources: Dict[str, Dict[str, Any]] = (
            (state.metadata or {}).get("resources", {}) if state.metadata else {}
        )
        resource_paths: Dict[str, Any] = {}
        resource_hashes: Dict[str, str] = {}
        resource_metadata: Dict[str, Dict[str, Any]] = {}
        has_changes = False

        for name, resource_cfg in self.config.resources.items():
            resource_info = self.get_package_resource(resource_cfg)
            suffix = self.infer_suffix(resource_info, resource_cfg)
            path = self.store.raw_path(self.config.slug, resource_cfg.resource_id, suffix)
            manifest_entry = previous_resources.get(name, {})
            last_modified = resource_info.get("last_modified")

            if (
                last_modified
                and manifest_entry.get("last_modified") == last_modified
                and path.exists()
            ):
                sha1 = manifest_entry.get("sha1") or sha1sum(path)
            else:
                path = self.download_resource(resource_cfg, suffix=suffix)
                sha1 = sha1sum(path)
                has_changes = True

            resource_paths[name] = path
            resource_hashes[name] = sha1
            resource_metadata[name] = {
                "resource_id": resource_cfg.resource_id,
                "last_modified": last_modified,
                "format": resource_info.get("format"),
                "sha1": sha1,
            }

        if not has_changes and state.last_resource_hash:
            prior_hashes = [item.get("sha1") for item in previous_resources.values() if item.get("sha1")]
            if prior_hashes and "|".join(sorted(prior_hashes)) == state.last_resource_hash:
                return None

        return ExtractionResult(
            resource_paths=resource_paths,
            resource_hashes=resource_hashes,
            resource_metadata=resource_metadata,
        )

    def transform(self, extraction: ExtractionResult, state: DatasetState) -> Dict[str, Any]:  # noqa: ARG002
        locations_path = extraction.resource_paths.get("locations")
        if locations_path is None:
            raise RuntimeError("ASE locations ETL expected 'locations' resource")

        charges_path = extraction.resource_paths.get("charges")
        charges_summary = _load_charges_summary(Path(charges_path) if charges_path else None)
        charges_location_index = {
            _normalise_location_key(payload.get("location")): code
            for code, payload in charges_summary.items()
            if payload.get("location")
        }
        unmatched_lookup: Dict[str, Dict[str, Any]] = dict(charges_summary)

        rows: List[Tuple[Any, ...]] = []
        for row in iter_csv(locations_path):
            location_name = row.get("location") or row.get("LOCATION")
            raw_code = (
                row.get("Location_Code")
                or row.get("LOCATION_CODE")
                or row.get("location_code")
                or row.get("Location Code")
            )
            code = _clean_location_code(raw_code)
            if not code:
                lookup_code = charges_location_index.get(_normalise_location_key(location_name))
                if lookup_code:
                    code = lookup_code
                else:
                    fallback_sources = (
                        row.get("Site Code"),
                        row.get("Site_Code"),
                        row.get("site_code"),
                        row.get("FID"),
                        row.get("_id"),
                    )
                    fallback_value = None
                    for candidate in fallback_sources:
                        cleaned_candidate = _clean_location_code(candidate)
                        if cleaned_candidate:
                            fallback_value = cleaned_candidate
                            break
                    if fallback_value:
                        code = f"ASE-{fallback_value}"
                    else:
                        code = f"ASE-{len(rows) + 1}"

            geometry_raw = (
                row.get("geometry_geojson")
                or row.get("geometry")
                or row.get("GEOMETRY")
            )
            if not code or not geometry_raw:
                continue

            try:
                geometry_obj = json.loads(geometry_raw)
            except (TypeError, json.JSONDecodeError):
                continue

            if isinstance(geometry_obj, dict) and geometry_obj.get("type") == "MultiPoint":
                coords = geometry_obj.get("coordinates") or []
                if coords:
                    first = coords[0]
                    if isinstance(first, (list, tuple)) and len(first) >= 2:
                        geometry_obj = {"type": "Point", "coordinates": list(first)}

            geometry_json = json.dumps(geometry_obj)

            metrics = None
            if code:
                metrics = unmatched_lookup.pop(code, None)
                if metrics is None:
                    metrics = charges_summary.get(code)
            ticket_count = int(metrics.get("ticket_count", 0)) if metrics else 0
            total_fine_amount = metrics.get("total_fine_amount") if metrics else None
            years = metrics.get("years", []) if metrics else []
            months = metrics.get("months", []) if metrics else []
            yearly_counts = metrics.get("yearly_counts", {}) if metrics else {}
            monthly_counts = metrics.get("monthly_counts", {}) if metrics else {}

            # We only persist yearly aggregates downstream to keep payloads compact.
            monthly_counts = {}

            rows.append(
                (
                    str(code).strip(),
                    row.get("ward") or row.get("WARD"),
                    row.get("Status") or row.get("STATUS"),
                    (location_name or (metrics.get("location") if metrics else None)),
                    ticket_count,
                    str(total_fine_amount) if total_fine_amount is not None else None,
                    _format_pg_array(years),
                    _format_pg_array(months),
                    json.dumps(yearly_counts) if yearly_counts else None,
                    json.dumps(monthly_counts) if monthly_counts else None,
                    geometry_json,
                )
            )

        if unmatched_lookup and not self._disable_geocoder:
            for code, metrics in list(unmatched_lookup.items()):
                location_label = metrics.get("location")
                coordinates = self._geocode_location(location_label)
                if coordinates is None:
                    continue

                longitude, latitude = coordinates
                geometry_json = json.dumps(
                    {
                        "type": "Point",
                        "coordinates": [longitude, latitude],
                    }
                )

                ticket_count = int(metrics.get("ticket_count", 0) or 0)
                total_fine_amount = metrics.get("total_fine_amount")
                years = metrics.get("years", [])
                months = metrics.get("months", [])
                yearly_counts = metrics.get("yearly_counts", {})

                rows.append(
                    (
                        _clean_location_code(code) or str(code),
                        metrics.get("ward"),
                        metrics.get("status") or "Historical",
                        metrics.get("location"),
                        ticket_count,
                        str(total_fine_amount) if total_fine_amount is not None else None,
                        _format_pg_array(years),
                        _format_pg_array(months),
                        json.dumps(yearly_counts) if yearly_counts else None,
                        None,
                        geometry_json,
                    )
                )
                unmatched_lookup.pop(code, None)

        return {"rows": rows, "row_count": len(rows)}

    def load(self, payload: Dict[str, Any], state: DatasetState) -> None:  # noqa: ARG002
        rows: List[Tuple[Any, ...]] = payload.get("rows", [])
        if not rows:
            return

        self.pg.ensure_extensions()
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS ase_camera_locations (
                location_code TEXT PRIMARY KEY,
                ward TEXT,
                status TEXT,
                location TEXT,
                ticket_count INTEGER DEFAULT 0,
                total_fine_amount NUMERIC(18, 2),
                years INTEGER[],
                months INTEGER[],
                yearly_counts JSONB,
                monthly_counts JSONB,
                geom geometry(POINT, 4326)
            )
            """
        )
        self.pg.execute(
            """
            ALTER TABLE ase_camera_locations
                ADD COLUMN IF NOT EXISTS ticket_count INTEGER DEFAULT 0,
                ADD COLUMN IF NOT EXISTS total_fine_amount NUMERIC(18, 2),
                ADD COLUMN IF NOT EXISTS years INTEGER[],
                ADD COLUMN IF NOT EXISTS months INTEGER[],
                ADD COLUMN IF NOT EXISTS yearly_counts JSONB,
                ADD COLUMN IF NOT EXISTS monthly_counts JSONB
            """
        )
        self.pg.execute("DROP TABLE IF EXISTS ase_camera_locations_staging")
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS ase_camera_locations_staging (
                location_code TEXT,
                ward TEXT,
                status TEXT,
                location TEXT,
                ticket_count INTEGER,
                total_fine_amount TEXT,
                years TEXT,
                months TEXT,
                yearly_counts_json TEXT,
                monthly_counts_json TEXT,
                geometry_geojson TEXT
            )
            """
        )

        self.pg.copy_rows("ase_camera_locations_staging", STAGING_COLUMNS, rows)
        self.pg.execute(
            """
            INSERT INTO ase_camera_locations AS target (
                location_code,
                ward,
                status,
                location,
                ticket_count,
                total_fine_amount,
                years,
                months,
                yearly_counts,
                monthly_counts,
                geom
            )
            SELECT DISTINCT ON (location_code)
                location_code,
                ward,
                status,
                location,
                COALESCE(ticket_count, 0),
                NULLIF(total_fine_amount, '')::NUMERIC,
                CASE
                    WHEN years IS NULL OR years = '' THEN NULL
                    ELSE years::INT[]
                END,
                CASE
                    WHEN months IS NULL OR months = '' THEN NULL
                    ELSE months::INT[]
                END,
                NULLIF(yearly_counts_json, '')::JSONB,
                NULLIF(monthly_counts_json, '')::JSONB,
                CASE
                    WHEN geometry_geojson IS NULL OR geometry_geojson = '' THEN NULL
                    ELSE ST_SetSRID(ST_GeomFromGeoJSON(geometry_geojson), 4326)
                END
            FROM ase_camera_locations_staging
            ORDER BY location_code, ticket_count DESC
            ON CONFLICT (location_code) DO UPDATE SET
                ward = EXCLUDED.ward,
                status = EXCLUDED.status,
                location = EXCLUDED.location,
                ticket_count = COALESCE(EXCLUDED.ticket_count, target.ticket_count),
                total_fine_amount = EXCLUDED.total_fine_amount,
                years = EXCLUDED.years,
                months = EXCLUDED.months,
                yearly_counts = EXCLUDED.yearly_counts,
                monthly_counts = EXCLUDED.monthly_counts,
                geom = EXCLUDED.geom
            """
        )
        self.pg.execute("TRUNCATE ase_camera_locations_staging")

    def _get_geocoder(self) -> CentrelineGeocoder:
        if self._geocoder is not None:
            return self._geocoder
        with self.pg.connect() as conn:  # type: ignore[arg-type]
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
        if "geometry" in df.columns:
            df["geometry"] = df["geometry"].apply(
                lambda value: json.loads(value) if isinstance(value, str) else value,
            )
        self._geocoder = CentrelineGeocoder(df)
        return self._geocoder

    def _geocode_location(self, location: Any) -> Optional[Tuple[float, float]]:
        if self._disable_geocoder or not location or not isinstance(location, str):
            return None
        for candidate in self._generate_location_candidates(location):
            try:
                result = self._get_geocoder().geocode(candidate)
            except Exception:
                result = None
            if isinstance(result, GeocodeResult):
                return result.longitude, result.latitude
        return None

    def _generate_location_candidates(self, value: str) -> List[str]:
        text = str(value).strip()
        if not text:
            return []
        candidates: List[str] = []

        def _append(candidate: str) -> None:
            cleaned = re.sub(r"[^A-Za-z0-9&@/ ]", " ", candidate)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            if cleaned and cleaned not in candidates:
                candidates.append(cleaned)

        _append(text)

        no_parentheses = re.sub(r"\s*\([^)]*\)", "", text).strip()
        _append(no_parentheses)

        normalised = re.sub(r"\bNEAR\b", " & ", text, flags=re.IGNORECASE)
        normalised = re.sub(r"\bBETWEEN\b", " & ", normalised, flags=re.IGNORECASE)
        normalised = re.sub(r"\b(NORTH|SOUTH|EAST|WEST)\s+OF\b", " & ", normalised, flags=re.IGNORECASE)
        normalised = re.sub(r"\b([NSEW])/?O\b", " & ", normalised, flags=re.IGNORECASE)
        normalised = re.sub(r"\bAND\b", " & ", normalised, flags=re.IGNORECASE)
        normalised = re.sub(r"\s+&\s+&\s+", " & ", normalised)
        normalised = re.sub(r"\s+", " ", normalised)
        _append(normalised)

        upper_variant = normalised.upper()
        _append(upper_variant)

        for separator in ('&', '@', '/'):  # basic delimiters between streets
            if separator in normalised:
                parts = [part.strip() for part in normalised.split(separator) if part.strip()]
                for part in parts:
                    _append(part)

        return candidates
