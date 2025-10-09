"""ETL pipeline for Automated Speed Enforcement (ASE) locations."""

from __future__ import annotations

import json
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import pandas as pd

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


class ASELocationsETL(DatasetETL):
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

        rows: List[Tuple[Any, ...]] = []
        for row in iter_csv(locations_path):
            code = row.get("Location_Code") or row.get("LOCATION_CODE") or row.get("location_code")
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

            metrics = charges_summary.get(str(code)) or charges_summary.get(str(code).strip())
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
                    (row.get("location") or row.get("LOCATION") or (metrics.get("location") if metrics else None)),
                    ticket_count,
                    str(total_fine_amount) if total_fine_amount is not None else None,
                    _format_pg_array(years),
                    _format_pg_array(months),
                    json.dumps(yearly_counts) if yearly_counts else None,
                    json.dumps(monthly_counts) if monthly_counts else None,
                    geometry_json,
                )
            )

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
