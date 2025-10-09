"""ETL pipeline for Red Light Camera locations."""

import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import pandas as pd

from ..state import DatasetState
from ..utils import iter_csv, sha1sum
from .base import DatasetETL, ExtractionResult


RLC_CODE_OFFSET = 3500
RED_LIGHT_FINE_ESTIMATE = Decimal(os.getenv("RED_LIGHT_FINE_AMOUNT", "325"))


STAGING_COLUMNS = (
    "intersection_id",
    "location_code",
    "linear_name_full_1",
    "linear_name_full_2",
    "location_name",
    "ward_1",
    "police_division_1",
    "activation_date",
    "ticket_count",
    "total_fine_amount",
    "years",
    "months",
    "yearly_counts_json",
    "geometry_geojson",
)


def _format_pg_array(values: Sequence[int]) -> str:
    if not values:
        return "{}"
    return "{" + ",".join(str(int(value)) for value in values) + "}"


def _load_charges_summary(path: Path | None) -> Dict[str, Dict[str, Any]]:
    if path is None or not path.exists():
        return {}

    dataframe = pd.read_excel(path, skiprows=4)
    if dataframe.empty:
        return {}

    dataframe = dataframe.rename(columns=lambda value: str(value).strip() if value is not None else "")
    dataframe["Location Codes"] = pd.to_numeric(dataframe.get("Location Codes"), errors="coerce")
    dataframe = dataframe.dropna(subset=["Location Codes"])

    year_columns: List[Any] = []
    for column in dataframe.columns:
        if isinstance(column, (int, float)) and not pd.isna(column):
            year_columns.append(int(column))
        else:
            text = str(column).strip()
            if text.isdigit():
                year_columns.append(int(text))

    summary: Dict[str, Dict[str, Any]] = {}

    for _, row in dataframe.iterrows():
        try:
            location_code = str(int(round(float(row["Location Codes"]))))
        except (TypeError, ValueError):
            continue

        yearly_counts: Dict[str, int] = {}
        years: List[int] = []
        total_tickets = 0

        for year in year_columns:
            raw_value = row.get(year) if year in row else row.get(str(year))
            value = pd.to_numeric(raw_value, errors="coerce")
            if pd.isna(value) or value <= 0:
                continue
            ticket_total = int(round(float(value)))
            if ticket_total <= 0:
                continue
            yearly_counts[str(year)] = yearly_counts.get(str(year), 0) + ticket_total
            years.append(year)
            total_tickets += ticket_total

        fine_amount = (RED_LIGHT_FINE_ESTIMATE * Decimal(total_tickets)).quantize(Decimal("0.01"))
        ward_value = row.get("Ward Number") or row.get("Ward")
        location_name = row.get("Charges Laid by Location & Year") or row.get("Location") or ""

        existing = summary.get(location_code)
        if existing is None:
            summary[location_code] = {
                "location_name": str(location_name).strip(),
                "ward": ward_value,
                "ticket_count": total_tickets,
                "total_fine_amount": fine_amount,
                "years": sorted(set(years)),
                "months": [],
                "yearly_counts": dict(yearly_counts),
            }
        else:
            existing["ticket_count"] += total_tickets
            existing["total_fine_amount"] = (Decimal(existing["total_fine_amount"]) + fine_amount).quantize(Decimal("0.01"))
            if ward_value and not existing.get("ward"):
                existing["ward"] = ward_value
            if existing.get("location_name") in (None, "") and location_name:
                existing["location_name"] = str(location_name).strip()
            merged_years = set(existing.get("years", []))
            merged_years.update(years)
            existing["years"] = sorted(merged_years)
            merged_counts = existing.get("yearly_counts", {})
            for key, value in yearly_counts.items():
                merged_counts[key] = merged_counts.get(key, 0) + value
            existing["yearly_counts"] = merged_counts

    return summary


class RedLightLocationsETL(DatasetETL):
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
            raise RuntimeError("Red light locations ETL expected 'locations' resource")

        charges_path = extraction.resource_paths.get("charges")
        charges_summary = _load_charges_summary(Path(charges_path) if charges_path else None)

        rows: List[Tuple[Any, ...]] = []
        for row in iter_csv(locations_path):
            intersection_id = row.get("INTERSECTION_ID") or row.get("intersection_id")
            geometry_raw = (
                row.get("geometry_geojson")
                or row.get("geometry")
                or row.get("GEOMETRY")
            )
            if not intersection_id or not geometry_raw:
                continue

            try:
                geometry_obj = json.loads(geometry_raw)
            except (TypeError, json.JSONDecodeError):
                continue

            if isinstance(geometry_obj, dict) and geometry_obj.get("type") == "MultiPoint":
                coordinates = geometry_obj.get("coordinates") or []
                if coordinates:
                    first = coordinates[0]
                    if isinstance(first, (list, tuple)) and len(first) >= 2:
                        geometry_obj = {"type": "Point", "coordinates": list(first)}

            geometry_json = json.dumps(geometry_obj)

            rlc_value = row.get("RLC") or row.get("rlc")
            location_code: str | None = None
            if rlc_value is not None:
                try:
                    location_code = str(int(round(float(rlc_value))) - RLC_CODE_OFFSET)
                except (TypeError, ValueError):
                    location_code = None

            metrics = charges_summary.get(str(location_code)) if location_code else None
            ticket_count = int(metrics.get("ticket_count", 0)) if metrics else 0
            total_fine_amount = metrics.get("total_fine_amount") if metrics else None
            years = metrics.get("years", []) if metrics else []
            months = metrics.get("months", []) if metrics else []
            yearly_counts = metrics.get("yearly_counts", {}) if metrics else {}

            rows.append(
                (
                    intersection_id,
                    location_code,
                    row.get("LINEAR_NAME_FULL_1") or row.get("linear_name_full_1"),
                    row.get("LINEAR_NAME_FULL_2") or row.get("linear_name_full_2"),
                    (metrics.get("location_name") if metrics else None)
                    or row.get("NAME")
                    or row.get("name"),
                    row.get("WARD_1") or row.get("ward_1") or row.get("WARD") or row.get("ward"),
                    row.get("POLICE_DIVISION_1") or row.get("police_division_1"),
                    row.get("ACTIVATION_DATE") or row.get("activation_date"),
                    ticket_count,
                    str(total_fine_amount) if total_fine_amount is not None else None,
                    _format_pg_array(years),
                    _format_pg_array(months),
                    json.dumps(yearly_counts) if yearly_counts else None,
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
            CREATE TABLE IF NOT EXISTS red_light_camera_locations (
                intersection_id TEXT PRIMARY KEY,
                location_code TEXT,
                linear_name_full_1 TEXT,
                linear_name_full_2 TEXT,
                location_name TEXT,
                ward_1 TEXT,
                police_division_1 TEXT,
                activation_date DATE,
                ticket_count INTEGER DEFAULT 0,
                total_fine_amount NUMERIC(18, 2),
                years INTEGER[],
                months INTEGER[],
                yearly_counts JSONB,
                geom geometry(POINT, 4326)
            )
            """
        )
        self.pg.execute(
            """
            ALTER TABLE red_light_camera_locations
                ADD COLUMN IF NOT EXISTS location_name TEXT,
                ADD COLUMN IF NOT EXISTS location_code TEXT,
                ADD COLUMN IF NOT EXISTS ticket_count INTEGER DEFAULT 0,
                ADD COLUMN IF NOT EXISTS total_fine_amount NUMERIC(18, 2),
                ADD COLUMN IF NOT EXISTS years INTEGER[],
                ADD COLUMN IF NOT EXISTS months INTEGER[],
                ADD COLUMN IF NOT EXISTS yearly_counts JSONB
            """
        )
        self.pg.execute("DROP TABLE IF EXISTS red_light_camera_locations_staging")
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS red_light_camera_locations_staging (
                intersection_id TEXT,
                location_code TEXT,
                linear_name_full_1 TEXT,
                linear_name_full_2 TEXT,
                location_name TEXT,
                ward_1 TEXT,
                police_division_1 TEXT,
                activation_date TEXT,
                ticket_count INTEGER,
                total_fine_amount TEXT,
                years TEXT,
                months TEXT,
                yearly_counts_json TEXT,
                geometry_geojson TEXT
            )
            """
        )

        self.pg.copy_rows("red_light_camera_locations_staging", STAGING_COLUMNS, rows)
        self.pg.execute(
            """
            INSERT INTO red_light_camera_locations AS target (
                intersection_id,
                location_code,
                linear_name_full_1,
                linear_name_full_2,
                location_name,
                ward_1,
                police_division_1,
                activation_date,
                ticket_count,
                total_fine_amount,
                years,
                months,
                yearly_counts,
                geom
            )
            SELECT DISTINCT ON (intersection_id)
                intersection_id,
                NULLIF(location_code, ''),
                linear_name_full_1,
                linear_name_full_2,
                location_name,
                ward_1,
                police_division_1,
                NULLIF(activation_date, '')::DATE,
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
                CASE
                    WHEN geometry_geojson IS NULL OR geometry_geojson = '' THEN NULL
                    ELSE ST_SetSRID(ST_GeomFromGeoJSON(geometry_geojson), 4326)
                END
            FROM red_light_camera_locations_staging
            ORDER BY intersection_id, ticket_count DESC
            ON CONFLICT (intersection_id) DO UPDATE SET
                linear_name_full_1 = EXCLUDED.linear_name_full_1,
                linear_name_full_2 = EXCLUDED.linear_name_full_2,
                location_name = EXCLUDED.location_name,
                location_code = COALESCE(EXCLUDED.location_code, target.location_code),
                ward_1 = EXCLUDED.ward_1,
                police_division_1 = EXCLUDED.police_division_1,
                activation_date = EXCLUDED.activation_date,
                ticket_count = COALESCE(EXCLUDED.ticket_count, target.ticket_count),
                total_fine_amount = EXCLUDED.total_fine_amount,
                years = EXCLUDED.years,
                months = EXCLUDED.months,
                yearly_counts = EXCLUDED.yearly_counts,
                geom = EXCLUDED.geom
            """
        )
        self.pg.execute("TRUNCATE red_light_camera_locations_staging")
