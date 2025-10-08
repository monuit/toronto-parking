"""ETL for Red Light Camera annual charge records."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ..utils import iter_csv, sha1sum
from ..state import DatasetState
from .base import DatasetETL, ExtractionResult


STAGING_COLUMNS = (
    "rlc_notice_number",
    "intersection_id",
    "charge_date",
    "set_fine_amount",
    "infraction_code",
    "infraction_description",
    "location",
    "time_of_infraction",
)


class RedLightChargesETL(DatasetETL):
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
        resource_path = extraction.resource_paths.get("annual_charges")
        if resource_path is None:
            raise RuntimeError("Red light charges ETL expected 'annual_charges' resource")

        rows: List[Tuple[Any, ...]] = []
        for row in iter_csv(resource_path):
            notice_number = row.get("RLC_NOTICE_NUMBER") or row.get("NOTICE_NUMBER")
            if not notice_number:
                continue
            rows.append(
                (
                    notice_number,
                    row.get("INTERSECTION_ID") or row.get("intersection_id"),
                    row.get("CHARGE_DATE") or row.get("charge_date"),
                    row.get("SET_FINE_AMOUNT") or row.get("set_fine_amount"),
                    row.get("INFRACTION_CODE") or row.get("infraction_code"),
                    row.get("INFRACTION_DESCRIPTION") or row.get("infraction_description"),
                    row.get("LOCATION") or row.get("location"),
                    row.get("TIME_OF_INFRACTION") or row.get("time_of_infraction"),
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
            CREATE TABLE IF NOT EXISTS red_light_charges (
                rlc_notice_number TEXT PRIMARY KEY,
                intersection_id TEXT,
                charge_date DATE,
                set_fine_amount NUMERIC,
                infraction_code TEXT,
                infraction_description TEXT,
                location TEXT,
                time_of_infraction TEXT,
                geom geometry(POINT, 4326)
            )
            """
        )
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS red_light_charges_staging (
                rlc_notice_number TEXT,
                intersection_id TEXT,
                charge_date TEXT,
                set_fine_amount TEXT,
                infraction_code TEXT,
                infraction_description TEXT,
                location TEXT,
                time_of_infraction TEXT
            )
            """
        )

        self.pg.copy_rows("red_light_charges_staging", STAGING_COLUMNS, rows)
        self.pg.execute(
            """
            INSERT INTO red_light_charges AS target (
                rlc_notice_number,
                intersection_id,
                charge_date,
                set_fine_amount,
                infraction_code,
                infraction_description,
                location,
                time_of_infraction,
                geom
            )
            SELECT
                staging.rlc_notice_number,
                staging.intersection_id,
                NULLIF(staging.charge_date, '')::DATE,
                NULLIF(staging.set_fine_amount, '')::NUMERIC,
                staging.infraction_code,
                staging.infraction_description,
                staging.location,
                staging.time_of_infraction,
                COALESCE(loc.geom, target.geom)
            FROM red_light_charges_staging AS staging
            LEFT JOIN red_light_camera_locations AS loc USING (intersection_id)
            ON CONFLICT (rlc_notice_number) DO UPDATE SET
                intersection_id = EXCLUDED.intersection_id,
                charge_date = EXCLUDED.charge_date,
                set_fine_amount = EXCLUDED.set_fine_amount,
                infraction_code = EXCLUDED.infraction_code,
                infraction_description = EXCLUDED.infraction_description,
                location = EXCLUDED.location,
                time_of_infraction = EXCLUDED.time_of_infraction,
                geom = COALESCE(EXCLUDED.geom, red_light_charges.geom)
            """
        )
        self.pg.execute("TRUNCATE red_light_charges_staging")
