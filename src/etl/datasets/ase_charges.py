"""ETL for Automated Speed Enforcement (ASE) charge records."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ..utils import iter_csv, sha1sum
from ..state import DatasetState
from .base import DatasetETL, ExtractionResult


STAGING_COLUMNS = (
    "ticket_number",
    "location_code",
    "infraction_date",
    "infraction_time",
    "set_fine_amount",
    "speed_over_limit",
    "location",
)


class ASEChargesETL(DatasetETL):
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
        resource_path = extraction.resource_paths.get("charges")
        if resource_path is None:
            raise RuntimeError("ASE charges ETL expected 'charges' resource")

        rows: List[Tuple[Any, ...]] = []
        for row in iter_csv(resource_path):
            ticket_number = row.get("TICKET_NUMBER") or row.get("ticket_number")
            if not ticket_number:
                continue
            rows.append(
                (
                    ticket_number,
                    row.get("LOCATION_CODE") or row.get("location_code"),
                    row.get("INFRACTION_DATE") or row.get("infraction_date"),
                    row.get("INFRACTION_TIME") or row.get("infraction_time"),
                    row.get("SET_FINE_AMOUNT") or row.get("set_fine_amount"),
                    row.get("SPEED_OVER_LIMIT") or row.get("speed_over_limit"),
                    row.get("LOCATION") or row.get("location"),
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
            CREATE TABLE IF NOT EXISTS ase_charges (
                ticket_number TEXT PRIMARY KEY,
                location_code TEXT,
                infraction_date DATE,
                infraction_time TEXT,
                set_fine_amount NUMERIC,
                speed_over_limit NUMERIC,
                location TEXT,
                geom geometry(POINT, 4326)
            )
            """
        )
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS ase_charges_staging (
                ticket_number TEXT,
                location_code TEXT,
                infraction_date TEXT,
                infraction_time TEXT,
                set_fine_amount TEXT,
                speed_over_limit TEXT,
                location TEXT
            )
            """
        )

        self.pg.copy_rows("ase_charges_staging", STAGING_COLUMNS, rows)
        self.pg.execute(
            """
            INSERT INTO ase_charges AS target (
                ticket_number,
                location_code,
                infraction_date,
                infraction_time,
                set_fine_amount,
                speed_over_limit,
                location,
                geom
            )
            SELECT
                staging.ticket_number,
                staging.location_code,
                NULLIF(staging.infraction_date, '')::DATE,
                staging.infraction_time,
                NULLIF(staging.set_fine_amount, '')::NUMERIC,
                NULLIF(staging.speed_over_limit, '')::NUMERIC,
                staging.location,
                COALESCE(loc.geom, target.geom)
            FROM ase_charges_staging AS staging
            LEFT JOIN ase_camera_locations AS loc USING (location_code)
            ON CONFLICT (ticket_number) DO UPDATE SET
                location_code = EXCLUDED.location_code,
                infraction_date = EXCLUDED.infraction_date,
                infraction_time = EXCLUDED.infraction_time,
                set_fine_amount = EXCLUDED.set_fine_amount,
                speed_over_limit = EXCLUDED.speed_over_limit,
                location = EXCLUDED.location,
                geom = COALESCE(EXCLUDED.geom, ase_charges.geom)
            """
        )
        self.pg.execute("TRUNCATE ase_charges_staging")
