"""ETL implementation for the Toronto Centreline dataset."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from ..utils import sha1sum
from ..state import DatasetState
from .base import DatasetETL, ExtractionResult


CENTRELINE_COLUMNS = (
    "centreline_id",
    "linear_name",
    "linear_name_type",
    "linear_name_dir",
    "linear_name_full",
    "linear_name_label",
    "parity_left",
    "parity_right",
    "low_num_even",
    "high_num_even",
    "low_num_odd",
    "high_num_odd",
    "feature_code",
    "feature_code_desc",
    "jurisdiction",
    "geometry_geojson",
)


class CentrelineETL(DatasetETL):
    """Loads Toronto Centreline features into PostGIS."""

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
        resource_path = extraction.resource_paths.get("metadata")
        if resource_path is None:
            raise RuntimeError("Centreline ETL expected 'metadata' resource")

        raw = json.loads(resource_path.read_text(encoding="utf-8"))
        features = raw.get("features", [])

        rows: List[Tuple[Any, ...]] = []

        def safe_int(value: Any) -> Any:
            if value is None or value == "":
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        for feature in features:
            properties = feature.get("properties") or {}
            geometry = feature.get("geometry")
            centreline_id = properties.get("CENTRELINE_ID")
            if centreline_id is None:
                continue
            try:
                centreline_id_int = int(centreline_id)
            except (TypeError, ValueError):
                continue

            geometry_geojson = json.dumps(geometry) if geometry else None
            rows.append(
                (
                    centreline_id_int,
                    properties.get("LINEAR_NAME"),
                    properties.get("LINEAR_NAME_TYPE"),
                    properties.get("LINEAR_NAME_DIR"),
                    properties.get("LINEAR_NAME_FULL"),
                    properties.get("LINEAR_NAME_LABEL"),
                    properties.get("PARITY_L"),
                    properties.get("PARITY_R"),
                    safe_int(properties.get("LOW_NUM_EVEN")),
                    safe_int(properties.get("HIGH_NUM_EVEN")),
                    safe_int(properties.get("LOW_NUM_ODD")),
                    safe_int(properties.get("HIGH_NUM_ODD")),
                    safe_int(properties.get("FEATURE_CODE")),
                    properties.get("FEATURE_CODE_DESC"),
                    properties.get("JURISDICTION"),
                    geometry_geojson,
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
            CREATE TABLE IF NOT EXISTS centreline_segments (
                centreline_id BIGINT PRIMARY KEY,
                linear_name TEXT,
                linear_name_type TEXT,
                linear_name_dir TEXT,
                linear_name_full TEXT,
                linear_name_label TEXT,
                parity_left TEXT,
                parity_right TEXT,
                low_num_even INTEGER,
                high_num_even INTEGER,
                low_num_odd INTEGER,
                high_num_odd INTEGER,
                feature_code INTEGER,
                feature_code_desc TEXT,
                jurisdiction TEXT,
                geom geometry(MULTILINESTRING, 4326)
            )
            """
        )
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS centreline_segments_staging (
                centreline_id BIGINT,
                linear_name TEXT,
                linear_name_type TEXT,
                linear_name_dir TEXT,
                linear_name_full TEXT,
                linear_name_label TEXT,
                parity_left TEXT,
                parity_right TEXT,
                low_num_even INTEGER,
                high_num_even INTEGER,
                low_num_odd INTEGER,
                high_num_odd INTEGER,
                feature_code INTEGER,
                feature_code_desc TEXT,
                jurisdiction TEXT,
                geometry_geojson TEXT
            )
            """
        )

        staging_rows = rows
        staging_table = "centreline_segments_staging"
        inserted = self.pg.copy_rows(staging_table, CENTRELINE_COLUMNS, staging_rows)

        self.pg.execute(
            """
            ALTER TABLE centreline_segments
            ALTER COLUMN geom TYPE geometry(MULTILINESTRING, 4326)
            USING CASE
                WHEN geom IS NULL THEN NULL
                ELSE ST_Multi(ST_Force2D(geom))
            END
            """
        )

        if inserted:
            self.pg.execute(
                """
                INSERT INTO centreline_segments as target (
                    centreline_id,
                    linear_name,
                    linear_name_type,
                    linear_name_dir,
                    linear_name_full,
                    linear_name_label,
                    parity_left,
                    parity_right,
                    low_num_even,
                    high_num_even,
                    low_num_odd,
                    high_num_odd,
                    feature_code,
                    feature_code_desc,
                    jurisdiction,
                    geom
                )
                SELECT DISTINCT ON (centreline_id)
                    centreline_id,
                    linear_name,
                    linear_name_type,
                    linear_name_dir,
                    linear_name_full,
                    linear_name_label,
                    parity_left,
                    parity_right,
                    low_num_even,
                    high_num_even,
                    low_num_odd,
                    high_num_odd,
                    feature_code,
                    feature_code_desc,
                    jurisdiction,
                    CASE
                        WHEN geometry_geojson IS NULL OR geometry_geojson = '' THEN NULL
                        ELSE ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(geometry_geojson)), 4326)
                    END
                FROM centreline_segments_staging
                ORDER BY centreline_id
                ON CONFLICT (centreline_id) DO UPDATE SET
                    linear_name = EXCLUDED.linear_name,
                    linear_name_type = EXCLUDED.linear_name_type,
                    linear_name_dir = EXCLUDED.linear_name_dir,
                    linear_name_full = EXCLUDED.linear_name_full,
                    linear_name_label = EXCLUDED.linear_name_label,
                    parity_left = EXCLUDED.parity_left,
                    parity_right = EXCLUDED.parity_right,
                    low_num_even = EXCLUDED.low_num_even,
                    high_num_even = EXCLUDED.high_num_even,
                    low_num_odd = EXCLUDED.low_num_odd,
                    high_num_odd = EXCLUDED.high_num_odd,
                    feature_code = EXCLUDED.feature_code,
                    feature_code_desc = EXCLUDED.feature_code_desc,
                    jurisdiction = EXCLUDED.jurisdiction,
                    geom = EXCLUDED.geom
                """
            )
            self.pg.execute("TRUNCATE centreline_segments_staging")
