"""Orchestrate the ward-level camera dataset build pipeline."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

import requests
from psycopg import Connection

from preprocessing.build_camera_datasets import (
    _locate_latest_ase_charges,
    _locate_latest_red_light_charges,
)
from src.etl.datasets.ase_locations import _load_charges_summary as load_ase_charges_summary
from src.etl.datasets.red_light_locations import _load_charges_summary as load_rlc_charges_summary

from .aggregations import (
    aggregate_charges,
    build_summary,
    load_totals_from_summary,
    merge_ward_totals,
    safe_number,
)
from .artifacts import artifact_invalid, checksum_to_version, compute_checksum, gzip_and_encode
from .constants import GEOJSON_PATHS, REDIS_KEYS, SUMMARY_PATHS, WARD_CACHE_PATH, WARD_GEOJSON_URL
from .state import WardDatasetState
from .tiles import build_precomputed_tiles


# MARK: Data structures


@dataclass
class DatasetArtifacts:
    """Computed artifacts for a single dataset build."""

    name: str
    totals: Dict[int, Dict[str, float]]
    summary: dict
    checksum: str
    changed: bool
    summary_path: Path
    geojson_path: Path
    version: int
    etag: str
    extra_fields: Optional[Dict[str, str]] = None


@dataclass
class BuildReport:
    """Aggregate result of a pipeline run."""

    datasets: Dict[str, DatasetArtifacts]
    tiles_triggered: List[str]

    def changed_datasets(self) -> List[str]:
        return [name for name, artifact in self.datasets.items() if artifact.changed]


# MARK: Pipeline manager


class WardDatasetPipeline:
    """Coordinates data downloads, aggregations, storage, and publishing."""

    def __init__(self, connection: Connection, redis_url: Optional[str] = None, *, force_download: bool = False) -> None:
        self.connection = connection
        self.redis_url = redis_url
        self.force_download = force_download
        self.state = WardDatasetState.load()
        self.schema_mismatch = self.state.schema_version_mismatch()

    def build(self) -> BuildReport:
        """Execute the full pipeline and return a report of changes."""

        ward_geojson = self._download_ward_geojson()
        self._ensure_tables()
        self._upsert_wards(ward_geojson)

        ase_path, rlc_path = self._resolve_charge_paths()

        ase_artifacts = self._build_primary_dataset(
            "ase_locations",
            ase_path,
            load_ase_charges_summary,
            self._outputs_missing("ase_locations"),
        )
        rlc_artifacts = self._build_primary_dataset(
            "red_light_locations",
            rlc_path,
            load_rlc_charges_summary,
            self._outputs_missing("red_light_locations"),
        )
        combined_artifacts = self._build_combined_dataset(
            ase_artifacts,
            rlc_artifacts,
            self._outputs_missing("cameras_combined"),
        )

        datasets = {
            "ase_locations": ase_artifacts,
            "red_light_locations": rlc_artifacts,
            "cameras_combined": combined_artifacts,
        }

        tiles: List[str] = []
        for artifact in datasets.values():
            if self._write_dataset_outputs(ward_geojson, artifact):
                tiles.append(artifact.name)

        self._update_state(ase_artifacts, ase_path, rlc_artifacts, rlc_path, combined_artifacts)

        if tiles:
            build_precomputed_tiles(tiles)

        return BuildReport(datasets=datasets, tiles_triggered=tiles)

    # MARK: High-level steps

    def _download_ward_geojson(self) -> dict:
        WARD_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        if WARD_CACHE_PATH.exists() and not self.force_download:
            return json.loads(WARD_CACHE_PATH.read_text(encoding="utf-8"))

        response = requests.get(WARD_GEOJSON_URL, timeout=60)
        response.raise_for_status()
        payload = response.json()
        WARD_CACHE_PATH.write_text(json.dumps(payload), encoding="utf-8")
        return payload

    def _ensure_tables(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS city_wards (
                    ward_code INTEGER PRIMARY KEY,
                    ward_name TEXT NOT NULL,
                    ward_short_code TEXT,
                    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
                    properties JSONB DEFAULT '{}'::JSONB,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS camera_ward_totals (
                    dataset TEXT NOT NULL,
                    ward_code INTEGER NOT NULL,
                    ward_name TEXT NOT NULL,
                    ticket_count BIGINT NOT NULL,
                    location_count INTEGER NOT NULL,
                    total_revenue NUMERIC(18, 2) NOT NULL,
                    metadata JSONB DEFAULT '{}'::JSONB,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (dataset, ward_code)
                )
                """
            )
        self.connection.commit()

    def _upsert_wards(self, geojson: dict) -> None:
        records = []
        for feature in geojson.get("features", []):
            properties = feature.get("properties") or {}
            geometry = feature.get("geometry")
            if not geometry:
                continue
            try:
                ward_code = int(str(properties.get("AREA_LONG_CODE") or properties.get("AREA_SHORT_CODE")))
            except (TypeError, ValueError):
                continue
            ward_name = str(properties.get("AREA_NAME") or f"Ward {ward_code}")
            ward_short = properties.get("AREA_SHORT_CODE")
            records.append((
                ward_code,
                ward_name,
                ward_short,
                json.dumps(geometry),
                json.dumps(properties),
            ))

        with self.connection.cursor() as cursor:
            for ward_code, ward_name, ward_short, geom_json, props_json in records:
                cursor.execute(
                    """
                    INSERT INTO city_wards (ward_code, ward_name, ward_short_code, geom, properties, updated_at)
                    VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s::JSONB, NOW())
                    ON CONFLICT (ward_code) DO UPDATE SET
                        ward_name = EXCLUDED.ward_name,
                        ward_short_code = EXCLUDED.ward_short_code,
                        geom = EXCLUDED.geom,
                        properties = EXCLUDED.properties,
                        updated_at = NOW()
                    """,
                    (ward_code, ward_name, ward_short, geom_json, props_json),
                )
        self.connection.commit()

    def _resolve_charge_paths(self) -> tuple[Path, Path]:
        ase_path = _locate_latest_ase_charges()
        rlc_path = _locate_latest_red_light_charges()
        if ase_path is None or rlc_path is None:
            raise RuntimeError("Missing ASE or Red Light charges source files")
        return Path(ase_path), Path(rlc_path)

    def _outputs_missing(self, dataset: str) -> bool:
        return artifact_invalid(SUMMARY_PATHS[dataset]) or artifact_invalid(
            GEOJSON_PATHS[dataset], expect_features=True
        )

    def _build_primary_dataset(
        self,
        dataset: str,
        source_path: Path,
        loader: Callable[[Path], Dict[str, Dict[str, float]]],
        outputs_missing: bool,
    ) -> DatasetArtifacts:
        checksum = compute_checksum(source_path)
        state_entry = self.state.entry(dataset)
        changed = (
            self.force_download
            or outputs_missing
            or state_entry.get("checksum") != checksum
            or self.schema_mismatch
        )

        summary, totals = load_totals_from_summary(dataset) if not changed else (None, None)
        if totals is None:
            lookup = loader(source_path)
            totals = aggregate_charges(lookup)
            summary = build_summary(totals)
            changed = True

        version = checksum_to_version(checksum)
        etag = f'W/"{dataset}:{checksum}"'
        return DatasetArtifacts(
            name=dataset,
            totals=totals,
            summary=summary or {},
            checksum=checksum,
            changed=changed,
            summary_path=SUMMARY_PATHS[dataset],
            geojson_path=GEOJSON_PATHS[dataset],
            version=version,
            etag=etag,
        )

    def _build_combined_dataset(
        self,
        ase: DatasetArtifacts,
        rlc: DatasetArtifacts,
        outputs_missing: bool,
    ) -> DatasetArtifacts:
        totals = merge_ward_totals(ase.totals, rlc.totals)
        checksum_input = f"{ase.checksum}:{rlc.checksum}".encode("utf-8")
        checksum = hashlib.sha256(checksum_input).hexdigest()
        state_entry = self.state.entry("cameras_combined")
        changed = (
            self.force_download
            or outputs_missing
            or ase.changed
            or rlc.changed
            or state_entry.get("checksum") != checksum
            or self.schema_mismatch
        )

        summary, _ = load_totals_from_summary("cameras_combined") if not changed else (None, None)
        if summary is None:
            summary = build_summary(totals)
            changed = True

        version = checksum_to_version(checksum)
        etag = f'W/"cameras_combined:{checksum}"'
        return DatasetArtifacts(
            name="cameras_combined",
            totals=totals,
            summary=summary,
            checksum=checksum,
            changed=changed,
            summary_path=SUMMARY_PATHS["cameras_combined"],
            geojson_path=GEOJSON_PATHS["cameras_combined"],
            version=version,
            etag=etag,
            extra_fields={
                "aseTicketCount": "ase_ticket_count",
                "rlcTicketCount": "rlc_ticket_count",
            },
        )

    # MARK: Output helpers

    def _write_dataset_outputs(self, ward_geojson: dict, artifact: DatasetArtifacts) -> bool:
        if not artifact.changed:
            return False

        self._upsert_ward_totals(artifact)
        geojson = self._build_geojson(ward_geojson, artifact.totals, artifact.extra_fields)
        self._save_json(artifact.geojson_path, geojson)
        self._save_json(artifact.summary_path, artifact.summary)
        self._maybe_push_to_redis(artifact, geojson)
        return True

    def _upsert_ward_totals(self, artifact: DatasetArtifacts) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute("DELETE FROM camera_ward_totals WHERE dataset = %s", (artifact.name,))
            for ward_code, bucket in artifact.totals.items():
                cursor.execute(
                    """
                    INSERT INTO camera_ward_totals (
                        dataset, ward_code, ward_name, ticket_count, location_count, total_revenue, metadata, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::JSONB, NOW())
                    """,
                    (
                        artifact.name,
                        ward_code,
                        bucket.get("ward_name") or f"Ward {ward_code}",
                        int(bucket.get("ticket_count", 0)),
                        int(bucket.get("location_count", 0)),
                        float(bucket.get("total_revenue", 0.0)),
                        json.dumps(
                            {
                                key: value
                                for key, value in bucket.items()
                                if key not in {"ward_name", "ticket_count", "location_count", "total_revenue"}
                            }
                        ),
                    ),
                )
        self.connection.commit()

    def _build_geojson(
        self,
        base_geojson: dict,
        totals: Dict[int, Dict[str, float]],
        extra_fields: Optional[Dict[str, str]] = None,
    ) -> dict:
        features: List[dict] = []
        for feature in base_geojson.get("features", []):
            properties = dict(feature.get("properties") or {})
            geometry = feature.get("geometry")
            if not geometry:
                continue
            try:
                ward_code = int(str(properties.get("AREA_LONG_CODE") or properties.get("AREA_SHORT_CODE")))
            except (TypeError, ValueError):
                continue
            stats = totals.get(ward_code, {})
            payload = {
                "wardCode": ward_code,
                "wardName": stats.get("ward_name") or properties.get("AREA_NAME") or f"Ward {ward_code}",
                "ticketCount": int(stats.get("ticket_count", 0)),
                "locationCount": int(stats.get("location_count", 0)),
                "totalRevenue": round(safe_number(stats.get("total_revenue")), 2),
            }
            if extra_fields:
                for key, source in extra_fields.items():
                    payload[key] = stats.get(source, 0)
            features.append(
                {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": payload,
                }
            )
        return {"type": "FeatureCollection", "features": features}

    def _save_json(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def _maybe_push_to_redis(self, artifact: DatasetArtifacts, geojson: dict) -> None:
        if not self.redis_url:
            return
        keys = REDIS_KEYS.get(artifact.name)
        if not keys:
            return
        try:
            import redis
        except ImportError:
            print("Redis library not available; skipping publish.")
            return

        client = redis.from_url(self.redis_url)
        updated_at = datetime.now(timezone.utc).isoformat()
        geojson_body = self._prepare_redis_payload(
            geojson,
            compress=True,
            version=artifact.version,
            etag=artifact.etag,
            updated_at=updated_at,
        )
        summary_body = self._prepare_redis_payload(
            artifact.summary,
            compress=False,
            version=artifact.version,
            etag=artifact.etag,
            updated_at=updated_at,
        )
        client.set(keys["geojson"], geojson_body)
        client.set(keys["summary"], summary_body)

    def _prepare_redis_payload(
        self,
        payload: dict,
        *,
        compress: bool,
        version: int,
        etag: str,
        updated_at: str,
    ) -> str:
        body = {
            "version": version,
            "updatedAt": updated_at,
            "data": gzip_and_encode(json.dumps(payload)) if compress else payload,
            "encoding": "gzip+base64" if compress else None,
            "etag": etag,
        }
        return json.dumps(body)

    def _update_state(
        self,
        ase: DatasetArtifacts,
        ase_path: Path,
        rlc: DatasetArtifacts,
        rlc_path: Path,
        combined: DatasetArtifacts,
    ) -> None:
        self.state.update_checksum("ase_locations", ase.checksum, source=str(ase_path))
        self.state.update_checksum("red_light_locations", rlc.checksum, source=str(rlc_path))
        self.state.update_checksum("cameras_combined", combined.checksum)
        self.state.save()
