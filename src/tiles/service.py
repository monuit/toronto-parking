"""Vector tile generation using PostGIS with pre-simplified tile tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..etl.postgres import PostgresClient
from .schema import TileSchemaManager


TILE_DATASET_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "parking_tickets": {
        "table": "parking_ticket_tiles",
        "geom_column": "geom",
        "min_zoom_column": "min_zoom",
        "max_zoom_column": "max_zoom",
        "tile_prefix_column": "tile_qk_prefix",
        "tile_group_column": "tile_qk_group",
        "attributes": [
            "dataset",
            "feature_id",
            "ticket_count",
            "total_fine_amount",
            "street_normalized",
            "centreline_id",
        ],
    },
    "red_light_locations": {
        "table": "red_light_camera_tiles",
        "geom_column": "geom",
        "min_zoom_column": "min_zoom",
        "max_zoom_column": "max_zoom",
        "tile_prefix_column": "tile_qk_prefix",
        "tile_group_column": "tile_qk_group",
        "attributes": [
            "dataset",
            "feature_id",
            "location_name",
            "ticket_count",
            "total_fine_amount",
        ],
    },
    "ase_locations": {
        "table": "ase_camera_tiles",
        "geom_column": "geom",
        "min_zoom_column": "min_zoom",
        "max_zoom_column": "max_zoom",
        "tile_prefix_column": "tile_qk_prefix",
        "tile_group_column": "tile_qk_group",
        "attributes": [
            "dataset",
            "feature_id",
            "location",
            "status",
            "ward",
            "ticket_count",
            "total_fine_amount",
        ],
    },
}


@dataclass
class TileService:
    pg: PostgresClient
    quadkey_prefix_length: int = 6

    def __post_init__(self) -> None:  # pragma: no cover - integration hook
        if not getattr(TileService, "_schema_initialized", False):
            TileSchemaManager(
                self.pg,
                quadkey_prefix_length=self.quadkey_prefix_length,
            ).ensure(include_tile_tables=False)
            TileService._schema_initialized = True

    def get_tile(
        self,
        dataset: str,
        z: int,
        x: int,
        y: int,
        *,
        filters: Optional[Dict[str, Any]] = None,  # noqa: ARG002 - retained for API parity
    ) -> bytes | None:
        definition = TILE_DATASET_DEFINITIONS.get(dataset)
        if not definition:
            raise ValueError(f"Dataset '{dataset}' is not configured for tiles")

        quadkey = _quadkey_prefix_from_tile(z, x, y)
        quadkey_prefix = quadkey[: self.quadkey_prefix_length]

        attribute_sql = ", ".join(definition["attributes"])
        where_clauses = [
            f"data.{definition['geom_column']} && bounds.geom",
            f"%s BETWEEN data.{definition['min_zoom_column']} AND data.{definition['max_zoom_column']}",
        ]
        params = [z, x, y, z]

        if quadkey_prefix:
            where_clauses.append(f"data.{definition['tile_group_column']} = %s")
            params.append(quadkey_prefix[0])
            where_clauses.append(f"data.{definition['tile_prefix_column']} LIKE %s")
            params.append(f"{quadkey_prefix}%")

        sql = f"""
            WITH bounds AS (
                SELECT tile_envelope_3857(%s, %s, %s) AS geom
            ), features AS (
                SELECT
                    ST_AsMVTGeom(
                        data.{definition['geom_column']},
                        bounds.geom,
                        4096,
                        64,
                        true
                    ) AS geom,
                    {attribute_sql}
                FROM {definition['table']} AS data
                CROSS JOIN bounds
                WHERE {' AND '.join(where_clauses)}
            )
            SELECT ST_AsMVT(features, %s, 4096, 'geom') FROM features;
        """

        params.append(dataset)
        row = self.pg.fetch_one(sql, tuple(params))
        if not row or row[0] is None:
            return None
        return bytes(row[0])


def _quadkey_prefix_from_tile(z: int, x: int, y: int) -> str:
    """Compute the quadkey string for the XYZ tile coordinates."""

    if z <= 0:
        return ""
    prefix_chars: list[str] = []
    for i in range(z, 0, -1):
        mask = 1 << (i - 1)
        digit = 0
        if x & mask:
            digit += 1
        if y & mask:
            digit += 2
        prefix_chars.append(str(digit))
    return "".join(prefix_chars)


TileService._schema_initialized = False  # type: ignore[attr-defined]


__all__ = ["TileService"]
