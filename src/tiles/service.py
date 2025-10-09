"""Vector tile generation using PostGIS."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..etl.postgres import PostgresClient


DATASET_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "parking_tickets": {
        "table": "parking_tickets",
        "geom_column": "geom",
        "attributes": [
            "date_of_infraction",
            "infraction_code",
            "set_fine_amount",
            "street_normalized",
            "centreline_id",
            ("years", "ARRAY[EXTRACT(YEAR FROM data.date_of_infraction)::INT]"),
            ("months", "ARRAY[EXTRACT(MONTH FROM data.date_of_infraction)::INT]"),
        ],
        "date_field": "date_of_infraction",
    },
    "red_light_locations": {
        "table": "red_light_camera_locations",
        "geom_column": "geom",
        "attributes": [
            "location_name",
            "linear_name_full_1",
            "linear_name_full_2",
            "ward_1",
            "police_division_1",
            "activation_date",
            "ticket_count",
            ("total_fine_amount", "COALESCE(data.total_fine_amount, 0)"),
            ("years", "COALESCE(data.years, ARRAY[]::INT[])"),
            ("months", "COALESCE(data.months, ARRAY[]::INT[])"),
            "yearly_counts",
        ],
        "date_field": None,
    },
    "ase_locations": {
        "table": "ase_camera_locations",
        "geom_column": "geom",
        "attributes": [
            "ward",
            "status",
            "location",
            "ticket_count",
            ("total_fine_amount", "COALESCE(data.total_fine_amount, 0)"),
            ("years", "COALESCE(data.years, ARRAY[]::INT[])"),
            ("months", "COALESCE(data.months, ARRAY[]::INT[])"),
            "yearly_counts",
        ],
        "date_field": None,
    },
}


@dataclass
class TileService:
    pg: PostgresClient

    def get_tile(self, dataset: str, z: int, x: int, y: int, *, filters: Optional[Dict[str, Any]] = None) -> bytes | None:
        self.pg.ensure_extensions()
        definition = DATASET_DEFINITIONS.get(dataset)
        if not definition:
            raise ValueError(f"Dataset '{dataset}' is not configured for tiles")

        bounds_clause = f"{definition['geom_column']} && bounds.geom"
        where_clauses = [bounds_clause]

        if filters:
            date_from = filters.get("date_from")
            date_to = filters.get("date_to")
            date_field = definition.get("date_field")
            if date_from and date_field:
                where_clauses.append(f"{date_field} >= '{date_from}'")
            if date_to and date_field:
                where_clauses.append(f"{date_field} <= '{date_to}'")

        table_alias = "data"
        where_sql = " AND ".join(
            clause if clause != bounds_clause else f"{table_alias}.{definition['geom_column']} && bounds.geom"
            for clause in where_clauses
        )
        attribute_expressions: List[str] = []
        for attr in definition["attributes"]:
            if isinstance(attr, tuple):
                alias, expression = attr
                attribute_expressions.append(f"{expression} AS {alias}")
            else:
                attribute_expressions.append(f"{table_alias}.{attr}")

        attribute_list = ", ".join(attribute_expressions)

        min_lng, min_lat, max_lng, max_lat = _tile_bounds(z, x, y)
        bounds_wkt = _bounds_wkt(min_lng, min_lat, max_lng, max_lat)

        sql = f"""
            WITH bounds AS (
                SELECT ST_GeomFromText(%s, 4326) AS geom
            ), tile AS (
                SELECT
                    ST_AsMVTGeom(
                        {table_alias}.{definition['geom_column']},
                        bounds.geom,
                        4096,
                        64,
                        true
                    ) AS geom,
                    {attribute_list}
                FROM {definition['table']} AS {table_alias}
                CROSS JOIN bounds
                WHERE {where_sql}
            )
            SELECT ST_AsMVT(tile, %s, 4096, 'geom') FROM tile;
        """

        row = self.pg.fetch_one(sql, (bounds_wkt, dataset))
        if not row or row[0] is None:
            return None
        return bytes(row[0])


def _tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    n = 2 ** z
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0

    def lat(tile_y: int) -> float:
        t = math.pi * (1 - 2 * tile_y / n)
        return math.degrees(math.atan(math.sinh(t)))

    lat_max = lat(y)
    lat_min = lat(y + 1)
    return lon_min, lat_min, lon_max, lat_max


def _bounds_wkt(min_lng: float, min_lat: float, max_lng: float, max_lat: float) -> str:
    return (
        "POLYGON(("
        f"{min_lng} {min_lat},"
        f"{max_lng} {min_lat},"
        f"{max_lng} {max_lat},"
        f"{min_lng} {max_lat},"
        f"{min_lng} {min_lat}"
        "))"
    )


__all__ = ["TileService"]
