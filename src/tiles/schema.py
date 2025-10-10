"""Schema management utilities for optimized vector tile delivery.

This module adds Web Mercator columns, helper functions, and partitioned tile
tables so that vector tile queries can avoid per-request reprojection or
runtime simplification.  All operations are idempotent and can be executed at
application start without requiring a full ETL re-run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

from src.etl.postgres import PostgresClient


BASE_POINT_TABLES: tuple[dict[str, str], ...] = (
    {
        "table": "parking_tickets",
        "geom": "geom",
        "geom_3857": "geom_3857",
    },
    {
        "table": "red_light_camera_locations",
        "geom": "geom",
        "geom_3857": "geom_3857",
    },
    {
        "table": "ase_camera_locations",
        "geom": "geom",
        "geom_3857": "geom_3857",
    },
    {
        "table": "red_light_charges",
        "geom": "geom",
        "geom_3857": "geom_3857",
    },
    {
        "table": "ase_charges",
        "geom": "geom",
        "geom_3857": "geom_3857",
    },
)


@dataclass
class TileSchemaManager:
    """Applies schema upgrades required for optimized tile generation."""

    pg: PostgresClient
    quadkey_zoom: int = 12
    quadkey_prefix_length: int = 6
    logger: Callable[[str], None] | None = None

    def ensure(self) -> None:
        """Apply all schema guarantees (idempotent)."""

        self._log("Ensuring PostGIS extensions")
        self.pg.ensure_extensions()
        self._log("Ensuring helper functions")
        self._ensure_helper_functions()
        self._log("Ensuring base columns and indexes")
        self._ensure_base_columns()
        self._log("Ensuring tile tables and partitions")
        self._ensure_tile_tables()

    # ------------------------------------------------------------------
    # Helpers

    def _log(self, message: str) -> None:
        if self.logger:
            self.logger(message)

    def _ensure_helper_functions(self) -> None:
        """Create SQL helper functions for quadkey and tile math."""

        self._log("  Creating mercator_quadkey_prefix function")
        self.pg.execute(
            """
            CREATE OR REPLACE FUNCTION mercator_quadkey_prefix(
                input geometry,
                zoom integer,
                prefix_length integer
            ) RETURNS text AS $$
            DECLARE
                lon float8;
                lat float8;
                tile_x bigint;
                tile_y bigint;
                i integer;
                digit integer;
                quadkey text := '';
                point_4326 geometry;
                max_zoom integer := GREATEST(1, zoom);
                effective_prefix integer := LEAST(GREATEST(prefix_length, 1), max_zoom);
            BEGIN
                IF input IS NULL THEN
                    RETURN NULL;
                END IF;
                point_4326 := ST_Transform(
                    ST_SetSRID(ST_Centroid(input), COALESCE(ST_SRID(input), 4326)),
                    4326
                );
                lon := ST_X(point_4326);
                lat := ST_Y(point_4326);
                lon := GREATEST(LEAST(lon, 180.0), -180.0);
                lat := GREATEST(LEAST(lat, 85.0511287798), -85.0511287798);

                tile_x := floor(((lon + 180.0) / 360.0) * power(2, max_zoom));
                tile_y := floor(
                    (
                        1.0 - ln(tan(radians(lat)) + (1.0 / cos(radians(lat)))) / pi()
                    ) / 2.0 * power(2, max_zoom)
                );

                FOR i IN REVERSE 1..max_zoom LOOP
                    digit := 0;
                    IF ((tile_x >> (i - 1)) & 1) = 1 THEN
                        digit := digit + 1;
                    END IF;
                    IF ((tile_y >> (i - 1)) & 1) = 1 THEN
                        digit := digit + 2;
                    END IF;
                    quadkey := quadkey || digit::text;
                END LOOP;

                RETURN SUBSTRING(quadkey FROM 1 FOR effective_prefix);
            END;
            $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
            """
        )

        # Function to compute tile bounds in Web Mercator for a given xyz.
        self._log("  Creating tile_envelope_3857 function")
        self.pg.execute(
            """
            CREATE OR REPLACE FUNCTION tile_envelope_3857(z integer, x integer, y integer)
            RETURNS geometry AS $$
            BEGIN
                RETURN ST_SetSRID(ST_TileEnvelope(z, x, y), 3857);
            END;
            $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
            """
        )

    def _ensure_base_columns(self) -> None:
        """Add Web Mercator columns and indexes to foundational tables."""

        for table_meta in BASE_POINT_TABLES:
            table = table_meta["table"]
            geom = table_meta["geom"]
            geom_3857 = table_meta["geom_3857"]

            self._log(f"  Processing base table '{table}'")
            self._log(f"    Adding {geom_3857} column if missing")
            self.pg.execute(
                f"""
                ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS {geom_3857} geometry(Point, 3857)
                """
            )

            # Ensure all rows have Web Mercator geometry populated.
            self._log(f"    Populating {geom_3857} where empty or invalid")
            updated_geom = self.pg.execute(
                f"""
                UPDATE {table}
                SET {geom_3857} = ST_Transform({geom}, 3857)
                WHERE {geom} IS NOT NULL
                  AND ({geom_3857} IS NULL OR ST_SRID({geom_3857}) <> 3857)
                """
            )
            if updated_geom:
                self._log(f"      Updated {updated_geom} rows")

            self._log(f"    Ensuring GIST index on {geom_3857}")
            self.pg.execute(
                f"CREATE INDEX IF NOT EXISTS {table}_geom_3857_idx ON {table} USING GIST ({geom_3857});"
            )

            # Tile prefix column to support pruning.
            self._log("    Adding tile_qk_prefix column if missing")
            self.pg.execute(
                f"""
                ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS tile_qk_prefix TEXT
                """
            )
            self._log("    Populating tile_qk_prefix where empty")
            updated_prefix = self.pg.execute(
                f"""
                UPDATE {table}
                SET tile_qk_prefix = mercator_quadkey_prefix({geom_3857}, {self.quadkey_zoom}, {self.quadkey_prefix_length})
                WHERE {geom_3857} IS NOT NULL
                  AND (tile_qk_prefix IS NULL OR tile_qk_prefix = '')
                """
            )
            if updated_prefix:
                self._log(f"      Updated {updated_prefix} rows")
            self.pg.execute(
                f"CREATE INDEX IF NOT EXISTS {table}_tile_qk_prefix_idx ON {table} (tile_qk_prefix);"
            )

    def _ensure_tile_tables(self) -> None:
        """Create partitioned tile tables populated from the base tables."""

        builders: Iterable[tuple[str, str, str]] = (
            (
                "parking_ticket_tiles",
                "parking_tickets",
                """
                    WITH ranked AS (
                        SELECT
                            COALESCE(centreline_id::text, street_normalized, location1, ticket_hash) AS feature_id,
                            geom_3857,
                            street_normalized,
                            centreline_id,
                            COUNT(*) OVER (PARTITION BY COALESCE(centreline_id::text, street_normalized, location1, ticket_hash)) AS ticket_count,
                            SUM(COALESCE(set_fine_amount, 0)) OVER (PARTITION BY COALESCE(centreline_id::text, street_normalized, location1, ticket_hash)) AS total_fines,
                            ROW_NUMBER() OVER (PARTITION BY COALESCE(centreline_id::text, street_normalized, location1, ticket_hash) ORDER BY date_of_infraction DESC NULLS LAST, time_of_infraction DESC NULLS LAST) AS rn
                        FROM parking_tickets
                        WHERE geom_3857 IS NOT NULL
                    ), aggregated AS (
                        SELECT
                            feature_id,
                            ticket_count,
                            total_fines,
                            geom_3857,
                            street_normalized,
                            centreline_id
                        FROM ranked
                        WHERE rn = 1
                    )
                    SELECT
                        'parking_tickets' AS dataset,
                        agg.feature_id,
                        variants.min_zoom,
                        variants.max_zoom,
                        mercator_quadkey_prefix(parts.geom, {self.quadkey_zoom}, {self.quadkey_prefix_length}) AS tile_qk_prefix,
                        SUBSTRING(mercator_quadkey_prefix(parts.geom, {self.quadkey_zoom}, {self.quadkey_prefix_length}) FROM 1 FOR 1) AS tile_qk_group,
                        parts.geom,
                        agg.ticket_count,
                        agg.total_fines,
                        agg.street_normalized,
                        agg.centreline_id::BIGINT,
                        NULL::TEXT AS location_name,
                        NULL::TEXT AS location,
                        NULL::TEXT AS status,
                        NULL::TEXT AS ward
                    FROM aggregated AS agg
                    CROSS JOIN LATERAL (
                        VALUES
                            (0, 10, ST_Subdivide(ST_SimplifyPreserveTopology(agg.geom_3857, 25), 32)),
                            (11, 16, ST_Subdivide(agg.geom_3857, 4))
                    ) AS variants(min_zoom, max_zoom, geom_variant)
                    CROSS JOIN LATERAL (
                        SELECT (ST_Dump(geom_variant)).geom
                    ) AS parts
                """,
            ),
            (
                "red_light_camera_tiles",
                "red_light_camera_locations",
                """
                    SELECT
                        'red_light_locations' AS dataset,
                        base.intersection_id AS feature_id,
                        variants.min_zoom,
                        variants.max_zoom,
                        mercator_quadkey_prefix(parts.geom, {self.quadkey_zoom}, {self.quadkey_prefix_length}) AS tile_qk_prefix,
                        SUBSTRING(mercator_quadkey_prefix(parts.geom, {self.quadkey_zoom}, {self.quadkey_prefix_length}) FROM 1 FOR 1) AS tile_qk_group,
                        parts.geom,
                        base.ticket_count,
                        base.total_fine_amount,
                        NULL::TEXT AS street_normalized,
                        NULL::BIGINT AS centreline_id,
                        base.location_name,
                        base.location_name AS location,
                        NULL::TEXT AS status,
                        base.ward_1 AS ward
                    FROM (
                        SELECT
                            intersection_id,
                            location_name,
                            ticket_count,
                            total_fine_amount,
                            ward_1,
                            geom_3857
                        FROM red_light_camera_locations
                        WHERE geom_3857 IS NOT NULL
                    ) AS base
                    CROSS JOIN LATERAL (
                        VALUES
                            (0, 11, ST_Subdivide(ST_SimplifyPreserveTopology(base.geom_3857, 30), 32)),
                            (12, 16, ST_Subdivide(base.geom_3857, 4))
                    ) AS variants(min_zoom, max_zoom, geom_variant)
                    CROSS JOIN LATERAL (
                        SELECT (ST_Dump(geom_variant)).geom
                    ) AS parts
                """,
            ),
            (
                "ase_camera_tiles",
                "ase_camera_locations",
                """
                    SELECT
                        'ase_locations' AS dataset,
                        base.location_code AS feature_id,
                        variants.min_zoom,
                        variants.max_zoom,
                        mercator_quadkey_prefix(parts.geom, {self.quadkey_zoom}, {self.quadkey_prefix_length}) AS tile_qk_prefix,
                        SUBSTRING(mercator_quadkey_prefix(parts.geom, {self.quadkey_zoom}, {self.quadkey_prefix_length}) FROM 1 FOR 1) AS tile_qk_group,
                        parts.geom,
                        base.ticket_count,
                        base.total_fine_amount,
                        NULL::TEXT AS street_normalized,
                        NULL::BIGINT AS centreline_id,
                        NULL::TEXT AS location_name,
                        base.location,
                        base.status,
                        base.ward
                    FROM (
                        SELECT
                            location_code,
                            location,
                            ticket_count,
                            total_fine_amount,
                            status,
                            ward,
                            geom_3857
                        FROM ase_camera_locations
                        WHERE geom_3857 IS NOT NULL
                    ) AS base
                    CROSS JOIN LATERAL (
                        VALUES
                            (0, 11, ST_Subdivide(ST_SimplifyPreserveTopology(base.geom_3857, 30), 32)),
                            (12, 16, ST_Subdivide(base.geom_3857, 4))
                    ) AS variants(min_zoom, max_zoom, geom_variant)
                    CROSS JOIN LATERAL (
                        SELECT (ST_Dump(geom_variant)).geom
                    ) AS parts
                """,
            ),
        )

        for table_name, base_table, populate_sql in builders:
            self._log(f"  Ensuring tile table '{table_name}' (source: {base_table})")
            parent_definition = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    tile_id BIGSERIAL PRIMARY KEY,
                    dataset TEXT NOT NULL,
                    feature_id TEXT NOT NULL,
                    min_zoom INTEGER NOT NULL,
                    max_zoom INTEGER NOT NULL,
                    tile_qk_prefix TEXT NOT NULL,
                    tile_qk_group TEXT NOT NULL,
                    geom geometry(GEOMETRY, 3857) NOT NULL,
                    ticket_count BIGINT,
                    total_fine_amount NUMERIC,
                    street_normalized TEXT,
                    centreline_id BIGINT,
                    location_name TEXT,
                    location TEXT,
                    status TEXT,
                    ward TEXT
                ) PARTITION BY LIST (tile_qk_group);
            """
            self._log("    Creating parent partitioned table if needed")
            self.pg.execute(parent_definition)

            self._ensure_quadkey_partitions(table_name)

            # Clear and repopulate to avoid duplicate entries.
            self._log("    Truncating existing tile rows")
            self.pg.execute(f"TRUNCATE {table_name} RESTART IDENTITY;")
            self._log("    Populating tile table")
            inserted_rows = self.pg.execute(
                f"""
                INSERT INTO {table_name} (
                    dataset,
                    feature_id,
                    min_zoom,
                    max_zoom,
                    tile_qk_prefix,
                    tile_qk_group,
                    geom,
                    ticket_count,
                    total_fine_amount,
                    street_normalized,
                    centreline_id,
                    location_name,
                    location,
                    status,
                    ward
                )
                {populate_sql}
                """
            )
            if inserted_rows:
                self._log(f"      Inserted {inserted_rows} rows into {table_name}")

            self._log("    Creating indexes")
            self.pg.execute(
                f"CREATE INDEX IF NOT EXISTS {table_name}_geom_idx ON {table_name} USING GIST (geom);"
            )
            self.pg.execute(
                f"CREATE INDEX IF NOT EXISTS {table_name}_zoom_idx ON {table_name} (min_zoom, max_zoom);"
            )
            self.pg.execute(
                f"CREATE INDEX IF NOT EXISTS {table_name}_prefix_idx ON {table_name} (tile_qk_prefix);"
            )

    def _ensure_quadkey_partitions(self, parent: str) -> None:
        """Ensure four list partitions (0-3) exist for the given parent table."""

        for symbol in ("0", "1", "2", "3"):
            partition_name = f"{parent}_p{symbol}"
            self._log(f"    Ensuring partition {partition_name}")
            self.pg.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {partition_name}
                PARTITION OF {parent}
                FOR VALUES IN ('{symbol}')
                """
            )
            self.pg.execute(
                f"CREATE INDEX IF NOT EXISTS {partition_name}_geom_idx ON {partition_name} USING GIST (geom);"
            )


__all__ = ["TileSchemaManager"]
