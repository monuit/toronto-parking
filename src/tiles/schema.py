"""Schema management utilities for optimized vector tile delivery.

This module adds Web Mercator columns, helper functions, and partitioned tile
tables so that vector tile queries can avoid per-request reprojection or
runtime simplification.  All operations are idempotent and can be executed at
application start without requiring a full ETL re-run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Tuple

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
    quadkey_zoom: int = 16
    quadkey_prefix_length: int = 16
    logger: Callable[[str], None] | None = None

    def ensure(self, *, include_tile_tables: bool = True) -> None:
        """Apply schema guarantees (idempotent).

        Parameters
        ----------
        include_tile_tables:
            When ``True`` (default) the legacy ``*_tiles`` partitioned tables are
            rebuilt.  Set to ``False`` to skip that expensive step when relying on
            streaming tile generation instead of precomputed tables.
        """

        self.ensure_helpers()
        self._log("Ensuring base columns and indexes")
        self._ensure_base_columns()
        if include_tile_tables:
            self._log("Ensuring tile tables and partitions")
            self._ensure_tile_tables()
        else:
            self._log("Skipping tile table rebuild (include_tile_tables=False)")

    # ------------------------------------------------------------------
    # Helpers
    def _log(self, message: str) -> None:
        if self.logger:
            self.logger(message)

    def ensure_helpers(self) -> None:
        self._log("Ensuring PostGIS extensions")
        self.pg.ensure_extensions()
        self._log("Ensuring helper functions")
        self._ensure_helper_functions()
        self._log("Ensuring tile batch functions")
        self._ensure_tile_fetch_functions()
        self._log("Ensuring glow vector tile support")
        self._ensure_glow_support()

    def _quote_ident(self, value: str) -> str:
        return f'"{str(value).replace("\"", "\"\"")}"'

    def _column_exists(self, table: str, column: str) -> bool:
        sql = """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
        """
        return self.pg.fetch_one(sql, (table, column)) is not None

    def _column_has_data(self, table: str, column: str, *, treat_blank_as_null: bool = False) -> bool:
        conditions = [f"{self._quote_ident(column)} IS NOT NULL"]
        if treat_blank_as_null:
            conditions.append(f"{self._quote_ident(column)} <> ''")
        predicate = " AND ".join(conditions)
        sql = f"SELECT EXISTS (SELECT 1 FROM {self._quote_ident(table)} WHERE {predicate} LIMIT 1)"
        row = self.pg.fetch_one(sql)
        return bool(row[0]) if row else False

    def _get_column_metadata(self, table: str) -> List[Tuple[str, bool, Optional[str]]]:
        rows = self.pg.fetch_all(
            """
            SELECT column_name, is_nullable = 'NO' AS not_null, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [(row[0], bool(row[1]), row[2]) for row in rows]

    def _get_primary_key_info(self, table: str) -> Optional[Tuple[str, List[str]]]:
        rows = self.pg.fetch_all(
            """
            SELECT tc.constraint_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """,
            (table,),
        )
        if not rows:
            return None
        constraint = rows[0][0]
        columns = [row[1] for row in rows]
        return constraint, columns

    def _get_index_definitions(self, table: str) -> List[Tuple[str, str]]:
        rows = self.pg.fetch_all(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = %s
            """,
            (table,),
        )
        return [(row[0], row[1]) for row in rows]

    def _rebuild_base_table(self, table_meta: dict[str, str]) -> None:
        table = table_meta["table"]
        geom_column = table_meta["geom"]
        geom_3857_column = table_meta["geom_3857"]
        tile_prefix_column = "tile_qk_prefix"

        column_metadata = self._get_column_metadata(table)
        existing_columns = [name for name, _, _ in column_metadata if name not in (geom_3857_column, tile_prefix_column)]
        not_null_columns = [name for name, required, _ in column_metadata if required and name in existing_columns]
        defaults = {name: default for name, _, default in column_metadata if default is not None and name in existing_columns}
        primary_key = self._get_primary_key_info(table)
        index_definitions = self._get_index_definitions(table)

        select_columns = ",\n                ".join(self._quote_ident(column) for column in existing_columns)
        tmp_table = f"{table}__geom_refresh_tmp"
        backup_table = f"{table}__geom_refresh_old"

        self._log(f"    Creating temporary replacement table '{tmp_table}'")
        self.pg.execute(f"DROP TABLE IF EXISTS {self._quote_ident(tmp_table)} CASCADE")
        self.pg.execute(f"DROP TYPE IF EXISTS {self._quote_ident(tmp_table)} CASCADE")

        create_sql = f"""
            CREATE TABLE {self._quote_ident(tmp_table)} AS
            WITH base AS (
                SELECT
                    {select_columns},
                    ST_Transform({self._quote_ident(geom_column)}, 3857) AS {self._quote_ident(geom_3857_column)}
                FROM {self._quote_ident(table)}
            )
            SELECT
                base.*,
                CASE
                    WHEN base.{self._quote_ident(geom_3857_column)} IS NULL THEN NULL
                    ELSE mercator_quadkey_prefix(base.{self._quote_ident(geom_3857_column)}, {self.quadkey_zoom}, {self.quadkey_prefix_length})
                END AS {self._quote_ident(tile_prefix_column)}
            FROM base;
        """
        self.pg.execute(create_sql)

        self._log(f"    Swapping new table into place for '{table}'")
        self.pg.execute(f"DROP TABLE IF EXISTS {self._quote_ident(backup_table)} CASCADE")
        self.pg.execute(f"ALTER TABLE {self._quote_ident(table)} RENAME TO {self._quote_ident(backup_table)}")
        self.pg.execute(f"ALTER TABLE {self._quote_ident(tmp_table)} RENAME TO {self._quote_ident(table)}")
        self.pg.execute(f"DROP TABLE {self._quote_ident(backup_table)} CASCADE")

        self._log("    Restoring column defaults and nullability")
        for column, default in defaults.items():
            self.pg.execute(
                f"ALTER TABLE {self._quote_ident(table)} ALTER COLUMN {self._quote_ident(column)} SET DEFAULT {default}"
            )
        for column in not_null_columns:
            self.pg.execute(
                f"ALTER TABLE {self._quote_ident(table)} ALTER COLUMN {self._quote_ident(column)} SET NOT NULL"
            )

        if primary_key:
            constraint, columns = primary_key
            cols_sql = ", ".join(self._quote_ident(col) for col in columns)
            self._log(f"    Reinstating primary key {constraint}")
            self.pg.execute(
                f"ALTER TABLE {self._quote_ident(table)} ADD CONSTRAINT {self._quote_ident(constraint)} PRIMARY KEY ({cols_sql})"
            )

        self._log("    Recreating secondary indexes")
        pk_name = primary_key[0] if primary_key else None
        for index_name, index_def in index_definitions:
            if pk_name and index_name == pk_name:
                continue
            self.pg.execute(index_def)

        self.pg.execute(
            f"CREATE INDEX IF NOT EXISTS {self._quote_ident(f'{table}_geom_3857_idx')} ON {self._quote_ident(table)} USING GIST ({self._quote_ident(geom_3857_column)});"
        )
        self.pg.execute(
            f"CREATE INDEX IF NOT EXISTS {self._quote_ident(f'{table}_tile_qk_prefix_idx')} ON {self._quote_ident(table)} ({self._quote_ident(tile_prefix_column)});"
        )

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
                max_zoom integer := GREATEST(1, zoom);
                effective_prefix integer := LEAST(GREATEST(prefix_length, 1), max_zoom);
                quadkey text;
                srid integer;
            BEGIN
                IF input IS NULL THEN
                    RETURN NULL;
                END IF;

                srid := COALESCE(NULLIF(ST_SRID(input), 0), 3857);

                WITH point AS (
                    SELECT ST_Transform(
                        ST_SetSRID(ST_Centroid(input), srid),
                        4326
                    ) AS geom
                ), coords AS (
                    SELECT
                        GREATEST(LEAST(ST_X(geom), 180.0), -180.0) AS lon,
                        GREATEST(LEAST(ST_Y(geom), 85.0511287798), -85.0511287798) AS lat
                    FROM point
                ), tiles AS (
                    SELECT
                        floor(((lon + 180.0) / 360.0) * power(2, max_zoom))::bigint AS tile_x,
                        floor(((1.0 - ln(tan(radians(lat)) + (1.0 / cos(radians(lat)))) / pi()) / 2.0 * power(2, max_zoom)))::bigint AS tile_y
                    FROM coords
                ), digits AS (
                    SELECT
                        ((tile_x >> (i - 1)) & 1) + 2 * ((tile_y >> (i - 1)) & 1) AS digit,
                        i
                    FROM tiles,
                    LATERAL generate_series(max_zoom, 1, -1) AS gs(i)
                )
                SELECT string_agg(digit::text, '' ORDER BY i DESC)
                INTO quadkey
                FROM digits;

                RETURN SUBSTRING(COALESCE(quadkey, '') FROM 1 FOR effective_prefix);
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

    def _ensure_tile_fetch_functions(self) -> None:
        prefix_len = self.quadkey_prefix_length
        quadkey_zoom = self.quadkey_zoom

        self._log("  Ensuring tile_blob_cache table")
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS public.tile_blob_cache (
                dataset text NOT NULL,
                z integer NOT NULL,
                x integer NOT NULL,
                y integer NOT NULL,
                mvt bytea NOT NULL,
                PRIMARY KEY (dataset, z, x, y)
            )
            """
        )
        self.pg.execute(
            """
            CREATE INDEX IF NOT EXISTS tile_blob_cache_dataset_z_idx
            ON public.tile_blob_cache (dataset, z)
            """
        )

        self._log("  Creating get_parking_tiles function")
        self.pg.execute(
            f"""
            CREATE OR REPLACE FUNCTION public.get_parking_tiles(
                zs integer[],
                xs integer[],
                ys integer[]
            ) RETURNS TABLE (z integer, x integer, y integer, mvt bytea)
            LANGUAGE sql
            STABLE
            PARALLEL SAFE
            AS
            $$
            WITH req AS (
                SELECT ord, z, x, y
                FROM unnest(zs, xs, ys) WITH ORDINALITY AS t(z, x, y, ord)
            ), bounds AS (
                SELECT ord, z, x, y, ST_SetSRID(ST_TileEnvelope(z, x, y), 3857) AS geom
                FROM req
            ), pref AS (
                SELECT
                    r.ord,
                    r.z,
                    r.x,
                    r.y,
                    mercator_quadkey_prefix(b.geom, {quadkey_zoom}, {prefix_len}) AS prefix_full,
                    mercator_quadkey_prefix(b.geom, {quadkey_zoom}, r.z) AS prefix_zoom,
                    SUBSTRING(mercator_quadkey_prefix(b.geom, {quadkey_zoom}, {prefix_len}) FROM 1 FOR 1) AS grp
                FROM req r
                JOIN bounds b USING (ord, z, x, y)
            )
            SELECT
                p.z,
                p.x,
                p.y,
                CASE
                    WHEN cache.mvt IS NOT NULL THEN cache.mvt
                    ELSE (
                        SELECT ST_AsMVT(mvt_rows, 'parking_tickets', 4096, 'geom')
                        FROM (
                            SELECT
                                ST_AsMVTGeom(t.geom, b.geom, 4096, 64, true) AS geom,
                                t.feature_id,
                                t.ticket_count,
                                t.total_fine_amount,
                                t.street_normalized,
                                t.centreline_id
                            FROM parking_ticket_tiles t
                            WHERE t.dataset = 'parking_tickets'
                              AND t.min_zoom <= p.z
                              AND t.max_zoom >= p.z
                              AND t.tile_qk_group = p.grp
                          AND t.tile_qk_prefix LIKE p.prefix_zoom || '%%'
                              AND t.geom && b.geom
                        ) AS mvt_rows
                    )
                END AS mvt
            FROM pref p
            JOIN bounds b USING (ord, z, x, y)
            LEFT JOIN LATERAL (
                SELECT mvt
                FROM public.tile_blob_cache cache
                WHERE cache.dataset = 'parking_tickets'
                  AND cache.z = p.z
                  AND cache.x = p.x
                  AND cache.y = p.y
            ) AS cache ON p.z <= 10
            WHERE p.prefix_full IS NOT NULL
            $$;
            """
        )

    def _ensure_glow_support(self) -> None:
        self._log("  Ensuring glow_lines table")
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS public.glow_lines (
                dataset TEXT NOT NULL,
                centreline_id BIGINT NOT NULL,
                count INTEGER NOT NULL,
                years_mask INTEGER NOT NULL,
                months_mask INTEGER NOT NULL,
                geom geometry(MultiLineString, 4326) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (dataset, centreline_id)
            );
            """
        )
        self.pg.execute(
            """
            CREATE INDEX IF NOT EXISTS glow_lines_geom_idx
            ON public.glow_lines USING GIST (geom);
            """
        )
        self.pg.execute(
            """
            CREATE INDEX IF NOT EXISTS glow_lines_dataset_count_idx
            ON public.glow_lines (dataset, count DESC);
            """
        )

        self._log("  Creating get_glow_tile function")
        self.pg.execute(
            """
            CREATE OR REPLACE FUNCTION public.get_glow_tile(
                p_dataset TEXT,
                p_z INTEGER,
                p_x INTEGER,
                p_y INTEGER
            )
            RETURNS BYTEA
            LANGUAGE SQL
            STABLE
            AS $$
                WITH bounds AS (
                    SELECT ST_TileEnvelope(p_z, p_x, p_y) AS geom
                ), metrics AS (
                    SELECT
                        geom AS tile_geom,
                        (ST_XMax(geom) - ST_XMin(geom)) AS tile_width
                    FROM bounds
                ), buffered AS (
                    SELECT
                        tile_geom,
                        tile_width,
                        ST_Buffer(tile_geom, (tile_width / 4096.0) * 32.0) AS geom
                    FROM metrics
                ), source AS (
                    SELECT
                        gl.centreline_id,
                        gl.count,
                        gl.years_mask,
                        gl.months_mask,
                        CASE
                            WHEN p_z <= 9 THEN ST_SimplifyVW(gl.geom_3857, tile_width / 6.0)
                            WHEN p_z <= 11 THEN ST_SimplifyVW(gl.geom_3857, tile_width / 12.0)
                            WHEN p_z <= 13 THEN ST_SimplifyVW(gl.geom_3857, tile_width / 24.0)
                            WHEN p_z <= 15 THEN ST_SimplifyVW(gl.geom_3857, tile_width / 48.0)
                            ELSE gl.geom_3857
                        END AS geom_3857
                    FROM (
                        SELECT
                            centreline_id,
                            count,
                            years_mask,
                            months_mask,
                            ST_Transform(geom, 3857) AS geom_3857
                        FROM public.glow_lines
                        WHERE dataset = p_dataset
                    ) AS gl
                    CROSS JOIN buffered b
                    WHERE ST_Intersects(gl.geom_3857, b.geom)
                ), clipped AS (
                    SELECT
                        centreline_id,
                        count,
                        years_mask,
                        months_mask,
                        ST_AsMVTGeom(
                            geom_3857,
                            (SELECT tile_geom FROM metrics LIMIT 1),
                            4096,
                            32,
                            TRUE
                        ) AS geom
                    FROM source
                    WHERE geom_3857 IS NOT NULL
                )
                SELECT COALESCE(
                    (SELECT ST_AsMVT(clipped, 'glow_lines', 4096, 'geom') FROM clipped),
                    '\\x'::BYTEA
                );
            $$;
            """
        )

        self._log("  Creating get_red_light_tiles function")
        self.pg.execute(
            f"""
            CREATE OR REPLACE FUNCTION public.get_red_light_tiles(
                zs integer[],
                xs integer[],
                ys integer[]
            ) RETURNS TABLE (z integer, x integer, y integer, mvt bytea)
            LANGUAGE sql
            STABLE
            PARALLEL SAFE
            AS
            $$
            WITH req AS (
                SELECT ord, z, x, y
                FROM unnest(zs, xs, ys) WITH ORDINALITY AS t(z, x, y, ord)
            ), bounds AS (
                SELECT ord, z, x, y, ST_SetSRID(ST_TileEnvelope(z, x, y), 3857) AS geom
                FROM req
            ), pref AS (
                SELECT
                    r.ord,
                    r.z,
                    r.x,
                    r.y,
                    mercator_quadkey_prefix(b.geom, {quadkey_zoom}, {prefix_len}) AS prefix_full,
                    mercator_quadkey_prefix(b.geom, {quadkey_zoom}, r.z) AS prefix_zoom,
                    SUBSTRING(mercator_quadkey_prefix(b.geom, {quadkey_zoom}, {prefix_len}) FROM 1 FOR 1) AS grp
                FROM req r
                JOIN bounds b USING (ord, z, x, y)
            )
            SELECT
                p.z,
                p.x,
                p.y,
                CASE
                    WHEN cache.mvt IS NOT NULL THEN cache.mvt
                    ELSE (
                        SELECT ST_AsMVT(mvt_rows, 'red_light_locations', 4096, 'geom')
                        FROM (
                            SELECT
                                ST_AsMVTGeom(t.geom, b.geom, 4096, 64, true) AS geom,
                                t.feature_id,
                                t.ticket_count,
                                t.total_fine_amount,
                                t.location_name,
                                t.location,
                                t.ward
                            FROM red_light_camera_tiles t
                            WHERE t.dataset = 'red_light_locations'
                              AND t.min_zoom <= p.z
                              AND t.max_zoom >= p.z
                              AND t.tile_qk_group = p.grp
                          AND t.tile_qk_prefix LIKE p.prefix_zoom || '%%'
                              AND t.geom && b.geom
                        ) AS mvt_rows
                    )
                END AS mvt
            FROM pref p
            JOIN bounds b USING (ord, z, x, y)
            LEFT JOIN LATERAL (
                SELECT mvt
                FROM public.tile_blob_cache cache
                WHERE cache.dataset = 'red_light_locations'
                  AND cache.z = p.z
                  AND cache.x = p.x
                  AND cache.y = p.y
            ) AS cache ON p.z <= 10
            WHERE p.prefix_full IS NOT NULL
            $$;
            """
        )

        self._log("  Creating get_ase_tiles function")
        self.pg.execute(
            f"""
            CREATE OR REPLACE FUNCTION public.get_ase_tiles(
                zs integer[],
                xs integer[],
                ys integer[]
            ) RETURNS TABLE (z integer, x integer, y integer, mvt bytea)
            LANGUAGE sql
            STABLE
            PARALLEL SAFE
            AS
            $$
            WITH req AS (
                SELECT ord, z, x, y
                FROM unnest(zs, xs, ys) WITH ORDINALITY AS t(z, x, y, ord)
            ), bounds AS (
                SELECT ord, z, x, y, ST_SetSRID(ST_TileEnvelope(z, x, y), 3857) AS geom
                FROM req
            ), pref AS (
                SELECT
                    r.ord,
                    r.z,
                    r.x,
                    r.y,
                    mercator_quadkey_prefix(b.geom, {quadkey_zoom}, {prefix_len}) AS prefix_full,
                    mercator_quadkey_prefix(b.geom, {quadkey_zoom}, r.z) AS prefix_zoom,
                    SUBSTRING(mercator_quadkey_prefix(b.geom, {quadkey_zoom}, {prefix_len}) FROM 1 FOR 1) AS grp
                FROM req r
                JOIN bounds b USING (ord, z, x, y)
            )
            SELECT
                p.z,
                p.x,
                p.y,
                CASE
                    WHEN cache.mvt IS NOT NULL THEN cache.mvt
                    ELSE (
                        SELECT ST_AsMVT(mvt_rows, 'ase_locations', 4096, 'geom')
                        FROM (
                            SELECT
                                ST_AsMVTGeom(t.geom, b.geom, 4096, 64, true) AS geom,
                                t.feature_id,
                                t.ticket_count,
                                t.total_fine_amount,
                                t.location,
                                t.status,
                                t.ward
                            FROM ase_camera_tiles t
                            WHERE t.dataset = 'ase_locations'
                              AND t.min_zoom <= p.z
                              AND t.max_zoom >= p.z
                              AND t.tile_qk_group = p.grp
                          AND t.tile_qk_prefix LIKE p.prefix_zoom || '%%'
                              AND t.geom && b.geom
                        ) AS mvt_rows
                    )
                END AS mvt
            FROM pref p
            JOIN bounds b USING (ord, z, x, y)
            LEFT JOIN LATERAL (
                SELECT mvt
                FROM public.tile_blob_cache cache
                WHERE cache.dataset = 'ase_locations'
                  AND cache.z = p.z
                  AND cache.x = p.x
                  AND cache.y = p.y
            ) AS cache ON p.z <= 10
            WHERE p.prefix_full IS NOT NULL
            $$;
            """
        )

    def _ensure_base_columns(self) -> None:
        """Add Web Mercator columns and indexes to foundational tables."""

        for table_meta in BASE_POINT_TABLES:
            table = table_meta["table"]
            geom_3857 = table_meta["geom_3857"]

            self._log(f"  Processing base table '{table}'")
            has_geom = self._column_exists(table, geom_3857) and self._column_has_data(table, geom_3857)
            has_prefix = self._column_exists(table, "tile_qk_prefix") and self._column_has_data(
                table, "tile_qk_prefix", treat_blank_as_null=True
            )

            if not has_geom or not has_prefix:
                self._log("    Missing projected columns detected; rebuilding table via CTAS")
                self._rebuild_base_table(table_meta)
                continue
            else:
                self._log(f"    {geom_3857} already present; ensuring supporting indexes")
                self.pg.execute(
                    f"CREATE INDEX IF NOT EXISTS {table}_geom_3857_idx ON {table} USING GIST ({geom_3857});"
                )
                self.pg.execute(
                    f"CREATE INDEX IF NOT EXISTS {table}_tile_qk_prefix_idx ON {table} (tile_qk_prefix);"
                )

    def _ensure_tile_tables(self) -> None:
        """Create partitioned tile tables populated from the base tables."""

        builders: Iterable[tuple[str, str, str, str]] = (
            (
                "parking_ticket_tiles",
                "parking_tickets",
                "parking_tickets",
                f"""
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
                        SELECT 0 AS min_zoom, 10 AS max_zoom, subdivided.geom AS geom_variant
                        FROM ST_Subdivide(
                            ST_SnapToGrid(ST_SimplifyPreserveTopology(agg.geom_3857, 25), 1.0),
                            32
                        ) AS subdivided(geom)
                        UNION ALL
                        SELECT 11 AS min_zoom, 16 AS max_zoom, subdivided.geom AS geom_variant
                        FROM ST_Subdivide(
                            ST_SnapToGrid(agg.geom_3857, 0.25),
                            32
                        ) AS subdivided(geom)
                    ) AS variants(min_zoom, max_zoom, geom_variant)
                    CROSS JOIN LATERAL (
                        SELECT (ST_Dump(geom_variant)).geom
                    ) AS parts
                """,
            ),
            (
                "red_light_camera_tiles",
                "red_light_camera_locations",
                "red_light_locations",
                f"""
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
                        SELECT 0 AS min_zoom, 11 AS max_zoom, subdivided.geom AS geom_variant
                        FROM ST_Subdivide(
                            ST_SnapToGrid(ST_SimplifyPreserveTopology(base.geom_3857, 30), 1.0),
                            32
                        ) AS subdivided(geom)
                        UNION ALL
                        SELECT 12 AS min_zoom, 16 AS max_zoom, subdivided.geom AS geom_variant
                        FROM ST_Subdivide(
                            ST_SnapToGrid(base.geom_3857, 0.25),
                            32
                        ) AS subdivided(geom)
                    ) AS variants(min_zoom, max_zoom, geom_variant)
                    CROSS JOIN LATERAL (
                        SELECT (ST_Dump(geom_variant)).geom
                    ) AS parts
                """,
            ),
            (
                "ase_camera_tiles",
                "ase_camera_locations",
                "ase_locations",
                f"""
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
                        SELECT 0 AS min_zoom, 11 AS max_zoom, subdivided.geom AS geom_variant
                        FROM ST_Subdivide(
                            ST_SnapToGrid(ST_SimplifyPreserveTopology(base.geom_3857, 30), 1.0),
                            32
                        ) AS subdivided(geom)
                        UNION ALL
                        SELECT 12 AS min_zoom, 16 AS max_zoom, subdivided.geom AS geom_variant
                        FROM ST_Subdivide(
                            ST_SnapToGrid(base.geom_3857, 0.25),
                            32
                        ) AS subdivided(geom)
                    ) AS variants(min_zoom, max_zoom, geom_variant)
                    CROSS JOIN LATERAL (
                        SELECT (ST_Dump(geom_variant)).geom
                    ) AS parts
                """,
            ),
        )

        for table_name, base_table, dataset_name, populate_sql in builders:
            self._log(f"  Ensuring tile table '{table_name}' (source: {base_table})")
            parent_definition = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    tile_id BIGSERIAL,
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
                    ward TEXT,
                    PRIMARY KEY (tile_qk_group, tile_id)
                ) PARTITION BY LIST (tile_qk_group);
            """
            self._log("    Creating parent partitioned table if needed")
            self.pg.execute(parent_definition)

            self.pg.execute(
                f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints
                        WHERE table_schema = 'public'
                          AND table_name = '{table_name}'
                          AND constraint_type = 'PRIMARY KEY'
                    ) THEN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM information_schema.key_column_usage
                            WHERE table_schema = 'public'
                              AND table_name = '{table_name}'
                              AND constraint_name = '{table_name}_pkey'
                              AND column_name = 'tile_qk_group'
                        ) THEN
                            EXECUTE 'ALTER TABLE {table_name} DROP CONSTRAINT {table_name}_pkey';
                        END IF;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints
                        WHERE table_schema = 'public'
                          AND table_name = '{table_name}'
                          AND constraint_type = 'PRIMARY KEY'
                    ) THEN
                        EXECUTE 'ALTER TABLE {table_name} ADD PRIMARY KEY (tile_qk_group, tile_id)';
                    END IF;
                END
                $$;
                """
            )

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
                f"CREATE INDEX IF NOT EXISTS {table_name}_dataset_prefix_idx ON {table_name} (dataset, tile_qk_group, tile_qk_prefix);"
            )
            self.pg.execute(
                f"CREATE INDEX IF NOT EXISTS {table_name}_zoom_idx ON {table_name} (min_zoom, max_zoom) WHERE dataset = '{dataset_name}';"
            )
            self.pg.execute(
                f"CREATE INDEX IF NOT EXISTS {table_name}_prefix_idx ON {table_name} (tile_qk_prefix);"
            )
            self.pg.execute(f"ANALYZE {table_name};")

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
