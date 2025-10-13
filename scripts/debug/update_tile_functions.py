"""Utility to refresh tile-generating SQL functions with zoom-aware prefixes.

This script updates the PostGIS functions that power the parking/red-light/ASE
vector tiles so that low-zoom tiles use the appropriate quadkey prefix length.
"""

from __future__ import annotations

import os
from textwrap import dedent

import psycopg


TILES_DB_URL = (
    os.getenv("TILES_DB_URL")
    or os.getenv("DATABASE_PRIVATE_URL")
    or os.getenv("DATABASE_URL")
    or "postgresql://postgres:CA3DeGBF23F5C3Aag3Ecg4f2eDGD52Be@interchange.proxy.rlwy.net:57747/railway"
)


QUADKEY_ZOOM = 16
PREFIX_LEN = 16


def build_sql(dataset: str, table: str, function: str, extra_columns: str) -> str:
    return dedent(
        f"""
        CREATE OR REPLACE FUNCTION {function}(
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
                mercator_quadkey_prefix(b.geom, {QUADKEY_ZOOM}, {PREFIX_LEN}) AS prefix_full,
                mercator_quadkey_prefix(b.geom, {QUADKEY_ZOOM}, r.z) AS prefix_zoom,
                SUBSTRING(
                    mercator_quadkey_prefix(b.geom, {QUADKEY_ZOOM}, {PREFIX_LEN})
                    FROM 1 FOR 1
                ) AS grp
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
                    SELECT ST_AsMVT(mvt_rows, '{dataset}', 4096, 'geom')
                    FROM (
                        SELECT
                            ST_AsMVTGeom(t.geom, b.geom, 4096, 64, true) AS geom,
                            t.feature_id,
                            t.ticket_count,
                            t.total_fine_amount,
                            t.street_normalized,
                            t.centreline_id
                            {extra_columns}
                        FROM {table} t
                        WHERE t.dataset = '{dataset}'
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
            WHERE cache.dataset = '{dataset}'
              AND cache.z = p.z
              AND cache.x = p.x
              AND cache.y = p.y
        ) AS cache ON p.z <= 10
        WHERE p.prefix_full IS NOT NULL
        $$;
        """
    )


def main() -> None:
    statements = [
        build_sql(
            dataset="parking_tickets",
            table="parking_ticket_tiles",
            function="public.get_parking_tiles",
            extra_columns=",\n                            t.location_name,\n                            t.location,\n                            t.ward",
        ),
        build_sql(
            dataset="red_light_locations",
            table="red_light_camera_tiles",
            function="public.get_red_light_tiles",
            extra_columns=",\n                            t.location_name,\n                            t.location,\n                            t.ward",
        ),
        build_sql(
            dataset="ase_locations",
            table="ase_camera_tiles",
            function="public.get_ase_tiles",
            extra_columns=",\n                            t.location_name,\n                            t.location,\n                            t.ward",
        ),
    ]

    with psycopg.connect(TILES_DB_URL) as conn:
        with conn.cursor() as cur:
            for sql in statements:
                cur.execute(sql)
        conn.commit()


if __name__ == "__main__":
    main()
