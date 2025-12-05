#!/usr/bin/env python3
"""
Create missing PostGIS functions on Railway database.

These functions are required for tile generation:
- mercator_quadkey_prefix: Helper for computing quadkey prefixes
- tile_envelope_3857: Helper for tile bounds
- get_glow_tile: Generates glow line MVT tiles
- get_red_light_tiles: Generates red light camera MVT tiles
- get_ase_tiles: Generates ASE camera MVT tiles
"""

import psycopg2

# Railway PostGIS connection
DATABASE_URL = "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway"

# Constants for quadkey functions (must match what the app expects)
QUADKEY_ZOOM = 20
PREFIX_LEN = 10


def create_functions():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    print("Creating missing PostGIS functions on Railway...")

    # 1. mercator_quadkey_prefix function
    print("\n1. Creating mercator_quadkey_prefix function...")
    cur.execute("""
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
    """)
    print("   ✓ mercator_quadkey_prefix created")

    # 2. tile_envelope_3857 function
    print("\n2. Creating tile_envelope_3857 function...")
    cur.execute("""
        CREATE OR REPLACE FUNCTION tile_envelope_3857(z integer, x integer, y integer)
        RETURNS geometry AS $$
        BEGIN
            RETURN ST_SetSRID(ST_TileEnvelope(z, x, y), 3857);
        END;
        $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
    """)
    print("   ✓ tile_envelope_3857 created")

    # 3. get_glow_tile function
    print("\n3. Creating get_glow_tile function...")
    cur.execute("""
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
    """)
    print("   ✓ get_glow_tile created")

    # 4. get_red_light_tiles function
    print("\n4. Creating get_red_light_tiles function...")
    cur.execute(f"""
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
                mercator_quadkey_prefix(b.geom, {QUADKEY_ZOOM}, {PREFIX_LEN}) AS prefix_full,
                mercator_quadkey_prefix(b.geom, {QUADKEY_ZOOM}, r.z) AS prefix_zoom,
                SUBSTRING(mercator_quadkey_prefix(b.geom, {QUADKEY_ZOOM}, {PREFIX_LEN}) FROM 1 FOR 1) AS grp
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
                            t.status,
                            t.ward,
                            t.kind,
                            COALESCE(t.cluster_size, 1)::integer AS cluster_size,
                            t.grid_meters,
                            t.ticket_count AS count,
                            t.total_fine_amount AS total_revenue
                        FROM red_light_camera_tiles t
                        WHERE t.dataset = 'red_light_locations'
                          AND t.min_zoom <= p.z
                          AND t.max_zoom >= p.z
                          AND t.tile_qk_group = p.grp
                          AND t.tile_qk_prefix LIKE p.prefix_zoom || '%'
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
    """)
    print("   ✓ get_red_light_tiles created")

    # 5. get_ase_tiles function
    print("\n5. Creating get_ase_tiles function...")
    cur.execute(f"""
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
                mercator_quadkey_prefix(b.geom, {QUADKEY_ZOOM}, {PREFIX_LEN}) AS prefix_full,
                mercator_quadkey_prefix(b.geom, {QUADKEY_ZOOM}, r.z) AS prefix_zoom,
                SUBSTRING(mercator_quadkey_prefix(b.geom, {QUADKEY_ZOOM}, {PREFIX_LEN}) FROM 1 FOR 1) AS grp
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
                            t.location_name,
                            t.location,
                            t.status,
                            t.ward,
                            t.kind,
                            COALESCE(t.cluster_size, 1)::integer AS cluster_size,
                            t.grid_meters,
                            t.ticket_count AS count,
                            t.total_fine_amount AS total_revenue
                        FROM ase_camera_tiles t
                        WHERE t.dataset = 'ase_locations'
                          AND t.min_zoom <= p.z
                          AND t.max_zoom >= p.z
                          AND t.tile_qk_group = p.grp
                          AND t.tile_qk_prefix LIKE p.prefix_zoom || '%'
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
    """)
    print("   ✓ get_ase_tiles created")

    # 6. Create tile_blob_cache table if not exists
    print("\n6. Ensuring tile_blob_cache table exists...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.tile_blob_cache (
            dataset text NOT NULL,
            z integer NOT NULL,
            x integer NOT NULL,
            y integer NOT NULL,
            mvt bytea NOT NULL,
            PRIMARY KEY (dataset, z, x, y)
        );
    """)
    print("   ✓ tile_blob_cache table ensured")

    # Verify functions exist
    print("\n" + "="*50)
    print("VERIFICATION")
    print("="*50)
    
    cur.execute("""
        SELECT routine_name, routine_type
        FROM information_schema.routines
        WHERE routine_schema = 'public'
          AND routine_name IN (
            'mercator_quadkey_prefix',
            'tile_envelope_3857',
            'get_glow_tile',
            'get_red_light_tiles',
            'get_ase_tiles'
          )
        ORDER BY routine_name;
    """)
    functions = cur.fetchall()
    
    print(f"\nFound {len(functions)} functions:")
    for name, rtype in functions:
        print(f"  ✓ {name} ({rtype})")
    
    if len(functions) == 5:
        print("\n✅ All 5 functions created successfully!")
    else:
        missing = 5 - len(functions)
        print(f"\n⚠️  Warning: {missing} function(s) may be missing")

    cur.close()
    conn.close()


if __name__ == "__main__":
    create_functions()
