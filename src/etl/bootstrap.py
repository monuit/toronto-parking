"""Bootstrap helpers for foundational PostGIS tables used by the refresh pipeline."""

from __future__ import annotations

from .postgres import PostgresClient
from .state import DDL as ETL_STATE_DDL

BASE_TABLE_DDLS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS parking_tickets (
        ticket_hash TEXT PRIMARY KEY,
        ticket_number TEXT,
        date_of_infraction DATE,
        time_of_infraction TEXT,
        infraction_code TEXT,
        infraction_description TEXT,
        set_fine_amount NUMERIC,
        location1 TEXT,
        location2 TEXT,
        location3 TEXT,
        location4 TEXT,
        street_normalized TEXT,
        centreline_id BIGINT,
        geom geometry(POINT, 4326),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS parking_tickets_staging (
        ticket_hash TEXT,
        ticket_number TEXT,
        date_of_infraction TEXT,
        time_of_infraction TEXT,
        infraction_code TEXT,
        infraction_description TEXT,
        set_fine_amount TEXT,
        location1 TEXT,
        location2 TEXT,
        location3 TEXT,
        location4 TEXT,
        street_normalized TEXT,
        centreline_id BIGINT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS red_light_camera_locations (
        intersection_id TEXT PRIMARY KEY,
        location_code TEXT,
        linear_name_full_1 TEXT,
        linear_name_full_2 TEXT,
        location_name TEXT,
        ward_1 TEXT,
        police_division_1 TEXT,
        activation_date DATE,
        ticket_count INTEGER DEFAULT 0,
        total_fine_amount NUMERIC(18, 2),
        years INTEGER[],
        months INTEGER[],
        yearly_counts JSONB,
        geom geometry(POINT, 4326)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS red_light_camera_locations_staging (
        intersection_id TEXT,
        location_code TEXT,
        linear_name_full_1 TEXT,
        linear_name_full_2 TEXT,
        location_name TEXT,
        ward_1 TEXT,
        police_division_1 TEXT,
        activation_date TEXT,
        ticket_count INTEGER,
        total_fine_amount TEXT,
        years TEXT,
        months TEXT,
        yearly_counts_json TEXT,
        geometry_geojson TEXT
    )
    """,
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
    """,
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
    """,
    """
    CREATE TABLE IF NOT EXISTS ase_camera_locations (
        location_code TEXT PRIMARY KEY,
        ward TEXT,
        status TEXT,
        location TEXT,
        ticket_count INTEGER DEFAULT 0,
        total_fine_amount NUMERIC(18, 2),
        years INTEGER[],
        months INTEGER[],
        yearly_counts JSONB,
        monthly_counts JSONB,
        geom geometry(POINT, 4326)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ase_camera_locations_staging (
        location_code TEXT,
        ward TEXT,
        status TEXT,
        location TEXT,
        ticket_count INTEGER,
        total_fine_amount TEXT,
        years TEXT,
        months TEXT,
        yearly_counts_json TEXT,
        monthly_counts_json TEXT,
        geometry_geojson TEXT
    )
    """,
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
    """,
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
    """,
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
    """,
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
    """,
    """
    CREATE TABLE IF NOT EXISTS city_wards (
        ward_code INTEGER PRIMARY KEY,
        ward_name TEXT NOT NULL,
        ward_short_code TEXT,
        geom geometry(MULTIPOLYGON, 4326) NOT NULL,
        properties JSONB DEFAULT '{}'::JSONB,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
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
    """,
)


def ensure_base_tables(dsn: str) -> None:
    """Create empty base tables required by downstream ETL steps."""

    client = PostgresClient(dsn=dsn, application_name="toronto-parking-bootstrap")
    client.ensure_extensions()
    for ddl in BASE_TABLE_DDLS:
        client.execute(ddl)
    client.execute(ETL_STATE_DDL)


__all__ = ["ensure_base_tables"]
