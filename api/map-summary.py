import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from src.etl.config import DatabaseConfig
from src.etl.postgres import PostgresClient


DATABASE_CONFIG = DatabaseConfig.from_env()
PG_CLIENT = PostgresClient(
    DATABASE_CONFIG.dsn,
    application_name="map-summary-service",
    connect_timeout=DATABASE_CONFIG.connect_timeout,
    statement_timeout_ms=DATABASE_CONFIG.statement_timeout_ms,
)

SUMMARY_ZOOM_THRESHOLD = 12


def _json(status: int, payload: Dict[str, Any]):
    return status, {"Content-Type": "application/json"}, json.dumps(payload)


def _parse_float(value: str | None) -> Optional[float]:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result


def _parse_int(value: str | None) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _summarize_parking(bounds: Dict[str, float], filters: Dict[str, Optional[int]]) -> Dict[str, Any]:
    base_params: List[Any] = [bounds["west"], bounds["south"], bounds["east"], bounds["north"]]
    where_clauses = ["ST_Intersects(data.geom, bounds.geom)"]
    filter_params: List[Any] = []

    if filters.get("year") is not None:
        where_clauses.append("EXTRACT(YEAR FROM data.date_of_infraction) = %s")
        filter_params.append(filters["year"])

    if filters.get("month") is not None:
        where_clauses.append("EXTRACT(MONTH FROM data.date_of_infraction) = %s")
        filter_params.append(filters["month"])

    where_sql = " AND ".join(where_clauses)
    summary_sql = f"""
        WITH bounds AS (
            SELECT ST_MakeEnvelope(%s, %s, %s, %s, 4326) AS geom
        )
        SELECT
            COUNT(*)::BIGINT AS ticket_count,
            COALESCE(SUM(data.set_fine_amount), 0)::NUMERIC AS total_fines
        FROM parking_tickets AS data
        CROSS JOIN bounds
        WHERE {where_sql}
    """

    row = PG_CLIENT.fetch_one(summary_sql, base_params + filter_params)
    visible_count = int(row[0]) if row and row[0] is not None else 0
    visible_revenue = float(row[1]) if row and row[1] is not None else 0.0

    top_sql = f"""
        WITH bounds AS (
            SELECT ST_MakeEnvelope(%s, %s, %s, %s, 4326) AS geom
        )
        SELECT
            COALESCE(NULLIF(data.street_normalized, ''), 'Unknown') AS street_key,
            COUNT(*)::BIGINT AS ticket_count,
            COALESCE(SUM(data.set_fine_amount), 0)::NUMERIC AS total_fines,
            MAX(data.location1) AS sample_location
        FROM parking_tickets AS data
        CROSS JOIN bounds
        WHERE {where_sql}
        GROUP BY street_key
        ORDER BY total_fines DESC, ticket_count DESC
        LIMIT 5
    """

    rows = PG_CLIENT.fetch_all(top_sql, base_params + filter_params)
    top_streets = [
        {
            "name": street if street else "Unknown",
            "ticketCount": int(ticket_count or 0),
            "totalRevenue": float(total_fines or Decimal("0")),
            "sampleLocation": sample or street or "Unknown",
        }
        for street, ticket_count, total_fines, sample in rows
    ]

    return {
        "zoomRestricted": False,
        "visibleCount": visible_count,
        "visibleRevenue": visible_revenue,
        "topStreets": top_streets,
    }


def _summarize_red_light(bounds: Dict[str, float], filters: Dict[str, Optional[int]]) -> Dict[str, Any]:
    base_params: List[Any] = [bounds["west"], bounds["south"], bounds["east"], bounds["north"]]
    where_clauses = ["ST_Intersects(data.geom, bounds.geom)"]
    filter_params: List[Any] = []

    if filters.get("year") is not None:
        where_clauses.append("COALESCE(data.years, ARRAY[]::INT[]) @> ARRAY[%s]::INT[]")
        filter_params.append(filters["year"])

    if filters.get("month") is not None:
        where_clauses.append("COALESCE(data.months, ARRAY[]::INT[]) @> ARRAY[%s]::INT[]")
        filter_params.append(filters["month"])

    where_sql = " AND ".join(where_clauses)
    summary_sql = f"""
        WITH bounds AS (
            SELECT ST_MakeEnvelope(%s, %s, %s, %s, 4326) AS geom
        )
        SELECT
            COALESCE(SUM(data.ticket_count), 0)::BIGINT AS ticket_count,
            COALESCE(SUM(data.total_fine_amount), 0)::NUMERIC AS total_fines
        FROM red_light_camera_locations AS data
        CROSS JOIN bounds
        WHERE {where_sql}
    """

    row = PG_CLIENT.fetch_one(summary_sql, base_params + filter_params)
    visible_count = int(row[0]) if row and row[0] is not None else 0
    visible_revenue = float(row[1]) if row and row[1] is not None else 0.0

    top_sql = f"""
        WITH bounds AS (
            SELECT ST_MakeEnvelope(%s, %s, %s, %s, 4326) AS geom
        )
        SELECT
            COALESCE(NULLIF(data.location_name, ''),
                     CONCAT_WS(' & ', NULLIF(data.linear_name_full_1, ''), NULLIF(data.linear_name_full_2, '')),
                     'Unknown') AS location_label,
            COALESCE(SUM(data.ticket_count), 0)::BIGINT AS ticket_count,
            COALESCE(SUM(data.total_fine_amount), 0)::NUMERIC AS total_fines
        FROM red_light_camera_locations AS data
        CROSS JOIN bounds
        WHERE {where_sql}
        GROUP BY location_label
        ORDER BY total_fines DESC, ticket_count DESC
        LIMIT 5
    """

    rows = PG_CLIENT.fetch_all(top_sql, base_params + filter_params)
    top_streets = [
        {
            "name": label,
            "ticketCount": int(ticket_count or 0),
            "totalRevenue": float(total_fines or Decimal("0")),
            "sampleLocation": label,
        }
        for label, ticket_count, total_fines in rows
    ]

    return {
        "zoomRestricted": False,
        "visibleCount": visible_count,
        "visibleRevenue": visible_revenue,
        "topStreets": top_streets,
    }


def _summarize_ase(bounds: Dict[str, float], filters: Dict[str, Optional[int]]) -> Dict[str, Any]:
    base_params: List[Any] = [bounds["west"], bounds["south"], bounds["east"], bounds["north"]]
    where_clauses = ["ST_Intersects(data.geom, bounds.geom)"]
    filter_params: List[Any] = []

    if filters.get("year") is not None:
        where_clauses.append("COALESCE(data.years, ARRAY[]::INT[]) @> ARRAY[%s]::INT[]")
        filter_params.append(filters["year"])

    if filters.get("month") is not None:
        where_clauses.append("COALESCE(data.months, ARRAY[]::INT[]) @> ARRAY[%s]::INT[]")
        filter_params.append(filters["month"])

    where_sql = " AND ".join(where_clauses)
    summary_sql = f"""
        WITH bounds AS (
            SELECT ST_MakeEnvelope(%s, %s, %s, %s, 4326) AS geom
        )
        SELECT
            COALESCE(SUM(data.ticket_count), 0)::BIGINT AS ticket_count,
            COALESCE(SUM(data.total_fine_amount), 0)::NUMERIC AS total_fines
        FROM ase_camera_locations AS data
        CROSS JOIN bounds
        WHERE {where_sql}
    """

    row = PG_CLIENT.fetch_one(summary_sql, base_params + filter_params)
    visible_count = int(row[0]) if row and row[0] is not None else 0
    visible_revenue = float(row[1]) if row and row[1] is not None else 0.0

    top_sql = f"""
        WITH bounds AS (
            SELECT ST_MakeEnvelope(%s, %s, %s, %s, 4326) AS geom
        )
        SELECT
            COALESCE(NULLIF(data.location, ''), data.location_code, 'Unknown') AS location_label,
            COALESCE(SUM(data.ticket_count), 0)::BIGINT AS ticket_count,
            COALESCE(SUM(data.total_fine_amount), 0)::NUMERIC AS total_fines
        FROM ase_camera_locations AS data
        CROSS JOIN bounds
        WHERE {where_sql}
        GROUP BY location_label
        ORDER BY total_fines DESC, ticket_count DESC
        LIMIT 5
    """

    rows = PG_CLIENT.fetch_all(top_sql, base_params + filter_params)
    top_streets = [
        {
            "name": label,
            "ticketCount": int(ticket_count or 0),
            "totalRevenue": float(total_fines or Decimal("0")),
            "sampleLocation": label,
        }
        for label, ticket_count, total_fines in rows
    ]

    return {
        "zoomRestricted": False,
        "visibleCount": visible_count,
        "visibleRevenue": visible_revenue,
        "topStreets": top_streets,
    }


def _summarize_dataset(dataset: str, bounds: Dict[str, float], filters: Dict[str, Optional[int]]) -> Dict[str, Any]:
    if dataset == "parking_tickets":
        return _summarize_parking(bounds, filters)
    if dataset == "red_light_locations":
        return _summarize_red_light(bounds, filters)
    if dataset == "ase_locations":
        return _summarize_ase(bounds, filters)
    raise ValueError(f"Unsupported dataset '{dataset}'")


def handler(request):
    if request.method != "GET":
        return _json(405, {"error": "Method not allowed"})

    dataset = request.args.get("dataset", "parking_tickets")
    west = _parse_float(request.args.get("west"))
    south = _parse_float(request.args.get("south"))
    east = _parse_float(request.args.get("east"))
    north = _parse_float(request.args.get("north"))
    zoom = _parse_float(request.args.get("zoom"))

    if not all(value is not None for value in (west, south, east, north, zoom)):
        return _json(400, {"error": "Bounds and zoom are required"})

    if zoom < SUMMARY_ZOOM_THRESHOLD:
        return _json(200, {"zoomRestricted": True, "topStreets": []})

    filters = {
        "year": _parse_int(request.args.get("year")),
        "month": _parse_int(request.args.get("month")),
    }

    bounds = {"west": west, "south": south, "east": east, "north": north}

    try:
        summary = _summarize_dataset(dataset, bounds, filters)
    except ValueError as exc:
        return _json(400, {"error": str(exc)})
    except Exception as exc:  # pragma: no cover - defensive
        return _json(500, {"error": str(exc)})

    return _json(200, summary)
