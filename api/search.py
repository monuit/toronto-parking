import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from src.etl.config import DatabaseConfig
from src.etl.postgres import PostgresClient


DATABASE_CONFIG = DatabaseConfig.from_env()
PG_CLIENT = PostgresClient(
    DATABASE_CONFIG.dsn,
    application_name="search-service",
    connect_timeout=DATABASE_CONFIG.connect_timeout,
    statement_timeout_ms=DATABASE_CONFIG.statement_timeout_ms,
)


def _json(status: int, payload: dict, *, headers: dict | None = None):
    response_headers = {"Content-Type": "application/json"}
    if headers:
        response_headers.update(headers)
    return status, response_headers, json.dumps(payload)


def handler(request):
    if request.method != "GET":
        return _json(405, {"error": "Method not allowed"})

    query = request.args.get("q") if request.args else None
    if not query or len(query.strip()) < 2:
        return _json(400, {"error": "Search query must be at least 2 characters"})

    query = query.strip()
    dataset = request.args.get("dataset", "parking_tickets") if request.args else "parking_tickets"

    if dataset == "parking_tickets":
        sql = (
            "SELECT ticket_number, date_of_infraction, infraction_code, set_fine_amount, street_normalized "
            "FROM parking_tickets "
            "WHERE (street_normalized ILIKE %s OR infraction_code = %s) "
            "ORDER BY date_of_infraction DESC NULLS LAST "
            "LIMIT 25"
        )
        rows = PG_CLIENT.fetch_all(sql, (f"%{query.upper()}%", query.upper()))
        results = [
            {
                "ticketNumber": row[0],
                "date": row[1].isoformat() if row[1] else None,
                "code": row[2],
                "amount": float(row[3]) if row[3] is not None else None,
                "street": row[4],
            }
            for row in rows
        ]
        return _json(200, {"results": results})

    if dataset == "red_light_locations":
        like_query = f"%{query}%"
        sql = (
            "SELECT intersection_id, "
            "       COALESCE(NULLIF(location_name, ''), CONCAT_WS(' & ', NULLIF(linear_name_full_1, ''), NULLIF(linear_name_full_2, '')), 'Unknown') AS label, "
            "       ticket_count, "
            "       COALESCE(total_fine_amount, 0) "
            "FROM red_light_camera_locations "
            "WHERE location_name ILIKE %s "
            "   OR linear_name_full_1 ILIKE %s "
            "   OR linear_name_full_2 ILIKE %s "
            "   OR intersection_id = %s "
            "ORDER BY ticket_count DESC NULLS LAST, total_fine_amount DESC NULLS LAST "
            "LIMIT 25"
        )
        rows = PG_CLIENT.fetch_all(sql, (like_query, like_query, like_query, query))
        results = [
            {
                "intersectionId": row[0],
                "location": row[1],
                "ticketCount": int(row[2] or 0),
                "totalRevenue": float(row[3] or 0),
            }
            for row in rows
        ]
        return _json(200, {"results": results})

    if dataset == "ase_locations":
        like_query = f"%{query}%"
        sql = (
            "SELECT location_code, location, status, ticket_count, COALESCE(total_fine_amount, 0) "
            "FROM ase_camera_locations "
            "WHERE location ILIKE %s OR location_code ILIKE %s "
            "ORDER BY ticket_count DESC NULLS LAST, total_fine_amount DESC NULLS LAST "
            "LIMIT 25"
        )
        rows = PG_CLIENT.fetch_all(sql, (like_query, f"%{query.upper()}%"))
        results = [
            {
                "locationCode": row[0],
                "location": row[1],
                "status": row[2],
                "ticketCount": int(row[3] or 0),
                "totalRevenue": float(row[4] or 0),
            }
            for row in rows
        ]
        return _json(200, {"results": results})

    return _json(400, {"error": "Unsupported dataset"})
