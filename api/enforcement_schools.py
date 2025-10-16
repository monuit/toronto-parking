"""
Enforcement cameras + schools combined endpoint
Returns GeoJSON with schools, ASE cameras, and red light cameras
with status-based coloring for active/inactive enforcement
"""
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
    application_name="enforcement-schools-service",
    connect_timeout=DATABASE_CONFIG.connect_timeout,
    statement_timeout_ms=DATABASE_CONFIG.statement_timeout_ms,
)


def _json(status: int, payload: dict, *, headers: dict | None = None):
    response_headers = {
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=3600"
    }
    if headers:
        response_headers.update(headers)
    return status, response_headers, json.dumps(payload)


async def handler(request):
    """
    GET /api/enforcement-schools
    
    Returns GeoJSON FeatureCollection combining:
    - Schools (orange, fixed)
    - ASE cameras (red=active, gray=inactive/historical/planned)
    - Red light cameras (green=active, light gray=inactive)
    
    Query parameters:
    - types: comma-separated list of types to include
             (school, ase_active, ase_historical, ase_planned, red_light_active, red_light_inactive)
             default: all
    """
    if request.method != "GET":
        return _json(405, {"error": "Method not allowed"})

    # Parse filter types
    types_param = request.args.get("types", "") if request.args else ""
    allowed_types = {
        "school", "ase_active", "ase_historical", "ase_planned",
        "red_light_active", "red_light_inactive"
    }
    
    if types_param:
        requested_types = {t.strip() for t in types_param.split(",") if t.strip()}
        requested_types = requested_types & allowed_types
    else:
        requested_types = allowed_types

    if not requested_types:
        return _json(400, {"error": "At least one valid type must be requested"})

    try:
        features = []

        # MARK: Schools
        if "school" in requested_types:
            features.extend(await _get_schools())

        # MARK: ASE Cameras
        if any(t.startswith("ase_") for t in requested_types):
            ase_types = {t.replace("ase_", "") for t in requested_types if t.startswith("ase_")}
            features.extend(await _get_ase_cameras(ase_types))

        # MARK: Red Light Cameras
        if any(t.startswith("red_light_") for t in requested_types):
            rl_types = {t.replace("red_light_", "") for t in requested_types if t.startswith("red_light_")}
            features.extend(await _get_red_light_cameras(rl_types))

        return _json(200, {
            "type": "FeatureCollection",
            "features": features
        })

    except Exception as e:
        print(f"Error in enforcement_schools endpoint: {e}", file=sys.stderr)
        return _json(500, {"error": "Internal server error"})


async def _get_schools():
    """Fetch all schools as GeoJSON features (bright orange)"""
    query = """
    SELECT
        id,
        name,
        ST_AsGeoJSON(geom)::jsonb as geom
    FROM schools
    WHERE geom IS NOT NULL
    ORDER BY id
    """

    rows = await PG_CLIENT.fetch(query)
    features = []

    for row in rows:
        features.append({
            "type": "Feature",
            "geometry": json.loads(row["geom"]),
            "properties": {
                "type": "school",
                "id": row["id"],
                "name": row["name"],
                "color": "#FFA500"  # Bright orange
            }
        })

    return features


async def _get_ase_cameras(status_types: set):
    """
    Fetch ASE cameras with status-based colors
    - active: red (#FF0000)
    - historical: gray (#CCCCCC)
    - planned: yellow (#FFFF00)
    """
    color_map = {
        "active": "#FF0000",
        "historical": "#CCCCCC",
        "planned": "#FFFF00"
    }

    if not status_types:
        return []

    status_list = tuple(s.capitalize() if s != "planned" else "Planned" for s in status_types)

    query = f"""
    SELECT
        location_code,
        location,
        status,
        ward,
        ticket_count,
        total_fine_amount,
        ST_AsGeoJSON(geom)::jsonb as geom
    FROM ase_camera_locations
    WHERE geom IS NOT NULL
        AND status = ANY(%s)
    ORDER BY location_code
    """

    rows = await PG_CLIENT.fetch(query, status_list)
    features = []

    for row in rows:
        status_key = row["status"].lower()
        features.append({
            "type": "Feature",
            "geometry": json.loads(row["geom"]),
            "properties": {
                "type": "ase_camera",
                "camera_code": row["location_code"],
                "location": row["location"],
                "status": row["status"],
                "ward": row["ward"],
                "tickets": row["ticket_count"],
                "revenue": float(row["total_fine_amount"]) if row["total_fine_amount"] else 0,
                "color": color_map.get(status_key, "#CCCCCC")
            }
        })

    return features


async def _get_red_light_cameras(status_types: set):
    """
    Fetch red light cameras with activation_date-based colors
    - active (activation_date IS NOT NULL): green (#00FF00)
    - inactive (activation_date IS NULL): light gray (#AAAAAA)
    """
    if not status_types or (status_types - {"active", "inactive"}):
        # Return empty if no valid types or invalid types requested
        if not status_types:
            return []

    features = []

    # MARK: Active red light cameras
    if "active" in status_types:
        query = """
        SELECT
            intersection_id,
            location_code,
            location_name,
            ward_1,
            activation_date,
            ticket_count,
            total_fine_amount,
            ST_AsGeoJSON(geom)::jsonb as geom
        FROM red_light_camera_locations
        WHERE geom IS NOT NULL
            AND activation_date IS NOT NULL
        ORDER BY location_code
        """

        rows = await PG_CLIENT.fetch(query)

        for row in rows:
            features.append({
                "type": "Feature",
                "geometry": json.loads(row["geom"]),
                "properties": {
                    "type": "red_light_camera",
                    "camera_code": row["location_code"],
                    "location": row["location_name"],
                    "status": "Active",
                    "activation_date": str(row["activation_date"]) if row["activation_date"] else None,
                    "ward": row["ward_1"],
                    "tickets": row["ticket_count"],
                    "revenue": float(row["total_fine_amount"]) if row["total_fine_amount"] else 0,
                    "color": "#00FF00"  # Green for active
                }
            })

    # MARK: Inactive red light cameras
    if "inactive" in status_types:
        query = """
        SELECT
            intersection_id,
            location_code,
            location_name,
            ward_1,
            ticket_count,
            total_fine_amount,
            ST_AsGeoJSON(geom)::jsonb as geom
        FROM red_light_camera_locations
        WHERE geom IS NOT NULL
            AND activation_date IS NULL
        ORDER BY location_code
        """

        rows = await PG_CLIENT.fetch(query)

        for row in rows:
            features.append({
                "type": "Feature",
                "geometry": json.loads(row["geom"]),
                "properties": {
                    "type": "red_light_camera",
                    "camera_code": row["location_code"],
                    "location": row["location_name"],
                    "status": "Decommissioned",
                    "activation_date": None,
                    "ward": row["ward_1"],
                    "tickets": row["ticket_count"],
                    "revenue": float(row["total_fine_amount"]) if row["total_fine_amount"] else 0,
                    "color": "#AAAAAA"  # Light gray for inactive
                }
            })

    return features
