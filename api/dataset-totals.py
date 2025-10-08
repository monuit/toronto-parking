import json
import sys
from pathlib import Path

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
    application_name="dataset-totals-service",
    connect_timeout=DATABASE_CONFIG.connect_timeout,
    statement_timeout_ms=DATABASE_CONFIG.statement_timeout_ms,
)


def _json(status: int, payload: dict):
    return status, {"Content-Type": "application/json"}, json.dumps(payload)


def _totals_for_dataset(dataset: str) -> dict:
    if dataset == "parking_tickets":
        row = PG_CLIENT.fetch_one(
            """
            SELECT COUNT(*)::BIGINT, COALESCE(SUM(set_fine_amount), 0)::NUMERIC
            FROM parking_tickets
            """
        )
        return {
            "dataset": dataset,
            "featureCount": int(row[0]) if row else 0,
            "ticketCount": int(row[0]) if row else 0,
            "totalRevenue": float(row[1]) if row and row[1] is not None else 0.0,
        }

    if dataset == "red_light_locations":
        row = PG_CLIENT.fetch_one(
            """
            SELECT COUNT(*)::BIGINT, COALESCE(SUM(ticket_count), 0)::BIGINT, COALESCE(SUM(total_fine_amount), 0)::NUMERIC
            FROM red_light_camera_locations
            """
        )
        return {
            "dataset": dataset,
            "featureCount": int(row[0]) if row else 0,
            "ticketCount": int(row[1]) if row else 0,
            "totalRevenue": float(row[2]) if row and row[2] is not None else 0.0,
        }

    if dataset == "ase_locations":
        row = PG_CLIENT.fetch_one(
            """
            SELECT COUNT(*)::BIGINT, COALESCE(SUM(ticket_count), 0)::BIGINT, COALESCE(SUM(total_fine_amount), 0)::NUMERIC
            FROM ase_camera_locations
            """
        )
        return {
            "dataset": dataset,
            "featureCount": int(row[0]) if row else 0,
            "ticketCount": int(row[1]) if row else 0,
            "totalRevenue": float(row[2]) if row and row[2] is not None else 0.0,
        }

    raise ValueError(f"Unsupported dataset '{dataset}'")


def handler(request):
    if request.method != "GET":
        return _json(405, {"error": "Method not allowed"})

    dataset = request.args.get("dataset", "parking_tickets") if request.args else "parking_tickets"
    try:
        payload = _totals_for_dataset(dataset)
    except ValueError as exc:
        return _json(400, {"error": str(exc)})
    except Exception as exc:  # pragma: no cover - defensive
        return _json(500, {"error": str(exc)})

    return _json(200, payload)
