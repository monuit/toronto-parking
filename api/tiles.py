import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / 'src'
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from src.etl.config import DatabaseConfig, RedisConfig
from src.etl.postgres import PostgresClient
from src.redis_cache import RedisCache
from src.tiles import TileService


DATABASE_CONFIG = DatabaseConfig.from_env()
REDIS_CONFIG = RedisConfig.from_env()

PG_CLIENT = PostgresClient(
    DATABASE_CONFIG.dsn,
    application_name="tiles-service",
    connect_timeout=DATABASE_CONFIG.connect_timeout,
    statement_timeout_ms=DATABASE_CONFIG.statement_timeout_ms,
)
TILE_SERVICE = TileService(pg=PG_CLIENT)
CACHE = RedisCache(
    REDIS_CONFIG.url,
    default_ttl_seconds=REDIS_CONFIG.default_ttl_seconds,
    namespace=f"{REDIS_CONFIG.namespace}:tiles",
)


def _json(status: int, payload: dict, *, headers: dict | None = None):
    base_headers = {"Content-Type": "application/json"}
    if headers:
        base_headers.update(headers)
    return status, base_headers, json.dumps(payload)


def handler(request):  # Vercel-style handler
    if request.method != "GET":
        return _json(405, {"error": "Method not allowed"})

    try:
        dataset = request.args.get("dataset", "parking_tickets")
        z = int(request.args.get("z"))
        x = int(request.args.get("x"))
        y = int(request.args.get("y"))
    except (TypeError, ValueError):
        return _json(400, {"error": "Invalid tile coordinates"})

    cache_key = (dataset, str(z), str(x), str(y))
    cached = CACHE.get(*cache_key)
    if cached is not None:
        headers = {
            "Content-Type": "application/x-protobuf",
            "Cache-Control": "public, max-age=86400, immutable",
        }
        return 200, headers, cached

    try:
        tile = TILE_SERVICE.get_tile(dataset, z, x, y)
    except Exception as exc:  # pragma: no cover - defensive
        return _json(500, {"error": str(exc)})

    if tile is None:
        return 204, {"Cache-Control": "public, max-age=300"}, b""

    CACHE.set(tile, *cache_key, ttl=REDIS_CONFIG.default_ttl_seconds)

    headers = {
        "Content-Type": "application/x-protobuf",
        "Cache-Control": "public, max-age=86400, immutable",
    }
    return 200, headers, tile
