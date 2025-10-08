import base64
import hmac
import json
import os
import sys
from hashlib import sha256
from pathlib import Path
from time import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


TILE_HOST = os.getenv("TILE_HOST", "https://tiles.example.com")
SIGNING_SECRET_RAW = os.getenv("TILE_SIGNING_SECRET")
DEFAULT_TTL = int(os.getenv("TILE_URL_TTL", "900"))

if not SIGNING_SECRET_RAW:
  raise RuntimeError("TILE_SIGNING_SECRET must be set in the environment")

SIGNING_SECRET = SIGNING_SECRET_RAW.encode("utf-8")


def _json(status: int, payload: dict):
    return status, {"Content-Type": "application/json"}, json.dumps(payload)


def handler(request):
    if request.method != "GET":
        return _json(405, {"error": "Method not allowed"})

    dataset = request.args.get("dataset", "parking_tickets") if request.args else "parking_tickets"
    path = request.args.get("path") if request.args else None
    if not path:
        return _json(400, {"error": "path parameter is required"})

    expiry = int(time()) + DEFAULT_TTL
    payload = f"{path}|{expiry}|{dataset}".encode("utf-8")
    signature = hmac.new(SIGNING_SECRET, payload, sha256).digest()
    token = base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")

    signed_url = f"{TILE_HOST}{path}?dataset={dataset}&expiry={expiry}&token={token}"
    return _json(200, {"url": signed_url, "expiresAt": expiry})
