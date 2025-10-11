"""Utilities for working with cached artifacts and checksums."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
from pathlib import Path
from typing import Optional


def compute_checksum(path: Path) -> str:
    """Compute a SHA256 checksum for a file."""

    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def checksum_to_version(value: Optional[str]) -> int:
    """Derive a monotonically increasing version identifier from a checksum."""

    from datetime import datetime, timezone

    if not value:
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        return int(value[:12], 16)
    except ValueError:
        return int(datetime.now(timezone.utc).timestamp() * 1000)


def artifact_invalid(path: Path, *, expect_features: bool = False) -> bool:
    """Return True if a cached artifact is missing or malformed."""

    if not path.exists():
        return True
    try:
        raw = path.read_text(encoding="utf-8")
        if not raw.strip():
            return True
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return True
    if expect_features:
        features = payload.get("features") if isinstance(payload, dict) else None
        if not isinstance(features, list) or not features:
            return True
        if not any(
            isinstance(feature, dict)
            and isinstance(feature.get("properties"), dict)
            and feature["properties"].get("ticketCount", 0)
            for feature in features
        ):
            return True
    return False


def gzip_and_encode(raw: str) -> str:
    """Gzip and base64 encode a string payload."""

    return base64.b64encode(gzip.compress(raw.encode("utf-8"))).decode("ascii")
