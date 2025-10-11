"""State management for camera ward dataset builds."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from .constants import STATE_FILE, STATE_VERSION


class WardDatasetState:
    """Representation of cached ward dataset build metadata."""

    def __init__(self, payload: Optional[dict] = None) -> None:
        self._data: Dict[str, dict] = payload or {}

    @property
    def version(self) -> int:
        return int(self._data.get("version") or 0)

    @property
    def data(self) -> Dict[str, dict]:
        return self._data

    def entry(self, dataset: str) -> dict:
        return self._data.get(dataset, {})

    def update_checksum(self, dataset: str, checksum: str, *, source: Optional[str] = None) -> None:
        entry = self._data.setdefault(dataset, {})
        entry.update(
            {
                "checksum": checksum,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        if source is not None:
            entry["source"] = source

    def touch(self) -> None:
        self._data["version"] = STATE_VERSION

    def to_json(self) -> str:
        self.touch()
        return json.dumps(self._data, indent=2, sort_keys=True)

    def save(self, path: Path = STATE_FILE) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def load(cls, path: Path = STATE_FILE) -> "WardDatasetState":
        if not path.exists():
            return cls({"version": STATE_VERSION})
        try:
            raw = path.read_text(encoding="utf-8")
            payload = json.loads(raw)
        except (OSError, json.JSONDecodeError):
            payload = {"version": STATE_VERSION}
        return cls(payload)

    def schema_version_mismatch(self) -> bool:
        return self.version != STATE_VERSION
