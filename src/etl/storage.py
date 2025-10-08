"""Local storage helpers for ETL artefacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import logging
from typing import Any, Dict


LOGGER = logging.getLogger(__name__)


@dataclass
class ArtefactStore:
    """Manages storage locations for downloaded resources and staging files."""

    raw_root: Path
    staging_root: Path

    def __post_init__(self) -> None:
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.staging_root.mkdir(parents=True, exist_ok=True)

    def raw_path(self, dataset_slug: str, resource_id: str, suffix: str | None = None) -> Path:
        key = hashlib.sha1(resource_id.encode("utf8"), usedforsecurity=False).hexdigest()
        filename = key if suffix is None else f"{key}{suffix}"
        return self.raw_root / dataset_slug / filename

    def staging_path(self, dataset_slug: str, name: str) -> Path:
        return self.staging_root / dataset_slug / name

    def write_manifest(self, dataset_slug: str, payload: Dict[str, Any]) -> Path:
        target = self.raw_root / dataset_slug / "manifest.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.debug("Writing manifest %s", target)
        target.write_text(json.dumps(payload, indent=2, sort_keys=True))
        return target

    def read_manifest(self, dataset_slug: str) -> Dict[str, Any] | None:
        target = self.raw_root / dataset_slug / "manifest.json"
        if not target.exists():
            return None
        try:
            return json.loads(target.read_text())
        except json.JSONDecodeError:  # pragma: no cover - best effort read
            LOGGER.warning("Manifest for %s is corrupted; ignoring", dataset_slug)
            return None


__all__ = ["ArtefactStore"]
