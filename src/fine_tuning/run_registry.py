"""Persistence helpers for fine-tune and evaluation run metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, MutableMapping, Optional


@dataclass(slots=True)
class FileRecord:
    """Metadata describing a file uploaded to OpenAI storage."""

    checksum: str
    path: str
    purpose: str
    openai_file_id: str
    uploaded_at: str
    bytes: int

    @classmethod
    def from_dict(cls, payload: MutableMapping[str, object]) -> "FileRecord":
        return cls(
            checksum=str(payload["checksum"]),
            path=str(payload["path"]),
            purpose=str(payload["purpose"]),
            openai_file_id=str(payload["openai_file_id"]),
            uploaded_at=str(payload["uploaded_at"]),
            bytes=int(payload.get("bytes", 0)),
        )

    def to_dict(self) -> Dict[str, object]:
        return {
            "checksum": self.checksum,
            "path": self.path,
            "purpose": self.purpose,
            "openai_file_id": self.openai_file_id,
            "uploaded_at": self.uploaded_at,
            "bytes": self.bytes,
        }


class RunRegistry:
    """Manages persisted metadata for fine-tune and evaluation runs."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: Dict[str, object] = {
            "files": {},
            "fine_tunes": [],
            "evals": [],
        }
        self._load()

    # MARK: loading/saving ------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return
        if isinstance(raw, dict):
            self._data["files"] = dict(raw.get("files", {}))
            self._data["fine_tunes"] = list(raw.get("fine_tunes", []))
            self._data["evals"] = list(raw.get("evals", []))

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "files": self._data["files"],
            "fine_tunes": self._data["fine_tunes"],
            "evals": self._data["evals"],
        }
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # MARK: file helpers --------------------------------------------------
    def get_file(self, checksum: str) -> Optional[FileRecord]:
        record = self._data["files"].get(checksum)
        if not record:
            return None
        return FileRecord.from_dict(record)

    def record_file(self, record: FileRecord) -> None:
        self._data["files"][record.checksum] = record.to_dict()
        self.save()

    # MARK: run tracking --------------------------------------------------
    def append_fine_tune(self, payload: Dict[str, object]) -> Dict[str, object]:
        enriched = {
            **payload,
            "recorded_at": datetime.now(UTC).isoformat(),
        }
        self._data["fine_tunes"].append(enriched)
        self.save()
        return enriched

    def append_eval(self, payload: Dict[str, object]) -> Dict[str, object]:
        enriched = {
            **payload,
            "recorded_at": datetime.now(UTC).isoformat(),
        }
        self._data["evals"].append(enriched)
        self.save()
        return enriched

    @property
    def fine_tunes(self) -> list:
        return list(self._data["fine_tunes"])

    @property
    def evals(self) -> list:
        return list(self._data["evals"])

    @property
    def files(self) -> Dict[str, Dict[str, object]]:
        return dict(self._data["files"])


__all__ = ["FileRecord", "RunRegistry"]
