"""Base classes for dataset-specific ETL logic."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable as TypingIterable, Mapping, Sequence
from functools import lru_cache

from ..ckan import CKANClient
from ..config import DatasetConfig, CKANResourceConfig
from ..postgres import PostgresClient
from ..state import DatasetState, ETLStateStore
from ..storage import ArtefactStore


@dataclass
class ExtractionResult:
    """Carries information about extracted resources."""

    resource_paths: Mapping[str, Path]
    resource_hashes: Mapping[str, str]
    resource_metadata: Mapping[str, Dict[str, Any]]
    row_count: int | None = None


class DatasetETL(ABC):
    """Template for ETL operations."""

    def __init__(
        self,
        config: DatasetConfig,
        *,
        ckan: CKANClient,
        store: ArtefactStore,
        pg: PostgresClient,
        state_store: ETLStateStore,
    ) -> None:
        self.config = config
        self.ckan = ckan
        self.store = store
        self.pg = pg
        self.state_store = state_store
        self._resource_cache: dict[str, Dict[str, Any]] = {}

    def run(self) -> None:
        state = self.state_store.get(self.config.slug)
        extraction = self.extract(state)
        if extraction is None:
            return
        transformed = self.transform(extraction, state)
        self.load(transformed, state)
        metadata = {
            "row_count": transformed.get("row_count"),
            "resources": extraction.resource_metadata,
        }
        self.state_store.upsert(
            self.config.slug,
            last_synced_at=datetime.utcnow(),
            last_resource_hash="|".join(sorted(extraction.resource_hashes.values())),
            metadata=metadata,
        )

    @abstractmethod
    def extract(self, state: DatasetState) -> ExtractionResult | None:
        """Download new artefacts if necessary."""

    @abstractmethod
    def transform(self, extraction: ExtractionResult, state: DatasetState) -> Dict[str, Any]:
        """Transform raw artefacts into loadable structures."""

    @abstractmethod
    def load(self, payload: Dict[str, Any], state: DatasetState) -> None:
        """Persist the transformed payload into PostgreSQL."""

    # Utility helpers -------------------------------------------------
    def download_resource(self, resource: CKANResourceConfig, *, suffix: str) -> Path:
        path = self.store.raw_path(self.config.slug, resource.resource_id, suffix)
        self.ckan.download_resource(resource.resource_id, path)
        return path

    def get_package_resource(self, resource: CKANResourceConfig) -> Dict[str, Any]:
        package_id = resource.package_id or self.config.package_id
        cache = self._resource_cache.get(package_id)
        if cache is None:
            package = self.ckan.package_show(package_id)
            cache = {res.get("id"): res for res in package.get("resources", [])}
            self._resource_cache[package_id] = cache
        data = cache.get(resource.resource_id)
        if not data:
            raise RuntimeError(
                f"Resource {resource.resource_id} not found in package {package_id}"
            )
        return data

    def infer_suffix(self, resource: Dict[str, Any], config: CKANResourceConfig) -> str:
        url = resource.get("url") or ""
        suffix = Path(url).suffix
        if suffix:
            return suffix
        if config.format_hint:
            return f".{config.format_hint.lower()}"
        fmt = resource.get("format")
        if isinstance(fmt, str) and fmt:
            return f".{fmt.lower()}"
        return ".dat"


__all__ = ["DatasetETL", "ExtractionResult"]
