"""Persistence layer for tracking ETL progress across runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from psycopg.types.json import Json

from .postgres import PostgresClient


DDL = """
CREATE TABLE IF NOT EXISTS etl_state (
    dataset_slug TEXT PRIMARY KEY,
    last_synced_at TIMESTAMPTZ,
    last_resource_hash TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


@dataclass
class DatasetState:
    dataset_slug: str
    last_synced_at: Optional[datetime]
    last_resource_hash: Optional[str]
    metadata: Dict[str, Any]


class ETLStateStore:
    """Reads and writes ETL progress information using PostgreSQL."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client
        self.client.execute(DDL)

    def get(self, dataset_slug: str) -> DatasetState:
        row = self.client.fetch_one(
            "SELECT dataset_slug, last_synced_at, last_resource_hash, metadata FROM etl_state WHERE dataset_slug = %s",
            (dataset_slug,),
        )
        if not row:
            return DatasetState(dataset_slug=dataset_slug, last_synced_at=None, last_resource_hash=None, metadata={})
        return DatasetState(
            dataset_slug=row[0],
            last_synced_at=row[1],
            last_resource_hash=row[2],
            metadata=row[3] or {},
        )

    def upsert(
        self,
        dataset_slug: str,
        *,
        last_synced_at: Optional[datetime],
        last_resource_hash: Optional[str],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.client.execute(
            """
            INSERT INTO etl_state (dataset_slug, last_synced_at, last_resource_hash, metadata)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (dataset_slug)
            DO UPDATE SET
                last_synced_at = EXCLUDED.last_synced_at,
                last_resource_hash = EXCLUDED.last_resource_hash,
                metadata = COALESCE(EXCLUDED.metadata, etl_state.metadata),
                updated_at = NOW()
            """,
            (
                dataset_slug,
                last_synced_at,
                last_resource_hash,
                Json(metadata) if metadata is not None else None,
            ),
        )


__all__ = ["ETLStateStore", "DatasetState"]
