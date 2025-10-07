"""Centreline data fetching utilities.

Single Responsibility: download and load Toronto Centreline (TCL) data.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
import requests


@dataclass(frozen=True)
class CentrelineResource:
    """Metadata about a centreline resource stored on CKAN."""

    resource_id: str
    name: str
    is_datastore_active: bool


class CentrelineFetcher:
    """Downloads and caches Toronto Centreline data from CKAN."""

    PACKAGE_ID = "toronto-centreline-tcl"
    BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    DEFAULT_RESOURCE_ID = "ad296ebf-fca6-4e67-b3ce-48040a20e6cd"  # GeoJSON (datastore active)

    def __init__(self, cache_dir: Path | str = Path("external_data/centreline"), timeout: int = 60):
        self.cache_dir = Path(cache_dir)
        self.timeout = timeout
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def ensure_cached(self, force_refresh: bool = False) -> Path:
        """Ensure the latest centreline dump is cached locally."""

        cache_path = self.cache_dir / "centreline_dump.csv"
        if cache_path.exists() and not force_refresh:
            return cache_path

        resource_id = self.DEFAULT_RESOURCE_ID
        url = f"{self.BASE_URL}/datastore/dump/{resource_id}"

        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()

        cache_path.write_bytes(response.content)
        return cache_path

    def load_dataframe(self, force_refresh: bool = False) -> pd.DataFrame:
        """Load the centreline dataset into a DataFrame."""

        csv_path = self.ensure_cached(force_refresh=force_refresh)
        df = pd.read_csv(csv_path)

        # geometry arrives as JSON encoded string; parse safely
        df["geometry"] = df["geometry"].apply(self._safe_load_geometry)
        return df

    def available_resources(self) -> Iterator[CentrelineResource]:
        """List all resources in the Centreline package."""

        package = self._fetch_package()
        for resource in package.get("resources", []):
            yield CentrelineResource(
                resource_id=resource["id"],
                name=resource.get("name", ""),
                is_datastore_active=bool(resource.get("datastore_active")),
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_package(self) -> dict:
        url = f"{self.BASE_URL}/api/3/action/package_show"
        params = {"id": self.PACKAGE_ID}
        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        return payload.get("result", {})

    @staticmethod
    def _safe_load_geometry(value: object) -> Optional[dict]:
        if not isinstance(value, str):
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None


__all__ = ["CentrelineFetcher", "CentrelineResource"]
