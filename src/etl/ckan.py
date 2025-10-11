"""CKAN API client helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional
import json
import logging
import time

import os

import requests
from requests import Response
from tenacity import retry, stop_after_attempt, wait_exponential


LOGGER = logging.getLogger(__name__)


class CKANError(RuntimeError):
    """Raised when CKAN returns an error response."""


@dataclass(frozen=True)
class PackageResource:
    """Simplified view of a CKAN resource returned by ``package_show``."""

    id: str
    url: Optional[str]
    datastore_active: bool
    format: Optional[str]
    name: Optional[str]
    last_modified: Optional[str]


class CKANClient:
    """Lightweight CKAN API client with retry handling."""

    def __init__(self, base_url: str, user_agent: str = "toronto-parking-etl/1.0", timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.user_agent})
        verify = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
        if verify and Path(verify).exists():
            self._session.verify = verify

    def close(self) -> None:
        self._session.close()

    def _handle_response(self, response: Response) -> Dict[str, Any]:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:  # pragma: no cover - network errors handled upstream
            raise CKANError(str(exc)) from exc
        payload = response.json()
        if not payload.get("success", False):
            raise CKANError(json.dumps(payload))
        return payload["result"]

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
    def package_show(self, package_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/api/3/action/package_show"
        LOGGER.debug("Fetching package metadata for %s", package_id)
        response = self._session.get(url, params={"id": package_id}, timeout=self.timeout)
        return self._handle_response(response)

    def iter_package_resources(self, package_id: str) -> Iterator[PackageResource]:
        result = self.package_show(package_id)
        for resource in result.get("resources", []):
            yield PackageResource(
                id=resource.get("id"),
                url=resource.get("url"),
                datastore_active=bool(resource.get("datastore_active")),
                format=resource.get("format"),
                name=resource.get("name"),
                last_modified=resource.get("last_modified"),
            )

    @retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
    def download_resource(self, resource_id: str, destination: Path) -> Path:
        """Download a CKAN resource dump to ``destination``."""

        meta_url = f"{self.base_url}/api/3/action/resource_show"
        response = self._session.get(meta_url, params={"id": resource_id}, timeout=self.timeout)
        resource = self._handle_response(response)
        url = resource.get("url")
        if not url:
            raise CKANError(f"Resource {resource_id} does not have a download URL")

        destination.parent.mkdir(parents=True, exist_ok=True)

        LOGGER.info("Downloading resource %s → %s", resource_id, destination)
        with self._session.get(url, stream=True, timeout=self.timeout) as stream:
            stream.raise_for_status()
            with destination.open("wb") as fh:
                for chunk in stream.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)
        return destination

    def datastore_search(
        self,
        resource_id: str,
        *,
        limit: int = 5000,
        filters: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate over rows in a datastore-active CKAN resource."""

        offset = 0
        has_more = True
        params = dict(params or {})
        params.setdefault("id", resource_id)
        params.setdefault("limit", limit)
        if filters:
            params["filters"] = json.dumps(filters)

        data_url = f"{self.base_url}/api/3/action/datastore_search"
        while has_more:
            current_params = {**params, "offset": offset}
            response = self._session.get(data_url, params=current_params, timeout=self.timeout)
            payload = self._handle_response(response)

            records = payload.get("records", [])
            if not records:
                break
            for record in records:
                yield record

            offset += len(records)
            total = payload.get("total")
            has_more = total is None or offset < total
            if has_more:
                time.sleep(0.2)

    @retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
    def datastore_search_sql(self, sql: str) -> Dict[str, Any]:
        url = f"{self.base_url}/api/3/action/datastore_search_sql"
        response = self._session.get(url, params={"sql": sql}, timeout=self.timeout)
        return self._handle_response(response)

    def iter_datastore_sql(
        self,
        resource_id: str,
        *,
        where: str | None = None,
        order_by: str | None = None,
        chunk_size: int = 5000,
    ) -> Iterator[Dict[str, Any]]:
        offset = 0
        while True:
            sql_parts = [f'SELECT * FROM "{resource_id}"']
            if where:
                sql_parts.append(f"WHERE {where}")
            if order_by:
                sql_parts.append(f"ORDER BY {order_by}")
            sql_parts.append(f"LIMIT {chunk_size} OFFSET {offset}")
            sql = " ".join(sql_parts)
            payload = self.datastore_search_sql(sql)
            records = payload.get("records", [])
            if not records:
                break
            for record in records:
                yield record
            offset += len(records)
            if len(records) < chunk_size:
                break


__all__ = [
    "CKANClient",
    "CKANError",
    "PackageResource",
]
