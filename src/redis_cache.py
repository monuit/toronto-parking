"""Redis cache helper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import redis


@dataclass
class RedisCache:
    url: str
    default_ttl_seconds: int = 3600
    namespace: str = "toronto:tiles"

    def __post_init__(self) -> None:
        self._client = redis.from_url(self.url, decode_responses=False)

    def build_key(self, *parts: str) -> str:
        return ":".join([self.namespace, *parts])

    def get(self, *parts: str) -> Optional[bytes]:
        return self._client.get(self.build_key(*parts))

    def set(self, value: bytes, *parts: str, ttl: Optional[int] = None) -> None:
        self._client.set(self.build_key(*parts), value, ex=ttl or self.default_ttl_seconds)

    def delete(self, *parts: str) -> None:
        self._client.delete(self.build_key(*parts))


__all__ = ["RedisCache"]
