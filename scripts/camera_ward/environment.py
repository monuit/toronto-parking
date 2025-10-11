"""Environment helpers for camera ward dataset scripts."""

from __future__ import annotations

import os
from typing import Optional

import dotenv

from .constants import REPO_ROOT


def load_env() -> None:
    """Load environment variables from the repository .env file."""

    dotenv.load_dotenv(REPO_ROOT / ".env")


def resolve_dsn(preferred: Optional[str]) -> str:
    """Resolve the database connection string, preferring an explicit override."""

    if preferred:
        return preferred
    for key in ("POSTGIS_DATABASE_URL", "DATABASE_URL", "POSTGRES_URL"):
        value = os.getenv(key)
        if value:
            return value
    raise RuntimeError("Database URL not provided; set POSTGIS_DATABASE_URL or DATABASE_URL")


def resolve_redis_url(override: Optional[str] = None) -> Optional[str]:
    """Resolve the Redis connection string if defined."""

    if override:
        return override
    for key in ("REDIS_PUBLIC_URL", "REDIS_URL", "REDIS_CONNECTION"):
        value = os.getenv(key)
        if value:
            return value
    return None
