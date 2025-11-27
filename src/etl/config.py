"""Configuration objects for the automated ETL pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping
import os


PARKING_TICKET_RESOURCES: Mapping[str, str] = {
    "year_2024": "3263cbd6-39f8-46c9-8ca6-bd8ffc730157",
    "year_2023": "95b34a0c-3403-4075-aa37-e9a91c4d4f29",
    "year_2022": "a0586d31-675e-4669-bcd7-b96325bfdf32",
    "year_2021": "4ba88ad4-897f-4916-b032-a3d353ed1a9a",
    "year_2020": "0d26a209-6e61-4154-9d70-8a6ad0e2d14d",
    "year_2019": "91006b8a-f018-4627-9353-4dfdab71861d",
    "year_2018": "590e5f97-461b-4f72-b448-ef52bbfd1296",
    "year_2017": "f1e9e1a3-cc95-4ce4-9dbe-081d9e6edc4a",
    "year_2016": "2fbcff61-4c3b-42a7-bdc5-02fb07e728aa",
    "year_2015": "81c8d080-864f-4dd7-aa92-325259eb00e0",
    "year_2014": "3a4673ed-9caa-4e06-8746-a8ac36de7ed2",
    "year_2013": "f8f7039f-9548-4d9c-a9fe-4d2e91015ea8",
    "year_2012": "27059645-a22f-445d-81a9-6f4f11ac403e",
    "year_2011": "312bc336-1d52-445d-98a4-ea385fd89d11",
    "year_2010": "6167002f-4a67-47f2-8250-c42fe6bb896d",
    "year_2009": "651f97ec-af99-4e24-94ac-0f00093aff99",
    "year_2008": "4042b925-4797-48c1-b417-475b2c0ea796",
}


def _normalise_postgres_dsn(dsn: str) -> str:
    return dsn.replace("postgres://", "postgresql://", 1) if dsn.startswith("postgres://") else dsn


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection settings for PostgreSQL/PostGIS."""

    dsn: str
    schema: str = "public"
    application_name: str = "toronto-parking-etl"
    connect_timeout: int = 10
    # Memory optimization: Reduced from 600000ms (10min) to 60000ms (1min)
    # Long-running queries were accumulating memory on Railway
    statement_timeout_ms: int | None = 60_000

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        candidates = [
            os.getenv("POSTGIS_DATABASE_URL"),
            os.getenv("DATABASE_URL"),
            os.getenv("PG_DSN"),
            os.getenv("DATABASE_PRIVATE_URL"),
            os.getenv("DATABASE_PUBLIC_URL"),
        ]
        dsn = next((value for value in candidates if value), None)
        if not dsn:
            raise RuntimeError(
                "DATABASE_URL (or PG_DSN) must be set in the environment")
        dsn = _normalise_postgres_dsn(dsn)
        schema = os.getenv("POSTGRES_SCHEMA", "public")
        # Memory optimization: Default reduced to 60000ms (1 minute)
        timeout = int(os.getenv("PG_STATEMENT_TIMEOUT_MS", "60000"))
        return cls(dsn=dsn, schema=schema, statement_timeout_ms=timeout)


@dataclass(frozen=True)
class RedisConfig:
    """Configuration for Redis caching."""

    url: str
    default_ttl_seconds: int = 3600
    namespace: str = "toronto:tiles"

    @classmethod
    def from_env(cls) -> "RedisConfig":
        url = (
            os.getenv("REDIS_URL")
            or os.getenv("REDIS_PUBLIC_URL")
            or os.getenv("REDIS_CONNECTION")
        )
        if not url:
            raise RuntimeError(
                "REDIS_URL (or REDIS_PUBLIC_URL) must be set in the environment")
        ttl = int(os.getenv("REDIS_DEFAULT_TTL", "3600"))
        namespace = os.getenv("REDIS_NAMESPACE", "toronto:tiles")
        return cls(url=url, default_ttl_seconds=ttl, namespace=namespace)


@dataclass(frozen=True)
class CKANResourceConfig:
    """Configuration for a CKAN resource inside a package."""

    resource_id: str
    format_hint: str | None = None
    datastore_active: bool | None = None
    package_id: str | None = None


@dataclass(frozen=True)
class DatasetConfig:
    """Represents a single dataset to be extracted and loaded."""

    slug: str
    package_id: str
    handler: str
    resources: Mapping[str, CKANResourceConfig]
    incremental_field: str | None = None
    primary_key: tuple[str, ...] = ("source_pk",)
    timezone: str = "America/Toronto"


@dataclass(frozen=True)
class StorageConfig:
    """Local storage configuration for downloaded artifacts."""

    raw_root: Path = field(
        default_factory=lambda: Path("output") / "etl" / "raw")
    staging_root: Path = field(
        default_factory=lambda: Path("output") / "etl" / "staging")

    def ensure(self) -> None:
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.staging_root.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class ETLConfig:
    """Top-level configuration for the daily ETL workflow."""

    database: DatabaseConfig
    redis: RedisConfig
    storage: StorageConfig
    datasets: tuple[DatasetConfig, ...]
    base_url: str = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    user_agent: str = "toronto-parking-etl/1.0"

    @classmethod
    def default(cls, overrides: MutableMapping[str, Mapping[str, str]] | None = None) -> "ETLConfig":
        """Build a default configuration with optional runtime overrides.

        Parameters
        ----------
        overrides:
            Optional mapping allowing resource IDs to be overridden at runtime.
            Expected shape: {dataset_slug: {resource_name: resource_id}}.
        """

        overrides = overrides or {}

        def apply_overrides(
            slug: str,
            resources: Mapping[str, CKANResourceConfig],
        ) -> Mapping[str, CKANResourceConfig]:
            mapped = {}
            overrides_for_dataset = overrides.get(slug, {})
            for name, config in resources.items():
                resource_id = overrides_for_dataset.get(
                    name, config.resource_id)
                mapped[name] = CKANResourceConfig(
                    resource_id=resource_id,
                    format_hint=config.format_hint,
                    datastore_active=config.datastore_active,
                    package_id=config.package_id,
                )
            return mapped

        datasets: list[DatasetConfig] = [
            DatasetConfig(
                slug="centreline",
                package_id="toronto-centreline-tcl",
                handler="src.etl.datasets.centreline:CentrelineETL",
                incremental_field=None,
                primary_key=("centreline_id",),
                resources=apply_overrides(
                    "centreline",
                    {
                        "metadata": CKANResourceConfig(resource_id="7bc94ccf-7bcf-4a7d-88b1-bdfc8ec5aaf1"),
                    },
                ),
            ),
            DatasetConfig(
                slug="parking_tickets",
                package_id="parking-tickets",
                handler="src.etl.datasets.parking_tickets:ParkingTicketsETL",
                incremental_field=None,
                primary_key=("ticket_number",),
                resources=apply_overrides(
                    "parking_tickets",
                    {
                        name: CKANResourceConfig(
                            resource_id=resource_id, format_hint="zip")
                        for name, resource_id in PARKING_TICKET_RESOURCES.items()
                    },
                ),
            ),
            DatasetConfig(
                slug="red_light_locations",
                package_id="red-light-cameras",
                handler="src.etl.datasets.red_light_locations:RedLightLocationsETL",
                incremental_field=None,
                primary_key=("intersection_id",),
                resources=apply_overrides(
                    "red_light_locations",
                    {
                        "locations": CKANResourceConfig(
                            resource_id="5b44dd7e-fa54-4e6b-a637-6bfbff90eeb4",
                            format_hint="csv",
                        ),
                        "charges": CKANResourceConfig(
                            resource_id="8dd4a83e-3284-4b16-9295-c256dcf62954",
                            format_hint="xlsx",
                            package_id="red-light-camera-annual-charges",
                        ),
                    },
                ),
            ),
            DatasetConfig(
                slug="ase_locations",
                package_id="automated-speed-enforcement-locations",
                handler="src.etl.datasets.ase_locations:ASELocationsETL",
                incremental_field=None,
                primary_key=("location_code",),
                resources=apply_overrides(
                    "ase_locations",
                    {
                        "locations": CKANResourceConfig(
                            resource_id="c19bc58a-d034-4e04-bbc3-0ebdf73a5feb",
                            format_hint="csv",
                        ),
                        "charges": CKANResourceConfig(
                            resource_id="a388bc08-622c-4647-bad8-ecdb7e62090a",
                            format_hint="xlsx",
                            package_id="automated-speed-enforcement-ase-charges",
                        ),
                    },
                ),
            ),
        ]

        storage = StorageConfig()
        storage.ensure()

        return cls(
            database=DatabaseConfig.from_env(),
            redis=RedisConfig.from_env(),
            storage=storage,
            datasets=tuple(datasets),
        )


__all__ = [
    "ETLConfig",
    "DatasetConfig",
    "CKANResourceConfig",
    "DatabaseConfig",
    "RedisConfig",
    "StorageConfig",
]
