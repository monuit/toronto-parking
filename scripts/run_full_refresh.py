"""Orchestrate the full refresh pipeline for parking and camera datasets."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence

import dotenv
import psycopg
import certifi
import warnings

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_FILE = REPO_ROOT / "output" / "etl" / "refresh_runs.log"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.etl.bootstrap import ensure_base_tables  # noqa: E402
from src.etl.postgres import PostgresClient  # noqa: E402
from src.tiles.schema import TileSchemaManager  # noqa: E402
from src.etl.runner import run_pipeline as run_core_datasets  # noqa: E402

# MARK: Constants

PYTHON = Path(sys.executable)
DEFAULT_REQUIRED_TABLES: tuple[str, ...] = (
    "ase_camera_locations",
    "ase_camera_locations_staging",
    "ase_camera_tiles",
    "ase_charges",
    "ase_yearly_locations",
    "camera_ward_totals",
    "centreline_segments",
    "centreline_segments_staging",
    "city_wards",
    "etl_state",
    "geography_columns",
    "geometry_columns",
    "parking_ticket_tiles",
    "parking_ticket_yearly_locations",
    "parking_ticket_yearly_neighbourhoods",
    "parking_ticket_yearly_streets",
    "parking_tickets",
    "parking_tickets_staging",
    "raster_columns",
    "raster_overviews",
    "red_light_camera_locations",
    "red_light_camera_locations_staging",
    "red_light_camera_tiles",
    "red_light_charges",
    "red_light_yearly_locations",
    "spatial_ref_sys",
)

SCRIPT_STEPS: tuple[tuple[str, Path, Sequence[str]], ...] = (
    (
        "Build yearly metrics",
        REPO_ROOT / "scripts" / "build_yearly_metrics.py",
        (),
    ),
    (
        "Build camera datasets",
        REPO_ROOT / "preprocessing" / "build_camera_datasets.py",
        (),
    ),
    (
        "Build ward datasets",
        REPO_ROOT / "scripts" / "build_camera_ward_datasets.py",
        (),
    ),
    (
        "Push tickets to Redis",
        REPO_ROOT / "scripts" / "push_tickets_to_redis.py",
        (),
    ),
)


# MARK: Utilities


def load_environment() -> None:
    warnings.filterwarnings(
        "ignore",
        message="pandas only supports SQLAlchemy",
        category=UserWarning,
    )
    dotenv.load_dotenv(REPO_ROOT / ".env")


def resolve_database_url(provided: str | None) -> str:
    if provided:
        return provided
    for key in ("POSTGIS_DATABASE_URL", "DATABASE_URL", "POSTGRES_URL"):
        value = os.getenv(key)
        if value:
            return value
    raise RuntimeError("Database URL not provided; set --database-url or DATABASE_URL")


def resolve_redis_url(provided: str | None) -> str | None:
    if provided:
        return provided
    for key in ("REDIS_PUBLIC_URL", "REDIS_URL", "REDIS_CONNECTION"):
        value = os.getenv(key)
        if value:
            return value
    return None


@contextmanager
def temporary_env(overrides: dict[str, str]) -> Iterable[None]:
    original = os.environ.copy()
    os.environ.update(overrides)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


def ensure_requests_ca(env: dict[str, str]) -> None:
    candidate = env.get("REQUESTS_CA_BUNDLE") or os.getenv("REQUESTS_CA_BUNDLE")
    if candidate:
        if Path(candidate).exists():
            env.setdefault("SSL_CERT_FILE", candidate)
            return
        # Remove invalid bundle to avoid requests/openssl failures
        env.pop("REQUESTS_CA_BUNDLE", None)
    ca_path = certifi.where()
    env.setdefault("REQUESTS_CA_BUNDLE", ca_path)
    env.setdefault("SSL_CERT_FILE", ca_path)


def ensure_tile_schema(dsn: str, quadkey_zoom: int, quadkey_prefix: int) -> None:
    client = PostgresClient(dsn=dsn, application_name="toronto-parking-refresh")
    manager = TileSchemaManager(client, quadkey_zoom=quadkey_zoom, quadkey_prefix_length=quadkey_prefix, logger=print)
    manager.ensure(include_tile_tables=False)


def verify_tables(dsn: str, tables: Iterable[str]) -> None:
    missing: list[str] = []
    query = "SELECT to_regclass(%s)"
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for table in tables:
                lookup = table if "." in table else f"public.{table}"
                cur.execute(query, (lookup,))
                regclass = cur.fetchone()
                if not regclass or regclass[0] is None:
                    missing.append(table)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise RuntimeError(
            "Unable to verify database prerequisites. Missing tables/views: "
            f"{missing_str}. Run the base dataset reload before this orchestrator."
        )


def run_python_script(script_path: Path, args: Sequence[str], env: dict[str, str]) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")
    command = [str(PYTHON), str(script_path)] + list(args)
    print(f"-> Running: {' '.join(command)}")
    subprocess.run(command, check=True, env=env)


def run_restart_commands(commands: list[list[str]]) -> None:
    for command in commands:
        if not command:
            continue
        print(f"-> Restarting server via: {' '.join(command)}")
        subprocess.run(command, check=True)


def prompt_for_log(log_file: Path, non_interactive: bool) -> None:
    if non_interactive:
        print("Skipping log prompt (--non-interactive)")
        return
    try:
        entry = input("Enter a log entry for this refresh (leave blank to skip): ").strip()
    except EOFError:
        entry = ""
    if not entry:
        print("No log entry recorded.")
        return
    timestamp = datetime.now(timezone.utc).isoformat()
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {entry}\n")
    print(f"Appended log entry to {log_file}")


# MARK: CLI


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full parking + camera refresh workflow")
    parser.add_argument("--database-url", help="Postgres connection string (defaults to env vars)")
    parser.add_argument("--redis-url", help="Redis connection string override")
    parser.add_argument(
        "--skip-etl",
        action="store_true",
        help="Skip running the CKAN ETL pipeline before downstream steps",
    )
    parser.add_argument(
        "--parking-years",
        nargs="*",
        type=int,
        help="Restrict the parking tickets ETL to specific years (space separated)",
    )
    parser.add_argument(
        "--parking-years-last",
        type=int,
        help="Restrict the parking tickets ETL to the N most recent years",
    )
    parser.add_argument(
        "--restart-command",
        action="append",
        nargs="+",
        help="Command to restart the Node server (can be supplied multiple times)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=DEFAULT_LOG_FILE,
        help=f"Path to append run log entries (default: {DEFAULT_LOG_FILE.relative_to(REPO_ROOT)})",
    )
    parser.add_argument(
        "--skip-tiles",
        action="store_true",
        help="Skip tile schema assurance step",
    )
    parser.add_argument(
        "--quadkey-zoom",
        type=int,
        default=12,
        help="Quadkey zoom level when ensuring tile schema (default: 12)",
    )
    parser.add_argument(
        "--quadkey-prefix",
        type=int,
        default=6,
        help="Quadkey prefix length when ensuring tile schema (default: 6)",
    )
    parser.add_argument(
        "--force-ward-download",
        action="store_true",
        help="Force re-download of ward GeoJSON when building ward datasets",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Disable interactive prompts (log entry)",
    )
    parser.add_argument(
        "--additional-table",
        action="append",
        dest="additional_tables",
        default=None,
        help="Extra table/view name to verify exists",
    )
    parsed = parser.parse_args(list(argv) if argv is not None else None)
    if parsed.parking_years_last and parsed.parking_years:
        parser.error("Use either --parking-years or --parking-years-last, not both")
    if parsed.parking_years_last is not None and parsed.parking_years_last <= 0:
        parser.error("--parking-years-last must be positive")
    return parsed


# MARK: Main entrypoint


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)

    load_environment()
    dsn = resolve_database_url(args.database_url)
    redis_url = resolve_redis_url(args.redis_url)

    required_tables = list(DEFAULT_REQUIRED_TABLES)
    if args.additional_tables:
        required_tables.extend(args.additional_tables)

    base_env_overrides: dict[str, str] = {
        "DATABASE_URL": dsn,
        "POSTGIS_DATABASE_URL": dsn,
    }
    if redis_url:
        base_env_overrides["REDIS_URL"] = redis_url
        base_env_overrides["REDIS_PUBLIC_URL"] = redis_url

    env = os.environ.copy()
    env.update(base_env_overrides)
    ensure_requests_ca(env)

    parking_years_env: dict[str, str] = {}
    if args.parking_years_last is not None:
        current_year = datetime.now(timezone.utc).year
        years = [current_year - offset for offset in range(args.parking_years_last)]
        parking_years_env["PARKING_TICKETS_YEARS"] = ",".join(str(year) for year in sorted(set(years)))
    elif args.parking_years:
        years = sorted(set(args.parking_years))
        parking_years_env["PARKING_TICKETS_YEARS"] = ",".join(str(year) for year in years)
    if parking_years_env:
        env.update(parking_years_env)

    print("=== Ensuring base tables ===")
    ensure_base_tables(dsn)

    if args.skip_etl:
        print("=== Skipping ETL pipeline (--skip-etl) ===")
    else:
        print("=== Running CKAN ETL pipeline ===")
        etl_env = base_env_overrides.copy()
        etl_env.update(parking_years_env)
        ensure_requests_ca(etl_env)
        if redis_url:
            etl_env["REDIS_URL"] = redis_url
            etl_env.setdefault("REDIS_PUBLIC_URL", redis_url)
        with temporary_env(etl_env):
            run_core_datasets()

    if not args.skip_tiles:
        print("=== Ensuring tile schema ===")
        ensure_tile_schema(dsn, args.quadkey_zoom, args.quadkey_prefix)

    for label, script_path, script_args in SCRIPT_STEPS:
        step_args = list(script_args)
        if label != "Push tickets to Redis":
            step_args.extend(["--database-url", dsn])
        if label == "Build ward datasets" and redis_url:
            step_args.extend(["--redis-url", redis_url])
        if label == "Build ward datasets" and args.force_ward_download:
            step_args.append("--force-download")
        print(f"=== {label} ===")
        run_python_script(script_path, step_args, env)

    print("=== Verifying required tables ===")
    verify_tables(dsn, required_tables)
    print("All prerequisite tables present.")

    print("=== Refresh complete ===")

    if args.restart_command:
        print("=== Restarting Node server ===")
        run_restart_commands(args.restart_command)
    else:
        print("No restart command provided; remember to restart the Node server manually.")

    prompt_for_log(args.log_file, args.non_interactive)


if __name__ == "__main__":
    main()
