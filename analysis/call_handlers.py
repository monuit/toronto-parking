from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv


POSTGIS_ENV_KEY = "POSTGIS_DATABASE_URL"
REDIS_ENV_KEY = "REDIS_URL"


def prepare_runtime() -> None:
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")

    database_url = os.environ.get(POSTGIS_ENV_KEY) or os.environ.get("DATABASE_URL")
    redis_url = (
        os.environ.get(REDIS_ENV_KEY)
        or os.environ.get("REDIS_PUBLIC_URL")
        or os.environ.get("REDIS_CONNECTION")
    )

    if not database_url:
        raise RuntimeError(
            "POSTGIS_DATABASE_URL (or DATABASE_URL) must be set in the environment"
        )
    if not redis_url:
        raise RuntimeError(
            "REDIS_URL (or REDIS_PUBLIC_URL/REDIS_CONNECTION) must be set in the environment"
        )

    os.environ[POSTGIS_ENV_KEY] = database_url
    os.environ.setdefault("DATABASE_URL", database_url)
    os.environ[REDIS_ENV_KEY] = redis_url
    os.environ.setdefault("REDIS_PUBLIC_URL", redis_url)
    os.environ.setdefault("REDIS_CONNECTION", redis_url)
    if str(root) not in sys.path:
        sys.path.append(str(root))
    src_root = root / "src"
    if str(src_root) not in sys.path:
        sys.path.append(str(src_root))


def dump_dataset_totals() -> None:
    from api.dataset_totals import handler

    class Request:
        method = "GET"
        args = {}

    for dataset in ("parking_tickets", "red_light_locations", "ase_locations"):
        Request.args = {"dataset": dataset}
        print(dataset, handler(Request))


def dump_map_summary(dataset: str) -> None:
    from api.map_summary import handler

    class Request:
        method = "GET"
        args = {
            "west": "-79.5",
            "south": "43.6",
            "east": "-79.2",
            "north": "43.7",
            "zoom": "13",
            "dataset": dataset,
        }

    print(dataset, handler(Request))


def main() -> None:
    prepare_runtime()
    dump_dataset_totals()
    for dataset in ("red_light_locations", "ase_locations"):
        dump_map_summary(dataset)


if __name__ == "__main__":
    main()
