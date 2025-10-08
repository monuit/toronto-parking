from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv


def prepare_runtime() -> None:
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")
    os.environ["POSTGIS_DATABASE_URL"] = (
        "postgresql://postgres:REDACTED_POSTGRES_PASSWORD@interchange.proxy.rlwy.net:57747/railway"
    )
    os.environ["DATABASE_URL"] = os.environ["POSTGIS_DATABASE_URL"]
    os.environ["REDIS_URL"] = (
        "redis://default:REDACTED_REDIS_PASSWORD@switchback.proxy.rlwy.net:23261"
    )
    os.environ["REDIS_PUBLIC_URL"] = os.environ["REDIS_URL"]
    os.environ["REDIS_CONNECTION"] = os.environ["REDIS_URL"]
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
