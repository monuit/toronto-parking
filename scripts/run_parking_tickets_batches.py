from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Iterable, List

import dotenv


DEFAULT_YEARS = list(range(2008, 2025))


def _parse_years(spec: str | None) -> List[int]:
    if not spec:
        return DEFAULT_YEARS.copy()

    years: set[int] = set()
    for part in spec.split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            start_str, end_str = token.split("-", 1)
            start_year = int(start_str)
            end_year = int(end_str)
            if end_year < start_year:
                start_year, end_year = end_year, start_year
            years.update(range(start_year, end_year + 1))
        else:
            years.add(int(token))
    return sorted(years)


def _chunk(items: Iterable[int], size: int) -> Iterable[List[int]]:
    batch: List[int] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the parking tickets ETL in year batches so the CLI stays responsive.",
    )
    parser.add_argument(
        "--years",
        help="Comma-separated years or ranges (e.g. '2008-2012,2019,2024'). Defaults to 2008-2024.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2,
        help="Number of years to load per ETL invocation (default: 2).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=5.0,
        help="Seconds to pause between batches (default: 5).",
    )
    args = parser.parse_args(argv)

    years = _parse_years(args.years)
    if not years:
        print("No years specified; nothing to do.")
        return

    batch_size = max(1, args.batch_size)
    pause = max(0.0, args.sleep)

    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.append(str(repo_root))

    dotenv.load_dotenv(repo_root / ".env")

    from src.etl.runner import run_pipeline  # noqa: WPS433 (local import for path setup)

    batches = list(_chunk(years, batch_size))
    total_batches = len(batches)

    for index, batch in enumerate(batches, start=1):
        batch_label = f"Batch {index}/{total_batches}: years {batch[0]}"
        if len(batch) > 1:
            batch_label += f"–{batch[-1]}"
        print(f"\n=== {batch_label} ===")
        os.environ['PARKING_TICKETS_YEARS'] = ",".join(str(year) for year in batch)

        start = time.time()
        try:
            run_pipeline(['parking_tickets'])
        except Exception as exc:  # pragma: no cover - operator will inspect failures
            print(f"!! Batch {index} failed: {exc}")
            raise
        finally:
            duration = time.time() - start
            print(f"--- Completed batch {index} in {duration:.1f}s ---")

        if index < total_batches and pause > 0:
            time.sleep(pause)


if __name__ == "__main__":  # pragma: no cover
    main()
