"""Fallback geocoding using the geocode.maps.co API for unresolved addresses.

Single Responsibility: submit a list of addresses to the maps.co API, resume safely,
write successes/failures to JSONL, and respect existing results to avoid duplicates.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from dotenv import load_dotenv

from .run_geocoding import RobustGeocoder


# MARK: address loaders

def _iter_jsonl(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                yield line.strip()
                continue

            if isinstance(payload, dict):
                value = payload.get("address") or payload.get("query") or payload.get("value")
                if value:
                    yield str(value)
            elif isinstance(payload, str):
                yield payload


def _iter_csv(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        sample = fh.read(1024)
        fh.seek(0)
        try:
            has_header = csv.Sniffer().has_header(sample)
        except csv.Error:
            has_header = False

        if has_header:
            reader = csv.DictReader(fh)
            for row in reader:
                if not row:
                    continue
                value = row.get("address")
                if value is None and reader.fieldnames:
                    value = row.get(reader.fieldnames[0])
                if value:
                    yield value
        else:
            reader = csv.reader(fh)
            for row in reader:
                if row:
                    yield row[0]


def _iter_json(path: Path) -> Iterator[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        for item in data:
            yield str(item)
    elif isinstance(data, dict):
        for key in ("addresses", "queries", "data", "values"):
            value = data.get(key)
            if isinstance(value, list):
                for item in value:
                    yield str(item)


def load_addresses(path: Path, limit: Optional[int] = None) -> List[str]:
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        iterator = _iter_jsonl(path)
    elif suffix == ".csv":
        iterator = _iter_csv(path)
    elif suffix == ".json":
        iterator = _iter_json(path)
    else:
        raise ValueError(f"Unsupported input format: {suffix}")

    results: List[str] = []
    seen = set()
    for value in iterator:
        cleaned = str(value).strip()
        if not cleaned:
            continue
        key = cleaned.upper()
        if key in seen:
            continue
        seen.add(key)
        results.append(cleaned)
        if limit is not None and len(results) >= limit:
            break
    return results


# MARK: exporting helpers

def export_results(results: Dict[str, Dict], output_path: Path, failed_path: Path) -> Dict[str, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    failed_path.parent.mkdir(parents=True, exist_ok=True)

    success_count = 0
    failure_count = 0

    with output_path.open("w", encoding="utf-8") as success_fh, failed_path.open(
        "w", encoding="utf-8"
    ) as failure_fh:
        for query, payload in results.items():
            lat = payload.get("lat")
            lon = payload.get("lon")
            status = payload.get("status", "unknown")

            if lat is not None and lon is not None:
                success_count += 1
                record = {
                    "address": query,
                    "latitude": lat,
                    "longitude": lon,
                    "display_name": payload.get("display_name"),
                    "type": payload.get("type"),
                    "importance": payload.get("importance"),
                    "source": "maps_co",
                }
                success_fh.write(json.dumps(record) + "\n")
            else:
                failure_count += 1
                failure_record = {
                    "address": query,
                    "status": status,
                    "error": payload.get("error_message"),
                    "source": "maps_co",
                }
                failure_fh.write(json.dumps(failure_record) + "\n")

    return {"successful": success_count, "failed": failure_count}


# MARK: command line interface

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fallback geocoding via geocode.maps.co API")
    parser.add_argument("--input-file", type=Path, required=True, help="JSON/JSONL/CSV file of addresses to geocode")
    parser.add_argument(
        "--cache-file",
        type=Path,
        default=Path("output/mapsco_geocoding_cache.json"),
        help="Persisted cache of API responses for resume",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/mapsco_geocoded_addresses.jsonl"),
        help="Destination JSONL of successful geocodes",
    )
    parser.add_argument(
        "--failed-output",
        type=Path,
        default=Path("output/mapsco_geocoded_failures.jsonl"),
        help="Destination JSONL of failed geocodes",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of addresses (useful for smoke tests)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=1.8,
        help="Requests per second to respect API quotas (default 1.8 to stay below 2 rps cap)",
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=100,
        help="How many addresses to process between checkpoints",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    addresses = load_addresses(args.input_file, limit=args.limit)
    if not addresses:
        print("No addresses to geocode.")
        return

    print(f"Loaded {len(addresses):,} addresses for maps.co fallback")

    load_dotenv()
    api_key = os.getenv("GEOCODE_MAPS_CO_API_KEY")
    if not api_key:
        raise RuntimeError("GEOCODE_MAPS_CO_API_KEY is not set in the environment")

    geocoder = RobustGeocoder(
        api_key=api_key,
        cache_file=str(args.cache_file),
        checkpoint_interval=args.checkpoint_interval,
        rate_limit=args.rate_limit,
    )

    results = geocoder.run(addresses)

    summary = export_results(results, args.output, args.failed_output)

    print("\n=== MAPS.CO FALLBACK SUMMARY ===")
    print(f"Addresses attempted: {len(addresses):,}")
    print(f"Successful: {summary['successful']:,}")
    print(f"Failed: {summary['failed']:,}")
    print(f"Success rate: {(summary['successful'] / len(addresses) * 100):.2f}%")
    print(f"Results saved to: {args.output}")
    print(f"Failures saved to: {args.failed_output}")
    print(f"Cache persisted at: {args.cache_file}")


if __name__ == "__main__":
    main()
