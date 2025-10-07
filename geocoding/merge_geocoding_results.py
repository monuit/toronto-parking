"""Merge centreline geocoding outputs with maps.co fallback results without duplicates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, Tuple


def _load_success_map(path: Path, source: str) -> Dict[str, dict]:
    mapping: Dict[str, dict] = {}
    if not path.exists():
        return mapping

    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            payload = json.loads(line)
            address = str(payload.get("address", "")).strip()
            if not address:
                continue
            key = address.upper()
            if key in mapping:
                continue
            payload["source"] = payload.get("source", source)
            mapping[key] = payload
    return mapping


def merge_results(
    centreline_paths: Iterable[Path],
    fallback_path: Path,
    output_path: Path,
) -> Tuple[int, int, int]:
    combined: Dict[str, dict] = {}
    centreline_count = 0

    for path in centreline_paths:
        centreline_data = _load_success_map(path, "centreline")
        centreline_count += len(centreline_data)
        for key, payload in centreline_data.items():
            if key not in combined:
                combined[key] = payload

    unique_centreline = len(combined)

    fallback_results = _load_success_map(fallback_path, "maps_co")
    added = 0

    for key, payload in fallback_results.items():
        if key in combined:
            continue
        combined[key] = payload
        added += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for record in combined.values():
            fh.write(json.dumps(record) + "\n")

    return unique_centreline, added, len(combined)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge centreline and maps.co geocoding outputs")
    parser.add_argument(
        "--centreline",
        type=Path,
        nargs="+",
        default=[
            Path("output/centreline_geocoded_addresses.jsonl"),
        ],
        help="One or more JSONL files with centreline successes",
    )
    parser.add_argument(
        "--fallback",
        type=Path,
        default=Path("output/mapsco_geocoded_addresses.jsonl"),
        help="JSONL file with maps.co successes",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/geocoded_addresses_combined.jsonl"),
        help="Destination JSONL for merged results",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    centreline_count, added, total = merge_results(args.centreline, args.fallback, args.output)

    print("=== MERGE SUMMARY ===")
    print(f"Centreline successes: {centreline_count:,}")
    print(f"Fallback successes added: {added:,}")
    print(f"Total unique addresses: {total:,}")
    print(f"Merged output saved to: {args.output}")


if __name__ == "__main__":
    main()
