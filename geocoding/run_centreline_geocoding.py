"""Run centreline-based geocoding for Toronto parking locations."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Iterable, Iterator, Optional

import pandas as pd

from .centreline_fetcher import CentrelineFetcher
from .centreline_geocoder import CentrelineGeocoder


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Geocode parking ticket addresses using Toronto Centreline data")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("parking_data/extracted"),
        help="Root directory containing yearly parking CSV folders (used when --input-file is unspecified)",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="Optional JSONL or CSV file containing unique addresses (one per row).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/centreline_geocoded_addresses.jsonl"),
        help="Destination JSONL file for successful geocodes.",
    )
    parser.add_argument(
        "--failed-output",
        type=Path,
        default=Path("output/centreline_geocoded_failures.jsonl"),
        help="Destination JSONL file for failed lookups (addresses only).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on the number of addresses to geocode (for sampling).",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=25_000,
        help="How many addresses to process between progress reports.",
    )
    parser.add_argument(
        "--refresh-centreline",
        action="store_true",
        help="Force re-download of the centreline dataset",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Address iterators
# ---------------------------------------------------------------------------


def iter_addresses_from_jsonl(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                yield line
                continue

            if isinstance(payload, dict):
                address = payload.get("address") or payload.get("street") or payload.get("value")
                if address:
                    yield str(address)
            elif isinstance(payload, str):
                yield payload


def iter_addresses_from_csv(path: Path) -> Iterator[str]:
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


def iter_addresses_from_json(path: Path) -> Iterator[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        for item in data:
            yield str(item)
    elif isinstance(data, dict):
        for key in ("addresses", "data", "values"):
            if key in data and isinstance(data[key], list):
                for item in data[key]:
                    yield str(item)


def iter_addresses_from_dataset(root: Path) -> Iterator[str]:
    seen = set()
    csv_paths = sorted(root.glob("**/Parking_Tags_Data_*.csv"))

    for csv_path in csv_paths:
        try:
            df = pd.read_csv(
                csv_path,
                usecols=["location1", "location2"],
                encoding="utf-16",
                dtype="string",
                on_bad_lines="skip",
            )
        except UnicodeError:
            df = pd.read_csv(
                csv_path,
                usecols=["location1", "location2"],
                encoding="utf-8",
                dtype="string",
                on_bad_lines="skip",
            )
        except FileNotFoundError:
            continue

        candidates = df["location2"].fillna(df["location1"]).dropna()
        for value in candidates:
            if value is None:
                continue
            text = str(value).strip()
            if not text or text.upper() == "<NA>":
                continue
            if text in seen:
                continue
            seen.add(text)
            yield text


def iter_addresses(args: argparse.Namespace) -> Iterator[str]:
    if args.input_file is None:
        yield from iter_addresses_from_dataset(args.root)
        return

    suffix = args.input_file.suffix.lower()
    if suffix == ".jsonl":
        yield from iter_addresses_from_jsonl(args.input_file)
    elif suffix == ".csv":
        yield from iter_addresses_from_csv(args.input_file)
    elif suffix == ".json":
        yield from iter_addresses_from_json(args.input_file)
    else:
        raise ValueError(f"Unsupported input file type: {suffix}")


# ---------------------------------------------------------------------------
# Geocoding pipeline
# ---------------------------------------------------------------------------


def geocode_stream(
    geocoder: CentrelineGeocoder,
    addresses: Iterable[str],
    output_path: Path,
    failed_output_path: Optional[Path] = None,
    limit: Optional[int] = None,
    progress_interval: int = 25_000,
) -> dict:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if failed_output_path is not None:
        failed_output_path.parent.mkdir(parents=True, exist_ok=True)

    jurisdictions = Counter()
    feature_codes = Counter()
    failed_examples: list[str] = []

    total = 0
    success = 0
    failures = 0

    with output_path.open("w", encoding="utf-8") as success_fh:
        failed_fh = None
        if failed_output_path is not None:
            failed_fh = failed_output_path.open("w", encoding="utf-8")

        try:
            for address in addresses:
                if limit is not None and total >= limit:
                    break

                total += 1
                cleaned = str(address).strip()
                if not cleaned:
                    continue

                result = geocoder.geocode(cleaned)
                if result is None:
                    failures += 1
                    if failed_fh is not None:
                        failed_fh.write(json.dumps({"address": cleaned}) + "\n")
                    if len(failed_examples) < 50:
                        failed_examples.append(cleaned)
                else:
                    success += 1
                    jurisdictions[result.jurisdiction or "UNKNOWN"] += 1
                    feature_codes[result.feature_code_desc or str(result.feature_code or "UNKNOWN")] += 1
                    payload = {
                        "address": cleaned,
                        "street_normalized": result.street_normalized,
                        "latitude": result.latitude,
                        "longitude": result.longitude,
                        "centreline_id": result.centreline_id,
                        "feature_code": result.feature_code,
                        "feature_code_desc": result.feature_code_desc,
                        "jurisdiction": result.jurisdiction,
                    }
                    success_fh.write(json.dumps(payload) + "\n")

                if total % progress_interval == 0:
                    print(
                        f"Processed {total:,} addresses | "
                        f"Success: {success:,} ({(success/total*100):.1f}%)"
                    )
        finally:
            if failed_fh is not None:
                failed_fh.close()

    return {
        "total_addresses": total,
        "successful": success,
        "failed": failures,
        "success_rate": (success / total * 100.0) if total else 0.0,
        "top_jurisdictions": jurisdictions.most_common(10),
        "top_feature_codes": feature_codes.most_common(10),
        "sample_failed_addresses": failed_examples,
        "output_path": str(output_path),
        "failed_output_path": str(failed_output_path) if failed_output_path else None,
    }


def main() -> None:
    args = parse_args()

    fetcher = CentrelineFetcher()
    centreline_df = fetcher.load_dataframe(force_refresh=args.refresh_centreline)
    geocoder = CentrelineGeocoder(centreline_df)

    address_iter = iter_addresses(args)
    summary = geocode_stream(
        geocoder=geocoder,
        addresses=address_iter,
        output_path=args.output,
        failed_output_path=args.failed_output,
        limit=args.limit,
        progress_interval=args.progress_interval,
    )

    summary_path = args.output.with_suffix(".summary.json")
    summary_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
