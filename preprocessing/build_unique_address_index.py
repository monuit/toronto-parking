"""Build a deduplicated address index from the raw parking ticket CSVs.

Single Responsibility: iterate through parking CSV exports and emit unique, normalized
addresses suitable for centreline geocoding.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple

import pandas as pd
from pandas.errors import ParserError

# Increase CSV parser field size limit for wide columns
try:
    csv.field_size_limit(min(sys.maxsize, 2_147_483_647))
except OverflowError:
    csv.field_size_limit(2_147_483_647)

# MARK: data structures


@dataclass
class AddressRecord:
    """Represents a single normalized address entry."""

    normalized: str
    sample_original: str
    count: int
    source_fields: Set[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "address": self.normalized,
            "sample_original": self.sample_original,
            "count": self.count,
            "source_fields": sorted(self.source_fields),
        }


# MARK: normalization helpers

_PREFIXES = {
    "NR",
    "AT",
    "OPP",
    "S/S",
    "S S",
    "N/S",
    "N S",
    "E/O",
    "E O",
    "W/O",
    "W O",
    "E",
    "W",
    "N",
    "S",
    "SB",
    "NB",
    "EB",
    "WB",
    "N/B",
    "S/B",
    "E/B",
    "W/B",
    "N B",
    "S B",
    "E B",
    "W B",
}

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_address(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None

    text = str(raw).strip().upper()
    if not text or text == "NAN":
        return None

    # Collapse repeated whitespace
    text = _WHITESPACE_RE.sub(" ", text)

    # Remove punctuation that separates prefixes (e.g., NR-, NR:)
    text = text.replace("-", " ").replace(":", " ").strip()
    text = _WHITESPACE_RE.sub(" ", text)

    # Remove leading prefixes repeatedly
    while True:
        parts = text.split(" ", 1)
        if len(parts) == 1:
            break
        candidate = parts[0]
        if candidate in _PREFIXES:
            text = parts[1].lstrip()
            continue
        # Remove forms like "NB," or "SB." etc
        cleaned = candidate.rstrip(",.")
        if cleaned in _PREFIXES:
            text = parts[1].lstrip()
            continue
        break

    # Trim trailing commas and whitespace
    text = text.strip(" ,")
    if not text:
        return None

    return text


def pick_primary_address(row) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Select the primary address field from a row-like object."""

    for field in ("location2", "location4", "location1", "location3"):
        # Support namedtuple rows (from itertuples) and dict-like rows
        if isinstance(row, tuple):
            value = getattr(row, field, None)
        else:
            value = row.get(field)
        normalized = normalize_address(value)
        if normalized:
            original = str(value).strip()
            return normalized, original, field
    return None, None, None


# MARK: core extraction logic


def iter_parking_csvs(root: Path) -> Iterator[Path]:
    for path in sorted(root.glob("**/Parking_Tags_Data_*.csv")):
        if path.is_file():
            yield path


def determine_encoding(csv_path: Path) -> str:
    try:
        pd.read_csv(csv_path, nrows=1, encoding="utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "utf-16"


def aggregate_addresses(csv_path: Path, storage: Dict[str, AddressRecord]) -> Tuple[int, int]:
    encoding = determine_encoding(csv_path)

    def _chunk_reader(engine: str):
        return pd.read_csv(
            csv_path,
            encoding=encoding,
            on_bad_lines="skip",
            chunksize=200_000,
            usecols=["location1", "location2", "location3", "location4"],
            dtype="string",
            engine=engine,
        )

    last_error: Optional[ParserError] = None

    for engine in ("c", "python"):
        try:
            chunk_iter = _chunk_reader(engine)
            rows_processed = 0
            rows_with_address = 0
            for chunk in chunk_iter:
                chunk = chunk.fillna("")
                for row in chunk.itertuples(index=False, name="Row"):
                    normalized, original, source_field = pick_primary_address(row)
                    rows_processed += 1

                    if not normalized:
                        continue

                    rows_with_address += 1
                    record = storage.get(normalized)
                    if record is None:
                        storage[normalized] = AddressRecord(
                            normalized=normalized,
                            sample_original=original or normalized,
                            count=1,
                            source_fields={source_field} if source_field else set(),
                        )
                    else:
                        record.count += 1
                        if source_field:
                            record.source_fields.add(source_field)
                        if not record.sample_original and original:
                            record.sample_original = original
            last_error = None
            return rows_processed, rows_with_address
        except (ParserError, csv.Error) as exc:
            last_error = exc
            continue

    if last_error is not None:
        try:
            rows_processed = 0
            rows_with_address = 0
            with csv_path.open("r", encoding=encoding, newline="") as fh:
                header_line = fh.readline()
                if not header_line:
                    return rows_processed, rows_with_address

                header_reader = csv.reader([header_line])
                try:
                    raw_fieldnames = next(header_reader)
                except StopIteration:
                    return rows_processed, rows_with_address

                fieldnames = [name.strip().lstrip("\ufeff") for name in raw_fieldnames]
                lower_field_map = {name.lower(): name for name in fieldnames}

                buffer = ""
                for raw_line in fh:
                    buffer += raw_line
                    try:
                        row_values = next(csv.reader([buffer]))
                    except (csv.Error, StopIteration):
                        # If unmatched quotes, continue accumulating
                        if buffer.count("\"") % 2 != 0:
                            continue
                        buffer = ""
                        continue

                    buffer = ""

                    rows_processed += 1
                    row_dict = {}
                    for idx, field in enumerate(fieldnames):
                        value = row_values[idx] if idx < len(row_values) else ""
                        row_dict[field] = value

                    location_values = {
                        "location1": row_dict.get(lower_field_map.get("location1", "location1"), ""),
                        "location2": row_dict.get(lower_field_map.get("location2", "location2"), ""),
                        "location3": row_dict.get(lower_field_map.get("location3", "location3"), ""),
                        "location4": row_dict.get(lower_field_map.get("location4", "location4"), ""),
                    }

                    normalized, original, source_field = pick_primary_address(location_values)
                    if not normalized:
                        continue

                    rows_with_address += 1
                    record = storage.get(normalized)
                    if record is None:
                        storage[normalized] = AddressRecord(
                            normalized=normalized,
                            sample_original=original or normalized,
                            count=1,
                            source_fields={source_field} if source_field else set(),
                        )
                    else:
                        record.count += 1
                        if source_field:
                            record.source_fields.add(source_field)
                        if not record.sample_original and original:
                            record.sample_original = original

                # Handle trailing buffer that still contained unmatched quotes by skipping
                return rows_processed, rows_with_address
        except Exception as exc:  # pragma: no cover - fallback failure
            raise last_error from exc

    return 0, 0


# MARK: persistence


def write_jsonl(records: List[AddressRecord], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record.to_dict(), ensure_ascii=False) + "\n")


def write_csv(records: List[AddressRecord], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["address", "sample_original", "count", "source_fields"]
    with destination.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "address": record.normalized,
                    "sample_original": record.sample_original,
                    "count": record.count,
                    "source_fields": ";".join(sorted(record.source_fields)),
                }
            )


# MARK: command-line interface


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract unique addresses for centreline geocoding")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("parking_data/extracted"),
        help="Root directory containing yearly parking CSVs",
    )
    parser.add_argument(
        "--jsonl-output",
        type=Path,
        default=Path("output/unique_addresses.jsonl"),
        help="Destination JSONL file (one address per line)",
    )
    parser.add_argument(
        "--csv-output",
        type=Path,
        default=Path("output/unique_addresses.csv"),
        help="Destination CSV file summarizing addresses",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of files processed (for testing)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    storage: Dict[str, AddressRecord] = {}

    csv_paths = list(iter_parking_csvs(args.root))
    if args.limit is not None:
        csv_paths = csv_paths[: args.limit]

    total_rows = 0
    rows_with_address = 0

    for idx, csv_path in enumerate(csv_paths, start=1):
        try:
            processed, with_address = aggregate_addresses(csv_path, storage)
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"[{idx}/{len(csv_paths)}] {csv_path.name}: FAILED ({exc})")
            continue
        total_rows += processed
        rows_with_address += with_address
        print(
            f"[{idx}/{len(csv_paths)}] {csv_path.name}: "
            f"{processed:,} rows, {with_address:,} with addresses. "
            f"Unique so far: {len(storage):,}"
        )

    records = sorted(storage.values(), key=lambda r: (-r.count, r.normalized))

    print("")
    print("=== ADDRESS EXTRACTION SUMMARY ===")
    print(f"CSV files processed: {len(csv_paths)}")
    print(f"Rows processed: {total_rows:,}")
    print(f"Rows with addresses: {rows_with_address:,}")
    print(f"Unique addresses: {len(records):,}")

    write_jsonl(records, args.jsonl_output)
    print(f"Saved JSONL to {args.jsonl_output} ({len(records):,} lines)")

    write_csv(records, args.csv_output)
    print(f"Saved CSV to {args.csv_output}")

    summary_path = args.jsonl_output.with_suffix(".summary.json")
    summary = {
        "csv_files_processed": len(csv_paths),
        "rows_processed": total_rows,
        "rows_with_address": rows_with_address,
        "unique_addresses": len(records),
        "jsonl_output": str(args.jsonl_output),
        "csv_output": str(args.csv_output),
        "sample_records": [record.to_dict() for record in records[:10]],
    }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"Saved summary to {summary_path}")


if __name__ == "__main__":
    main()
