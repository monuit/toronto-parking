"""Generate map-ready datasets from geocoded addresses and parking ticket CSVs."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
import sys
from typing import Dict, Iterable, Iterator, List, Optional

import pandas as pd
from shapely.geometry import Point, shape
from shapely.strtree import STRtree

if __name__ == "__main__" and __package__ is None:
    sys.path.append(str(Path(__file__).resolve().parent))

from map_dataset_helpers import (  # type: ignore
    LocationAggregate,
    PlateAggregate,
    build_neighbourhood_stats,
    build_plate_stats,
    build_street_stats,
    build_tickets_geojson,
    normalize_address,
    normalize_infraction,
    normalize_plate,
    parse_date_components,
    to_float,
    write_geojson,
    write_json,
)

# Columns required from ticket CSVs
USE_COLUMNS = [
    "date_of_infraction",
    "location2",
    "set_fine_amount",
    "infraction_code",
    "tag_number_masked",
]
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build map datasets for the frontend")
    parser.add_argument(
        "--geocodes",
        type=Path,
        default=Path("output/geocoded_addresses_combined.jsonl"),
        help="Path to merged geocoding results (JSONL)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("parking_data/extracted"),
        help="Directory containing yearly parking CSV folders",
    )
    parser.add_argument(
        "--neighbourhoods",
        type=Path,
        default=Path("map-app/public/data/neighbourhoods.geojson"),
        help="GeoJSON file with Toronto neighbourhood polygons",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("map-app/public/data"),
        help="Directory to write GeoJSON/JSON outputs",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=50000,
        help="Number of rows to load per CSV chunk",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of ticket rows to process (for testing)",
    )
    return parser.parse_args()


def load_geocode_map(path: Path) -> Dict[str, dict]:
    """Read the combined geocode JSONL into an uppercase lookup map."""

    lookup: Dict[str, dict] = {}
    if not path.exists():
        raise FileNotFoundError(f"Geocode file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            address = str(payload.get("address", "")).strip()
            if not address:
                continue
            key = address.upper()
            if key in lookup:
                continue
            lookup[key] = {
                "address": address,
                "latitude": float(payload["latitude"]),
                "longitude": float(payload["longitude"]),
                "source": payload.get("source", "centreline"),
                "centreline_id": payload.get("centreline_id"),
                "street_normalized": payload.get("street_normalized"),
            }
    return lookup


def list_ticket_files(data_dir: Path) -> List[Path]:
    """Return all CSV files, preferring *_fixed.csv when available per year."""

    if not data_dir.exists():
        raise FileNotFoundError(f"Ticket directory not found: {data_dir}")

    ticket_files: List[Path] = []
    for year_dir in sorted(p for p in data_dir.iterdir() if p.is_dir()):
        fixed = sorted(year_dir.glob("*_fixed.csv"))
        if fixed:
            ticket_files.extend(fixed)
            continue
        ticket_files.extend(sorted(year_dir.glob("*.csv")))
    return ticket_files


def detect_encoding(csv_path: Path) -> str:
    """Detect encoding by attempting UTF-8 read before falling back to UTF-16."""

    try:
        pd.read_csv(csv_path, nrows=1, encoding="utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "utf-16"
    except Exception:
        return "utf-8"


def iter_ticket_chunks(csv_path: Path, chunksize: int) -> Iterator[pd.DataFrame]:
    """Yield DataFrame chunks for a CSV file using the detected encoding."""

    encoding = detect_encoding(csv_path)
    reader = None
    try:
        reader = pd.read_csv(
            csv_path,
            usecols=lambda col: col in USE_COLUMNS,
            encoding=encoding,
            on_bad_lines="skip",
            chunksize=chunksize,
            low_memory=False,
            dtype={"location2": str},
        )
        for chunk in reader:
            yield chunk
    except pd.errors.ParserError:
        if reader is not None:
            reader.close()
        csv.field_size_limit(1_000_000_000)
        reader = pd.read_csv(
            csv_path,
            usecols=lambda col: col in USE_COLUMNS,
            encoding=encoding,
            on_bad_lines="skip",
            chunksize=chunksize,
            engine="python",
            dtype={"location2": str},
        )
        try:
            for chunk in reader:
                yield chunk
        except csv.Error as exc:
            print(f"⚠️  CSV warning in {csv_path.name}: {exc}; continuing with partial data")
    finally:
        if reader is not None:
            reader.close()


def build_spatial_index(neighbourhood_path: Path) -> tuple[Optional[STRtree], List[str], List]:
    if not neighbourhood_path.exists():
        print(f"⚠️  Neighbourhoods file missing: {neighbourhood_path}, skipping assignment")
        return None, [], []

    with neighbourhood_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    features = data.get("features", [])
    geometries = []
    names: List[str] = []

    for feature in features:
        geometry = feature.get("geometry")
        if not geometry:
            continue
        geom = shape(geometry)
        if geom.is_empty:
            continue
        geometries.append(geom)
        props = feature.get("properties", {})
        name = props.get("name") or props.get("AREA_NAME") or "Unknown"
        names.append(name)

    if not geometries:
        print("⚠️  No neighbourhood geometries found; proceeding without assignment")
        return None, names, geometries

    index = STRtree(geometries)
    return index, names, geometries


def assign_neighbourhoods(location_stats: Dict[str, LocationAggregate], neighbourhood_path: Path) -> None:
    index, names, geometries = build_spatial_index(neighbourhood_path)
    if index is None:
        for stats in location_stats.values():
            stats.neighbourhood = "Unknown"
        return

    name_lookup = {geom.wkb: names[idx] for idx, geom in enumerate(geometries)}

    for stats in location_stats.values():
        point = Point(stats.longitude, stats.latitude)
        assigned = "Unknown"
        for candidate in index.query(point, predicate="intersects"):
            if hasattr(candidate, "wkb"):
                assigned = name_lookup.get(candidate.wkb, assigned)
            else:
                idx = int(candidate)
                if 0 <= idx < len(names):
                    assigned = names[idx]
            if assigned != "Unknown":
                break
        stats.neighbourhood = assigned


def aggregate_tickets(
    geocodes: Dict[str, dict],
    ticket_files: Iterable[Path],
    chunksize: int,
    limit: Optional[int] = None,
) -> tuple[Dict[str, LocationAggregate], Dict[str, PlateAggregate], dict]:
    location_stats: Dict[str, LocationAggregate] = {}
    plate_stats: Dict[str, PlateAggregate] = {}

    counters = defaultdict(int)
    total_rows = 0

    for csv_path in ticket_files:
        if limit is not None and counters["processed"] >= limit:
            break

        try:
            chunk_iter = iter_ticket_chunks(csv_path, chunksize)
        except Exception as exc:
            print(f"❌ Failed to read {csv_path.name}: {exc}")
            continue

        for chunk in chunk_iter:
            for row in chunk.itertuples(index=False):
                process_ticket_row(
                    row,
                    geocodes,
                    location_stats,
                    plate_stats,
                    counters,
                )
                if limit is not None and counters["processed"] >= limit:
                    break

            total_rows += len(chunk)
            if total_rows and total_rows % 500000 == 0:
                print(f"  • Processed {total_rows:,} rows so far")

            if limit is not None and counters["processed"] >= limit:
                break

    return location_stats, plate_stats, counters
def process_ticket_row(
    row,
    geocodes: Dict[str, dict],
    location_stats: Dict[str, LocationAggregate],
    plate_stats: Dict[str, PlateAggregate],
    counters: Dict[str, int],
) -> None:
    address_key = normalize_address(getattr(row, "location2", None))
    if not address_key:
        counters["missing_location"] += 1
        return

    geocode = geocodes.get(address_key)
    if geocode is None:
        counters["missing_geocode"] += 1
        return

    date_parts = parse_date_components(getattr(row, "date_of_infraction", None))
    if date_parts is None:
        counters["invalid_date"] += 1
        return

    year, month = date_parts
    fine = to_float(getattr(row, "set_fine_amount", 0))
    infraction = normalize_infraction(getattr(row, "infraction_code", None))

    stats = location_stats.get(address_key)
    if stats is None:
        stats = LocationAggregate(
            location=geocode["address"],
            latitude=geocode["latitude"],
            longitude=geocode["longitude"],
            source=geocode.get("source", "centreline"),
        )
        location_stats[address_key] = stats

    stats.count += 1
    stats.total_revenue += fine
    stats.years.add(year)
    stats.months.add(month)
    stats.infractions[infraction] = stats.infractions.get(infraction, 0) + 1

    plate = normalize_plate(getattr(row, "tag_number_masked", None))
    if plate:
        plate_stat = plate_stats.get(plate)
        if plate_stat is None:
            plate_stat = PlateAggregate()
            plate_stats[plate] = plate_stat
        plate_stat.ticket_count += 1
        plate_stat.total_revenue += fine
        plate_stat.infractions[infraction] = plate_stat.infractions.get(infraction, 0) + 1

    counters["processed"] += 1


def main() -> None:
    args = parse_args()

    print("=== Toronto Parking Map Dataset Builder ===")
    print(f"Geocode file:        {args.geocodes}")
    print(f"Tickets directory:   {args.data_dir}")
    print(f"Neighbourhoods file: {args.neighbourhoods}")
    print(f"Output directory:    {args.output_dir}\n")

    geocodes = load_geocode_map(args.geocodes)
    print(f"Loaded geocodes: {len(geocodes):,}")

    ticket_files = list_ticket_files(args.data_dir)
    print(f"Ticket CSV files: {len(ticket_files)}")

    location_stats, plate_stats, counters = aggregate_tickets(
        geocodes,
        ticket_files,
        chunksize=args.chunksize,
        limit=args.limit,
    )

    print(f"Processed tickets: {counters['processed']:,}")
    print(f"Missing geocode matches: {counters['missing_geocode']:,}")
    print(f"Missing location2: {counters['missing_location']:,}")
    print(f"Invalid dates: {counters['invalid_date']:,}\n")

    assign_neighbourhoods(location_stats, args.neighbourhoods)

    output_dir = args.output_dir
    tickets_geojson = build_tickets_geojson(location_stats)
    street_stats = build_street_stats(location_stats)
    plate_output = build_plate_stats(plate_stats)
    neighbourhood_stats = build_neighbourhood_stats(location_stats)

    write_geojson(output_dir / "tickets_aggregated.geojson", tickets_geojson)
    write_json(output_dir / "street_stats.json", street_stats)
    write_json(output_dir / "officer_stats.json", plate_output)
    write_json(output_dir / "neighbourhood_stats.json", neighbourhood_stats)

    print("Outputs written:")
    print(f"  • tickets_aggregated.geojson ({len(tickets_geojson['features']):,} features)")
    print(f"  • street_stats.json ({len(street_stats):,} entries)")
    print(f"  • officer_stats.json ({len(plate_output):,} entries)")
    print(f"  • neighbourhood_stats.json ({len(neighbourhood_stats):,} entries)")


if __name__ == "__main__":
    main()
