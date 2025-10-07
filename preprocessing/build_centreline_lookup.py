"""Build a centreline lookup dataset for street drill-down interactions."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

import sys

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from preprocessing.build_map_datasets import load_geocode_map  # noqa: E402


@dataclass
class LocationEntry:
    """Aggregated ticket statistics for a single address."""

    location: str
    ticket_count: int
    total_revenue: float
    top_infraction: Optional[str] = None

    def to_dict(self) -> dict:
        payload: dict[str, object] = {
            "location": self.location,
            "ticketCount": self.ticket_count,
            "totalRevenue": round(self.total_revenue, 2),
        }
        if self.top_infraction:
            payload["topInfraction"] = self.top_infraction
        return payload


@dataclass
class CentrelineDetail:
    """Roll-up of ticket metrics for a single centreline segment."""

    centreline_id: int
    street: str = "Unknown"
    ticket_count: int = 0
    total_revenue: float = 0.0
    years: set[int] = field(default_factory=set)
    months: set[int] = field(default_factory=set)
    locations: list[LocationEntry] = field(default_factory=list)
    infractions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    bbox: Optional[tuple[float, float, float, float]] = None

    def to_dict(self, *, location_limit: int = 8, infraction_limit: int = 5) -> dict:
        ordered_locations = sorted(
            self.locations,
            key=lambda entry: entry.ticket_count,
            reverse=True,
        )[:location_limit]

        ordered_infractions = sorted(
            ((code, count) for code, count in self.infractions.items() if code),
            key=lambda item: item[1],
            reverse=True,
        )[:infraction_limit]

        payload: dict[str, object] = {
            "centrelineId": self.centreline_id,
            "street": self.street,
            "ticketCount": self.ticket_count,
            "totalRevenue": round(self.total_revenue, 2),
            "years": sorted(self.years),
            "months": sorted(self.months),
        }
        if ordered_locations:
            payload["topLocations"] = [entry.to_dict() for entry in ordered_locations]
        if ordered_infractions:
            payload["topInfractions"] = [
                {"code": code, "count": count} for code, count in ordered_infractions
            ]
        if self.bbox:
            payload["bbox"] = [round(value, 6) for value in self.bbox]
        return payload


@dataclass
class StreetSummary:
    """Aggregated metrics across all centreline segments for a street."""

    name: str
    centreline_ids: set[int] = field(default_factory=set)
    ticket_count: int = 0
    total_revenue: float = 0.0
    years: set[int] = field(default_factory=set)
    months: set[int] = field(default_factory=set)
    locations: list[LocationEntry] = field(default_factory=list)
    infractions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    bbox: Optional[tuple[float, float, float, float]] = None

    def extend_bbox(self, candidate: Optional[tuple[float, float, float, float]]) -> None:
        if candidate is None:
            return
        if self.bbox is None:
            self.bbox = candidate
            return
        min_lng, min_lat, max_lng, max_lat = self.bbox
        c_min_lng, c_min_lat, c_max_lng, c_max_lat = candidate
        self.bbox = (
            min(min_lng, c_min_lng),
            min(min_lat, c_min_lat),
            max(max_lng, c_max_lng),
            max(max_lat, c_max_lat),
        )

    def to_dict(self, *, location_limit: int = 12, infraction_limit: int = 5) -> dict:
        ordered_locations = sorted(
            self.locations,
            key=lambda entry: entry.ticket_count,
            reverse=True,
        )[:location_limit]

        ordered_infractions = sorted(
            ((code, count) for code, count in self.infractions.items() if code),
            key=lambda item: item[1],
            reverse=True,
        )[:infraction_limit]

        payload: dict[str, object] = {
            "street": self.name,
            "centrelineIds": sorted(self.centreline_ids),
            "ticketCount": self.ticket_count,
            "totalRevenue": round(self.total_revenue, 2),
            "years": sorted(self.years),
            "months": sorted(self.months),
        }
        if ordered_locations:
            payload["topLocations"] = [entry.to_dict() for entry in ordered_locations]
        if ordered_infractions:
            payload["topInfractions"] = [
                {"code": code, "count": count} for code, count in ordered_infractions
            ]
        if self.bbox:
            payload["bbox"] = [round(value, 6) for value in self.bbox]
        return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build centreline lookup dataset")
    parser.add_argument(
        "--geocodes",
        type=Path,
        default=Path("output/geocoded_addresses_combined.jsonl"),
        help="Path to combined geocode JSONL file",
    )
    parser.add_argument(
        "--glow-lines",
        type=Path,
        default=Path("map-app/public/data/tickets_glow_lines.geojson"),
        help="Glow line GeoJSON file",
    )
    parser.add_argument(
        "--aggregated",
        type=Path,
        default=Path("map-app/public/data/tickets_aggregated.geojson"),
        help="Aggregated ticket GeoJSON file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("map-app/public/data/centreline_lookup.json"),
        help="Output JSON file",
    )
    parser.add_argument(
        "--location-limit",
        type=int,
        default=12,
        help="Maximum number of top locations to include per street",
    )
    return parser.parse_args()


def build_centreline_metadata(geocode_lookup: dict[str, dict]) -> dict[int, dict]:
    metadata: dict[int, dict] = {}
    for entry in geocode_lookup.values():
        centreline_id = entry.get("centreline_id")
        if not centreline_id:
            continue
        try:
            cid = int(centreline_id)
        except (TypeError, ValueError):
            continue
        if cid in metadata:
            if metadata[cid].get("street_normalized"):
                continue
        metadata[cid] = entry
    return metadata


def flatten_coordinates(coords: Iterable) -> Iterable[tuple[float, float]]:
    if not isinstance(coords, Iterable):
        return

    coords_list = list(coords)
    if not coords_list:
        return

    first = coords_list[0]
    if isinstance(first, (float, int)):
        if len(coords_list) >= 2:
            yield float(coords_list[0]), float(coords_list[1])
        return

    for item in coords_list:
        yield from flatten_coordinates(item)


def compute_bbox(geometry: Optional[dict]) -> Optional[tuple[float, float, float, float]]:
    if not geometry:
        return None

    coords = geometry.get("coordinates")
    if coords is None:
        return None

    min_lng = min_lat = float("inf")
    max_lng = max_lat = float("-inf")
    has_coords = False

    for lng, lat in flatten_coordinates(coords):
        if not isinstance(lng, (float, int)) or not isinstance(lat, (float, int)):
            continue
        has_coords = True
        if lng < min_lng:
            min_lng = lng
        if lng > max_lng:
            max_lng = lng
        if lat < min_lat:
            min_lat = lat
        if lat > max_lat:
            max_lat = lat

    if not has_coords:
        return None
    return (min_lng, min_lat, max_lng, max_lat)


def load_glow_details(
    glow_path: Path,
    *,
    centreline_metadata: dict[int, dict],
) -> dict[int, CentrelineDetail]:
    with glow_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    details: dict[int, CentrelineDetail] = {}
    for feature in payload.get("features", []):
        properties = feature.get("properties", {})
        centreline_id = properties.get("centreline_id")
        if centreline_id is None:
            continue
        try:
            cid = int(centreline_id)
        except (TypeError, ValueError):
            continue

        street = centreline_metadata.get(cid, {}).get("street_normalized") or "Unknown"
        bbox = compute_bbox(feature.get("geometry"))

        detail = CentrelineDetail(centreline_id=cid, street=str(street).upper())
        detail.ticket_count = int(properties.get("count", 0))
        detail.years.update(int(value) for value in properties.get("years", []) if isinstance(value, (int, float)))
        detail.months.update(int(value) for value in properties.get("months", []) if isinstance(value, (int, float)))
        detail.bbox = bbox
        details[cid] = detail
    return details


def aggregate_location_stats(
    aggregated_path: Path,
    geocode_lookup: dict[str, dict],
    centreline_details: dict[int, CentrelineDetail],
    *,
    location_limit: int,
) -> dict[str, StreetSummary]:
    with aggregated_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    street_summaries: dict[str, StreetSummary] = {}
    for feature in payload.get("features", []):
        properties = feature.get("properties", {})
        location = str(properties.get("location", "")).strip()
        if not location:
            continue

        geocode = geocode_lookup.get(location.upper())
        if not geocode:
            continue
        centreline_id = geocode.get("centreline_id")
        if not centreline_id:
            continue
        try:
            cid = int(centreline_id)
        except (TypeError, ValueError):
            continue

        detail = centreline_details.get(cid)
        if detail is None:
            continue

        # Update centreline detail
        count = int(properties.get("count", 0))
        revenue = float(properties.get("total_revenue", 0.0))
        detail.total_revenue += revenue
        detail.years.update(int(value) for value in properties.get("years", []) if isinstance(value, (int, float)))
        detail.months.update(int(value) for value in properties.get("months", []) if isinstance(value, (int, float)))

        location_entry = LocationEntry(
            location=location,
            ticket_count=count,
            total_revenue=revenue,
            top_infraction=str(properties.get("top_infraction")) if properties.get("top_infraction") else None,
        )
        detail.locations.append(location_entry)

        infraction_code = properties.get("top_infraction")
        infraction_count = int(properties.get("infraction_count", 0))
        if infraction_code:
            detail.infractions[str(infraction_code)] += max(infraction_count, count)

        street_name = detail.street
        summary = street_summaries.get(street_name)
        if summary is None:
            summary = StreetSummary(name=street_name)
            street_summaries[street_name] = summary

        summary.centreline_ids.add(cid)
        summary.ticket_count += count
        summary.total_revenue += revenue
        summary.years.update(int(value) for value in properties.get("years", []) if isinstance(value, (int, float)))
        summary.months.update(int(value) for value in properties.get("months", []) if isinstance(value, (int, float)))
        summary.locations.append(location_entry)
        if infraction_code:
            summary.infractions[str(infraction_code)] += max(infraction_count, count)
        summary.extend_bbox(detail.bbox)

    # Trim stored lists to reduce payload size
    for detail in centreline_details.values():
        if len(detail.locations) > location_limit:
            detail.locations = sorted(
                detail.locations,
                key=lambda entry: entry.ticket_count,
                reverse=True,
            )[:location_limit]

    for summary in street_summaries.values():
        if len(summary.locations) > location_limit:
            summary.locations = sorted(
                summary.locations,
                key=lambda entry: entry.ticket_count,
                reverse=True,
            )[:location_limit]

    return street_summaries


def write_lookup(output_path: Path, *, centreline_details: dict[int, CentrelineDetail], street_summaries: dict[str, StreetSummary]) -> None:
    payload = {
        "generatedAt": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "centreline": {
            str(cid): detail.to_dict()
            for cid, detail in sorted(
                centreline_details.items(), key=lambda item: item[1].ticket_count, reverse=True
            )
        },
        "streets": {
            street: summary.to_dict()
            for street, summary in sorted(
                street_summaries.items(), key=lambda item: item[1].ticket_count, reverse=True
            )
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote centreline lookup to {output_path}")


def main() -> None:
    args = parse_args()

    if not args.geocodes.exists():
        raise FileNotFoundError(f"Geocode file not found: {args.geocodes}")
    if not args.glow_lines.exists():
        raise FileNotFoundError(f"Glow dataset not found: {args.glow_lines}")
    if not args.aggregated.exists():
        raise FileNotFoundError(f"Aggregated dataset not found: {args.aggregated}")

    geocode_lookup = load_geocode_map(args.geocodes)
    metadata = build_centreline_metadata(geocode_lookup)
    centreline_details = load_glow_details(args.glow_lines, centreline_metadata=metadata)
    street_summaries = aggregate_location_stats(
        args.aggregated,
        geocode_lookup,
        centreline_details,
        location_limit=args.location_limit,
    )
    write_lookup(args.output, centreline_details=centreline_details, street_summaries=street_summaries)


if __name__ == "__main__":
    main()
