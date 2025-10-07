"""Build a centreline-based glow dataset for the map from existing aggregates."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional, Set

import sys


CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shapely import set_precision
from shapely.geometry import shape, mapping
from shapely.geometry.base import BaseGeometry

from geocoding.centreline_fetcher import CentrelineFetcher
from preprocessing.build_map_datasets import load_geocode_map


@dataclass
class CentrelineGlowAggregate:
    centreline_id: int
    count: int = 0
    years: Set[int] = field(default_factory=set)
    months: Set[int] = field(default_factory=set)

    def absorb(self, properties: dict) -> None:
        tickets = int(properties.get("count", 0))

        self.count += tickets
        self.years.update(properties.get("years", []))
        self.months.update(properties.get("months", []))

    def to_feature(self, geometry: BaseGeometry) -> dict:
        if geometry.is_empty:
            raise ValueError("Cannot build feature from empty geometry")

        return {
            "type": "Feature",
            "geometry": mapping(geometry),
            "properties": {
                "centreline_id": self.centreline_id,
                "count": self.count,
                "years": sorted(self.years),
                "months": sorted(self.months),
            },
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build centreline glow dataset")
    parser.add_argument(
        "--aggregated",
        type=Path,
        default=Path("map-app/public/data/tickets_aggregated.geojson"),
        help="Path to aggregated ticket GeoJSON",
    )
    parser.add_argument(
        "--geocodes",
        type=Path,
        default=Path("output/geocoded_addresses_combined.jsonl"),
        help="Path to merged geocode JSONL",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("map-app/public/data/tickets_glow_lines.geojson"),
        help="Destination for the glow line GeoJSON",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=5,
        help="Minimum ticket count required for a centreline segment to be included",
    )
    parser.add_argument(
        "--max-segments",
        type=int,
        default=20000,
        help="Maximum number of segments to keep (highest counts win)",
    )
    parser.add_argument(
        "--simplify",
        type=float,
        default=0.0003,
        help="Simplification tolerance for centreline geometries (degrees)",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=5,
        help="Decimal precision to snap coordinates to (use 0 to disable)",
    )
    return parser.parse_args()


def load_glow_aggregates(aggregated_path: Path, geocode_lookup: dict[str, dict]) -> dict[int, CentrelineGlowAggregate]:
    with aggregated_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    aggregates: dict[int, CentrelineGlowAggregate] = {}
    missing_centreline = 0
    for feature in payload.get("features", []):
        properties = feature.get("properties", {})
        location = str(properties.get("location", "")).strip()
        if not location:
            continue

        geocode = geocode_lookup.get(location.upper())
        if not geocode:
            missing_centreline += 1
            continue

        centreline_id = geocode.get("centreline_id")
        if not centreline_id:
            missing_centreline += 1
            continue

        aggregate = aggregates.get(centreline_id)
        if aggregate is None:
            aggregate = CentrelineGlowAggregate(centreline_id=centreline_id)
            aggregates[centreline_id] = aggregate

        aggregate.absorb(properties)

    print(f"Mapped {len(aggregates):,} centreline segments; {missing_centreline:,} locations lacked centreline IDs")
    return aggregates


def subset_centreline_dataframe(fetcher: CentrelineFetcher, centreline_ids: Iterable[int]) -> dict[int, dict]:
    df = fetcher.load_dataframe(force_refresh=False)
    filtered = df[df["CENTRELINE_ID"].isin(list(centreline_ids))]
    geometries: dict[int, dict] = {}
    for row in filtered[["CENTRELINE_ID", "geometry"]].itertuples(index=False):
        geometry = row.geometry
        if isinstance(geometry, dict):
            geometries[int(row.CENTRELINE_ID)] = geometry
    return geometries


def simplify_geometry(raw_geometry: dict, tolerance: float, *, precision: Optional[int]) -> Optional[BaseGeometry]:
    geom = shape(raw_geometry)
    if geom.is_empty:
        return None
    if tolerance > 0:
        simplified = geom.simplify(tolerance, preserve_topology=True)
        if not simplified.is_empty:
            geom = simplified
    if precision and precision > 0:
        grid_size = 10 ** (-precision)
        snapped = set_precision(geom, grid_size)
        if not snapped.is_empty:
            geom = snapped
    return geom


def build_features(
    aggregates: dict[int, CentrelineGlowAggregate],
    centreline_geometries: dict[int, dict],
    *,
    min_count: int,
    max_segments: int,
    simplify_tolerance: float,
    precision: Optional[int],
) -> list[dict]:
    # Keep highest ticket counts first to prioritise significant corridors
    ordered = sorted(aggregates.values(), key=lambda agg: agg.count, reverse=True)
    features: list[dict] = []

    for aggregate in ordered:
        if aggregate.count < min_count:
            continue
        geometry_payload = centreline_geometries.get(aggregate.centreline_id)
        if not geometry_payload:
            continue

        geometry = simplify_geometry(
            geometry_payload,
            simplify_tolerance,
            precision=precision,
        )
        if geometry is None or geometry.is_empty:
            continue

        try:
            feature = aggregate.to_feature(geometry)
        except ValueError:
            continue

        features.append(feature)
        if max_segments and len(features) >= max_segments:
            break

    print(f"Built {len(features):,} glow segments from {len(aggregates):,} aggregates")
    return features


def write_geojson(path: Path, features: list[dict]) -> None:
    payload = {"type": "FeatureCollection", "features": features}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))


def main() -> None:
    args = parse_args()

    if not args.aggregated.exists():
        raise FileNotFoundError(f"Aggregated dataset not found: {args.aggregated}")
    geocode_lookup = load_geocode_map(args.geocodes)

    aggregates = load_glow_aggregates(args.aggregated, geocode_lookup)
    if not aggregates:
        print("No centreline aggregates available; nothing to export")
        return

    fetcher = CentrelineFetcher()
    centreline_geometries = subset_centreline_dataframe(fetcher, aggregates.keys())
    if not centreline_geometries:
        raise RuntimeError("Failed to load centreline geometries")

    features = build_features(
        aggregates,
        centreline_geometries,
        min_count=args.min_count,
        max_segments=args.max_segments,
        simplify_tolerance=args.simplify,
        precision=args.precision,
    )
    if not features:
        print("No features generated; check filters")
        return

    write_geojson(args.output, features)
    print(f"Wrote glow dataset to {args.output}")


if __name__ == "__main__":
    main()
