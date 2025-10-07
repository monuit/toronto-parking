"""Utility helpers for building map datasets from parking ticket records."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


# MARK: Aggregation data models

@dataclass
class LocationAggregate:
    """Statistics accumulated for an individual geocoded location."""

    location: str
    latitude: float
    longitude: float
    source: str
    count: int = 0
    total_revenue: float = 0.0
    years: set[int] = field(default_factory=set)
    months: set[int] = field(default_factory=set)
    infractions: Dict[str, int] = field(default_factory=dict)
    neighbourhood: Optional[str] = None


@dataclass
class PlateAggregate:
    """Aggregated statistics for a masked licence plate."""

    ticket_count: int = 0
    total_revenue: float = 0.0
    infractions: Dict[str, int] = field(default_factory=dict)


# MARK: Normalization helpers

def normalize_address(value: str) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text.upper()


def normalize_infraction(value) -> str:
    if value is None:
        return "Unknown"
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return "Unknown"
    return text


def normalize_plate(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def parse_date_components(raw) -> Optional[tuple[int, int]]:
    text = str(raw).strip()
    if len(text) < 6:
        return None
    try:
        year = int(text[:4])
        month = int(text[4:6])
    except ValueError:
        return None
    if month < 1 or month > 12:
        return None
    return year, month


def to_float(value) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


# MARK: Aggregation helpers

def top_entry(infractions: Dict[str, int]) -> Optional[str]:
    if not infractions:
        return None
    return max(infractions.items(), key=lambda item: item[1])[0]


def build_tickets_geojson(location_stats: Dict[str, LocationAggregate]) -> dict:
    features = []
    for stats in location_stats.values():
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [stats.longitude, stats.latitude],
                },
                "properties": {
                    "location": stats.location,
                    "count": stats.count,
                    "total_revenue": round(stats.total_revenue, 2),
                    "years": sorted(stats.years),
                    "months": sorted(stats.months),
                    "top_infraction": top_entry(stats.infractions),
                    "infraction_count": len(stats.infractions),
                    "neighbourhood": stats.neighbourhood or "Unknown",
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def build_street_stats(location_stats: Dict[str, LocationAggregate], limit: int = 500) -> Dict[str, dict]:
    sorted_locations = sorted(
        location_stats.values(),
        key=lambda item: item.count,
        reverse=True,
    )[:limit]

    result: Dict[str, dict] = {}
    for stats in sorted_locations:
        result[stats.location] = {
            "ticketCount": stats.count,
            "totalRevenue": round(stats.total_revenue, 2),
            "topInfraction": top_entry(stats.infractions),
            "neighbourhood": stats.neighbourhood or "Unknown",
        }
    return result


def build_plate_stats(plate_stats: Dict[str, PlateAggregate], limit: int = 500) -> Dict[str, dict]:
    sorted_plates = sorted(
        plate_stats.items(),
        key=lambda item: item[1].ticket_count,
        reverse=True,
    )[:limit]

    result: Dict[str, dict] = {}
    for plate, stats in sorted_plates:
        result[plate] = {
            "ticketCount": stats.ticket_count,
            "totalRevenue": round(stats.total_revenue, 2),
            "topInfraction": top_entry(stats.infractions),
        }
    return result


def build_neighbourhood_stats(location_stats: Dict[str, LocationAggregate]) -> Dict[str, dict]:
    aggregate: Dict[str, dict] = {}
    for stats in location_stats.values():
        hood = stats.neighbourhood or "Unknown"
        entry = aggregate.get(hood)
        if entry is None:
            entry = {
                "count": 0,
                "totalFines": 0.0,
                "infractions": defaultdict(int),
            }
            aggregate[hood] = entry
        entry["count"] += stats.count
        entry["totalFines"] += stats.total_revenue
        for code, qty in stats.infractions.items():
            entry["infractions"][code] += qty

    result: Dict[str, dict] = {}
    for hood, info in aggregate.items():
        infractions = info["infractions"]
        result[hood] = {
            "count": info["count"],
            "totalFines": round(info["totalFines"], 2),
            "topInfraction": top_entry(infractions),
            "infractionVariety": len(infractions),
        }
    return result


# MARK: Output helpers

def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False)


def write_geojson(path: Path, payload: dict) -> None:
    write_json(path, payload)


__all__ = [
    "LocationAggregate",
    "PlateAggregate",
    "normalize_address",
    "normalize_infraction",
    "normalize_plate",
    "parse_date_components",
    "to_float",
    "top_entry",
    "build_tickets_geojson",
    "build_street_stats",
    "build_plate_stats",
    "build_neighbourhood_stats",
    "write_json",
    "write_geojson",
]
