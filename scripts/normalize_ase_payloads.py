"""Normalize ASE dataset payloads to yearly aggregates and aligned ward totals."""

from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "map-app" / "public" / "data"

ASE_LOCATIONS_PATH = DATA_DIR / "ase_locations.geojson"
ASE_SUMMARY_PATH = DATA_DIR / "ase_summary.json"
ASE_WARD_SUMMARY_PATH = DATA_DIR / "ase_ward_summary.json"
ASE_WARD_CHOROPLETH_PATH = DATA_DIR / "ase_ward_choropleth.geojson"

RLC_WARD_SUMMARY_PATH = DATA_DIR / "red_light_ward_summary.json"
CAMERAS_COMBINED_WARD_SUMMARY_PATH = DATA_DIR / "cameras_combined_ward_summary.json"
CAMERAS_COMBINED_WARD_CHOROPLETH_PATH = DATA_DIR / "cameras_combined_ward_choropleth.geojson"


def _extract_ward_code(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\d+", value)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def build_yearly_counts(monthly_counts: dict[str, int]) -> dict[str, int]:
    yearly: dict[str, int] = {}
    for key, raw_value in (monthly_counts or {}).items():
        if not isinstance(key, str):
            continue
        segments = key.split("-", 1)
        if not segments:
            continue
        year_segment = segments[0]
        if len(year_segment) != 4 or not year_segment.isdigit():
            continue
        value = int(raw_value) if isinstance(raw_value, (int, float)) else None
        if value is None or value < 0:
            continue
        yearly[year_segment] = yearly.get(year_segment, 0) + value
    return yearly


def normalise_ase_payloads() -> None:
    locations_fc = _load_json(ASE_LOCATIONS_PATH)
    features = locations_fc.get("features", [])

    total_ticket_count = 0
    total_revenue = 0.0
    status_counts: Counter[str] = Counter()
    ward_totals: dict[int, dict[str, float]] = defaultdict(
        lambda: {
            "ward_name": None,
            "ticket_count": 0,
            "location_count": 0,
            "total_revenue": 0.0,
        }
    )

    locations_by_id: dict[str, dict] = {}
    top_candidates: list[dict] = []

    for feature in features:
        properties = feature.get("properties") or {}

        location_id = (
            properties.get("locationCode")
            or properties.get("location_code")
            or properties.get("intersectionId")
            or properties.get("intersection_id")
        )
        if location_id is None:
            continue
        location_id = str(location_id)

        ticket_count = int(properties.get("ticketCount") or properties.get("ticket_count") or 0)
        revenue = float(properties.get("totalRevenue") or properties.get("total_revenue") or 0.0)
        ward_name = properties.get("ward") or "Unknown"
        status = (properties.get("status") or "Unknown").strip() or "Unknown"

        monthly_counts = properties.pop("monthlyCounts", {}) or properties.pop("monthly_counts", {}) or {}
        yearly_counts = build_yearly_counts(monthly_counts)
        if not yearly_counts and properties.get("yearlyCounts"):
            yearly_counts = {
                str(key): int(value)
                for key, value in properties.get("yearlyCounts", {}).items()
                if isinstance(key, (str, int))
            }
        properties["yearlyCounts"] = yearly_counts
        properties.pop("months", None)
        properties.pop("months_1", None)

        years = properties.get("years") or []
        normalized_years = sorted({int(year) for year in years if isinstance(year, (int, float, str)) and str(year).isdigit()})
        if not normalized_years and yearly_counts:
            normalized_years = sorted({int(year) for year in yearly_counts.keys() if year.isdigit()})
        properties["years"] = normalized_years

        coordinates = feature.get("geometry", {}).get("coordinates") or []
        longitude = float(coordinates[0]) if len(coordinates) >= 2 and isinstance(coordinates[0], (int, float)) else None
        latitude = float(coordinates[1]) if len(coordinates) >= 2 and isinstance(coordinates[1], (int, float)) else None
        if longitude is not None:
            properties["longitude"] = longitude
        if latitude is not None:
            properties["latitude"] = latitude

        feature["properties"] = properties

        total_ticket_count += ticket_count
        total_revenue += revenue
        status_counts[status] += 1

        ward_code = _extract_ward_code(ward_name)
        if ward_code is not None:
            ward_entry = ward_totals[ward_code]
            if not ward_entry["ward_name"]:
                ward_entry["ward_name"] = ward_name
            ward_entry["ticket_count"] += ticket_count
            ward_entry["location_count"] += 1
            ward_entry["total_revenue"] += revenue

        base_record = {
            "id": location_id,
            "name": properties.get("name") or properties.get("location") or location_id,
            "ward": ward_name,
            "status": status,
            "ticketCount": ticket_count,
            "totalRevenue": round(revenue, 2),
            "years": normalized_years,
            "yearlyCounts": yearly_counts,
            "longitude": longitude,
            "latitude": latitude,
        }

        locations_by_id[location_id] = base_record
        top_candidates.append(base_record)

    # Persist normalised locations file
    _save_json(ASE_LOCATIONS_PATH, locations_fc)

    summary_payload = _load_json(ASE_SUMMARY_PATH)
    summary_payload["totals"] = {
        "locationCount": len(locations_by_id),
        "ticketCount": total_ticket_count,
        "totalRevenue": round(total_revenue, 2),
        "ticketCountWithGeometry": total_ticket_count,
    }
    summary_payload["topLocations"] = sorted(
        top_candidates,
        key=lambda item: (item["ticketCount"], item["totalRevenue"]),
        reverse=True,
    )[:10]

    summary_payload["locationsById"] = locations_by_id
    summary_payload["statusBreakdown"] = [
        {"status": status, "count": count}
        for status, count in status_counts.most_common()
    ]

    summary_payload.setdefault("topGroups", {})["wards"] = [
        {
            "name": ward_data.get("ward_name") or f"Ward {ward_code}",
            "ticketCount": int(ward_data.get("ticket_count", 0)),
            "totalRevenue": round(ward_data.get("total_revenue", 0.0), 2),
        }
        for ward_code, ward_data in sorted(
            ward_totals.items(),
            key=lambda item: (item[1]["ticket_count"], item[1]["total_revenue"]),
            reverse=True,
        )[:10]
    ]

    summary_payload["meta"] = {
        "historicalLocationsWithoutGeometry": [],
        "historicalTicketCount": 0,
    }

    _save_json(ASE_SUMMARY_PATH, summary_payload)

    # Build ward-level summaries
    ordered_wards = []
    for ward_code, ward_data in ward_totals.items():
        ward_name = ward_data.get("ward_name") or f"Ward {ward_code}"
        ordered_wards.append(
            {
                "wardCode": ward_code,
                "wardName": ward_name,
                "ticketCount": int(ward_data.get("ticket_count", 0)),
                "locationCount": int(ward_data.get("location_count", 0)),
                "totalRevenue": round(ward_data.get("total_revenue", 0.0), 2),
            }
        )

    ordered_wards.sort(key=lambda item: (item["ticketCount"], item["totalRevenue"]), reverse=True)

    ward_summary = {
        "generatedAt": summary_payload.get("generatedAt") or summary_payload.get("updatedAt"),
        "totals": {
            "ticketCount": sum(item["ticketCount"] for item in ordered_wards),
            "locationCount": sum(item["locationCount"] for item in ordered_wards),
            "totalRevenue": round(sum(item["totalRevenue"] for item in ordered_wards), 2),
        },
        "topWards": ordered_wards[:10],
        "wards": ordered_wards,
    }
    _save_json(ASE_WARD_SUMMARY_PATH, ward_summary)

    ward_choropleth = _load_json(ASE_WARD_CHOROPLETH_PATH)
    features = ward_choropleth.get("features", [])
    for feature in features:
        properties = feature.get("properties") or {}
        ward_code = _extract_ward_code(str(properties.get("wardCode") or properties.get("WARDS")))
        aggregate = ward_totals.get(ward_code or -1, {})
        properties.update(
            {
                "wardCode": ward_code,
                "wardName": aggregate.get("ward_name") or properties.get("wardName") or f"Ward {ward_code}",
                "ticketCount": int(aggregate.get("ticket_count", 0)),
                "locationCount": int(aggregate.get("location_count", 0)),
                "totalRevenue": round(aggregate.get("total_revenue", 0.0), 2),
            }
        )
        feature["properties"] = properties
    _save_json(ASE_WARD_CHOROPLETH_PATH, ward_choropleth)

    # Update combined camera ward datasets to align with new ASE totals
    if RLC_WARD_SUMMARY_PATH.exists():
        red_light_summary = _load_json(RLC_WARD_SUMMARY_PATH)
        rlc_totals = {
            int(ward["wardCode"]): {
                "ward_name": ward.get("wardName"),
                "ticket_count": int(ward.get("ticketCount", 0)),
                "location_count": int(ward.get("locationCount", 0)),
                "total_revenue": float(ward.get("totalRevenue", 0.0)),
            }
            for ward in red_light_summary.get("wards", [])
        }

        combined_totals: dict[int, dict[str, float]] = {}
        for source, payload in (("ase", ward_totals), ("rlc", rlc_totals)):
            for code, stats in payload.items():
                bucket = combined_totals.setdefault(
                    code,
                    {
                        "ward_name": stats.get("ward_name") or f"Ward {code}",
                        "ticket_count": 0,
                        "location_count": 0,
                        "total_revenue": 0.0,
                        "ase_ticket_count": 0,
                        "rlc_ticket_count": 0,
                    },
                )
                bucket["ticket_count"] += stats.get("ticket_count", 0)
                bucket["location_count"] += stats.get("location_count", 0)
                bucket["total_revenue"] += stats.get("total_revenue", 0.0)
                if source == "ase":
                    bucket["ase_ticket_count"] += stats.get("ticket_count", 0)
                else:
                    bucket["rlc_ticket_count"] += stats.get("ticket_count", 0)

        combined_rows = []
        for code, stats in combined_totals.items():
            combined_rows.append(
                {
                    "wardCode": code,
                    "wardName": stats.get("ward_name") or f"Ward {code}",
                    "ticketCount": int(stats.get("ticket_count", 0)),
                    "locationCount": int(stats.get("location_count", 0)),
                    "totalRevenue": round(stats.get("total_revenue", 0.0), 2),
                    "aseTicketCount": int(stats.get("ase_ticket_count", 0)),
                    "rlcTicketCount": int(stats.get("rlc_ticket_count", 0)),
                }
            )

        combined_rows.sort(key=lambda item: (item["ticketCount"], item["totalRevenue"]), reverse=True)

        combined_summary = {
            "generatedAt": summary_payload.get("generatedAt"),
            "totals": {
                "ticketCount": sum(item["ticketCount"] for item in combined_rows),
                "locationCount": sum(item["locationCount"] for item in combined_rows),
                "totalRevenue": round(sum(item["totalRevenue"] for item in combined_rows), 2),
            },
            "topWards": combined_rows[:10],
            "wards": combined_rows,
        }
        _save_json(CAMERAS_COMBINED_WARD_SUMMARY_PATH, combined_summary)

        combined_choropleth = _load_json(CAMERAS_COMBINED_WARD_CHOROPLETH_PATH)
        for feature in combined_choropleth.get("features", []):
            properties = feature.get("properties") or {}
            ward_code = _extract_ward_code(str(properties.get("wardCode")))
            stats = combined_totals.get(ward_code or -1)
            if stats:
                properties.update(
                    {
                        "wardCode": ward_code,
                        "wardName": stats.get("ward_name") or properties.get("wardName") or f"Ward {ward_code}",
                        "ticketCount": int(stats.get("ticket_count", 0)),
                        "locationCount": int(stats.get("location_count", 0)),
                        "totalRevenue": round(stats.get("total_revenue", 0.0), 2),
                        "aseTicketCount": int(stats.get("ase_ticket_count", 0)),
                        "rlcTicketCount": int(stats.get("rlc_ticket_count", 0)),
                    }
                )
            feature["properties"] = properties
        _save_json(CAMERAS_COMBINED_WARD_CHOROPLETH_PATH, combined_choropleth)


if __name__ == "__main__":
    normalise_ase_payloads()
