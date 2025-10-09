"""Build summary datasets for red light and ASE camera locations."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, date
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List
import sys

import dotenv
import psycopg
from psycopg.rows import dict_row

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.etl.datasets.ase_locations import _load_charges_summary as load_ase_charges_summary  # noqa: E402
from src.etl.datasets.red_light_locations import _load_charges_summary as load_rlc_charges_summary  # noqa: E402


@dataclass
class DatasetSummary:
    dataset: str
    features: List[dict]
    summary: Dict[str, Any]
    glow_features: List[dict]

    def to_geojson(self) -> Dict[str, Any]:
        return {
            "type": "FeatureCollection",
            "features": self.features,
        }

    def glow_geojson(self) -> Dict[str, Any]:
        return {
            "type": "FeatureCollection",
            "features": self.glow_features,
        }


def _resolve_dsn(provided: str | None = None) -> str:
    if not os.getenv("DATABASE_URL") and not os.getenv("POSTGRES_URL") and not os.getenv("POSTGIS_DATABASE_URL"):
        repo_root = Path(__file__).resolve().parents[1]
        dotenv.load_dotenv(repo_root / ".env")

    if provided:
        return provided
    for key in ("DATABASE_URL", "POSTGRES_URL", "POSTGIS_DATABASE_URL"):
        value = os.getenv(key)
        if value:
            return value
    raise RuntimeError(
        "Database URL is required via --database-url or one of DATABASE_URL/POSTGRES_URL/POSTGIS_DATABASE_URL"
    )


def _fetch_rows(conn: psycopg.Connection, query: str) -> List[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query)
        return list(cur.fetchall())


def _locate_latest_ase_charges() -> Path | None:
    raw_root = REPO_ROOT / "output" / "etl" / "raw" / "ase_locations"
    if not raw_root.exists():
        return None
    candidates = sorted(raw_root.glob("*.xlsx"), key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _locate_latest_red_light_charges() -> Path | None:
    raw_root = REPO_ROOT / "output" / "etl" / "raw" / "red_light_locations"
    if not raw_root.exists():
        return None
    candidates = sorted(raw_root.glob("*.xlsx"), key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _safe_number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalise_ward_name(value: Any) -> str:
    if value is None:
        return "Unknown"
    try:
        number = float(value)
        if math.isfinite(number):
            return f"Ward {int(round(number))}"
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text if text else "Unknown"


def _format_date(value: Any) -> str | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _build_glow_segments(
    longitude: float,
    latitude: float,
    intensity: float,
    identifier: str | None,
    props: Dict[str, Any],
) -> List[dict]:
    if not (math.isfinite(longitude) and math.isfinite(latitude)):
        return []

    intensity = max(float(intensity or 0), 0.0)
    if intensity <= 0:
        return []

    # Convert metres to degrees (approximate). Base length scaled by intensity.
    base_metres = 90 + min(intensity, 2500) * 0.012  # between ~90m and ~120m
    metres_per_degree_lat = 111_320
    metres_per_degree_lng = math.cos(math.radians(latitude)) * metres_per_degree_lat
    if metres_per_degree_lng == 0:
        metres_per_degree_lng = metres_per_degree_lat

    delta_lat = (base_metres / metres_per_degree_lat) * 0.5
    delta_lng = (base_metres / metres_per_degree_lng) * 0.5

    segments: List[dict] = []
    for angle in (0, 45, 90, 135):
        radians_value = math.radians(angle)
        offset_x = delta_lng * math.cos(radians_value)
        offset_y = delta_lat * math.sin(radians_value)
        start = [longitude - offset_x, latitude - offset_y]
        end = [longitude + offset_x, latitude + offset_y]
        segment_props = {
            "count": round(intensity, 2),
            "locationId": identifier,
        }
        segment_props.update(props)
        segments.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [start, end],
                },
                "properties": segment_props,
            }
        )

    return segments


def build_red_light_summary(conn: psycopg.Connection) -> DatasetSummary:
    charges_path = _locate_latest_red_light_charges()
    charges_lookup: Dict[str, Dict[str, Any]] = {}
    total_tickets_all = 0
    total_revenue_all = 0.0

    if charges_path is not None:
        charges_lookup = load_rlc_charges_summary(charges_path)
        total_tickets_all = sum(int(entry.get("ticket_count", 0) or 0) for entry in charges_lookup.values())
        total_revenue_all = sum(_safe_number(entry.get("total_fine_amount")) for entry in charges_lookup.values())

    rows = _fetch_rows(
        conn,
        """
        SELECT
            intersection_id,
            location_code,
            location_name,
            linear_name_full_1,
            linear_name_full_2,
            ward_1,
            police_division_1,
            activation_date,
            ticket_count,
            total_fine_amount,
            years,
            months,
            yearly_counts,
            ST_X(geom)::DOUBLE PRECISION AS longitude,
            ST_Y(geom)::DOUBLE PRECISION AS latitude
        FROM red_light_camera_locations
        WHERE geom IS NOT NULL
        """,
    )

    features: List[dict] = []
    glow_features: List[dict] = []
    top_locations: List[dict] = []
    ward_totals: dict[str, dict[str, float]] = defaultdict(lambda: {"ticketCount": 0, "totalRevenue": 0.0})
    division_totals: dict[str, dict[str, float]] = defaultdict(lambda: {"ticketCount": 0, "totalRevenue": 0.0})
    locations_by_id: Dict[str, Dict[str, Any]] = {}

    total_tickets_with_geometry = 0
    total_revenue_with_geometry = 0.0

    unmatched_lookup = dict(charges_lookup)

    for row in rows:
        location_code = str(row.get("location_code") or "").strip()
        metrics = unmatched_lookup.pop(location_code, None)

        ticket_count = int(metrics.get("ticket_count", 0)) if metrics else int(row.get("ticket_count") or 0)
        total_fine_amount = _safe_number(metrics.get("total_fine_amount")) if metrics else _safe_number(row.get("total_fine_amount"))

        total_tickets_with_geometry += ticket_count
        total_revenue_with_geometry += total_fine_amount

        ward_source = metrics.get("ward") if metrics else row.get("ward_1")
        ward = _normalise_ward_name(ward_source)
        division = (row.get("police_division_1") or "Unknown").strip() or "Unknown"
        ward_totals[ward]["ticketCount"] += ticket_count
        ward_totals[ward]["totalRevenue"] += total_fine_amount
        division_totals[division]["ticketCount"] += ticket_count
        division_totals[division]["totalRevenue"] += total_fine_amount

        location_name = metrics.get("location_name") if metrics else row.get("location_name")
        streets = [
            value.strip()
            for value in (row.get("linear_name_full_1"), row.get("linear_name_full_2"))
            if value and value.strip()
        ]
        display_name = (location_name or " & ".join(streets) or str(row.get("intersection_id") or "Unknown")).strip()

        longitude = _safe_number(row.get("longitude"))
        latitude = _safe_number(row.get("latitude"))

        years = metrics.get("years") if metrics else (row.get("years") or [])
        months = metrics.get("months") if metrics else (row.get("months") or [])
        yearly_counts = metrics.get("yearly_counts") if metrics else (row.get("yearly_counts") or {})

        feature_properties = {
            "intersectionId": row.get("intersection_id"),
            "locationCode": location_code or None,
            "name": display_name,
            "streetA": row.get("linear_name_full_1"),
            "streetB": row.get("linear_name_full_2"),
            "ward": ward,
            "policeDivision": division,
            "activationDate": _format_date(row.get("activation_date")),
            "ticketCount": ticket_count,
            "totalRevenue": round(total_fine_amount, 2),
            "years": years or [],
            "months": months or [],
            "yearlyCounts": yearly_counts or {},
        }

        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                "properties": feature_properties,
            }
        )

        glow_features.extend(
            _build_glow_segments(
                longitude,
                latitude,
                ticket_count,
                row.get("intersection_id"),
                {
                    "ward": ward,
                    "policeDivision": division,
                },
            )
        )

        locations_by_id[str(row.get("intersection_id"))] = {
            "id": row.get("intersection_id"),
            "locationCode": location_code or None,
            "name": display_name,
            "longitude": longitude,
            "latitude": latitude,
            "ticketCount": ticket_count,
            "totalRevenue": round(total_fine_amount, 2),
            "ward": ward,
            "policeDivision": division,
            "activationDate": _format_date(row.get("activation_date")),
            "years": feature_properties["years"],
            "months": feature_properties["months"],
            "yearlyCounts": feature_properties["yearlyCounts"],
        }

        top_locations.append(
            {
                "id": row.get("intersection_id"),
                "locationCode": location_code or None,
                "name": display_name,
                "ticketCount": ticket_count,
                "totalRevenue": round(total_fine_amount, 2),
                "ward": ward,
                "policeDivision": division,
                "activationDate": _format_date(row.get("activation_date")),
                "streetA": row.get("linear_name_full_1"),
                "streetB": row.get("linear_name_full_2"),
                "years": feature_properties["years"],
                "months": feature_properties["months"],
                "yearlyCounts": feature_properties["yearlyCounts"],
                "longitude": longitude,
                "latitude": latitude,
            }
        )

    top_locations.sort(key=lambda item: (item["ticketCount"], item["totalRevenue"]), reverse=True)

    historical_ticket_total = 0
    historical_revenue_total = 0.0
    historical_codes: List[str] = []

    for code, metrics in unmatched_lookup.items():
        tickets = int(metrics.get("ticket_count", 0) or 0)
        revenue = _safe_number(metrics.get("total_fine_amount"))
        ward_name = _normalise_ward_name(metrics.get("ward"))

        historical_ticket_total += tickets
        historical_revenue_total += revenue
        ward_totals[ward_name]["ticketCount"] += tickets
        ward_totals[ward_name]["totalRevenue"] += revenue
        historical_codes.append(str(code))

    if charges_lookup:
        total_tickets = total_tickets_all
        total_revenue = total_revenue_all
    else:
        total_tickets = total_tickets_with_geometry + historical_ticket_total
        total_revenue = total_revenue_with_geometry + historical_revenue_total

    def _top_groups(groups: dict[str, dict[str, float]]) -> List[dict]:
        ranked = sorted(
            groups.items(),
            key=lambda item: (item[1]["ticketCount"], item[1]["totalRevenue"]),
            reverse=True,
        )[:10]
        results: List[dict] = []
        for name, metrics in ranked:
            results.append({
                "name": name,
                "ticketCount": int(metrics["ticketCount"]),
                "totalRevenue": round(metrics["totalRevenue"], 2),
            })
        return results

    summary = {
        "dataset": "red_light_locations",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "locationCount": len(rows),
            "ticketCount": total_tickets,
            "totalRevenue": round(total_revenue, 2),
            "ticketCountWithGeometry": total_tickets_with_geometry,
        },
        "topLocations": top_locations[:10],
        "topGroups": {
            "wards": _top_groups(ward_totals),
            "policeDivisions": _top_groups(division_totals),
        },
        "locationsById": locations_by_id,
        "meta": {
            "historicalLocationCodes": sorted(historical_codes),
            "historicalLocationCount": len(historical_codes),
            "historicalTicketCount": historical_ticket_total,
        },
    }

    return DatasetSummary(dataset="red_light_locations", features=features, summary=summary, glow_features=glow_features)


def build_ase_summary(conn: psycopg.Connection) -> DatasetSummary:
    charges_path = _locate_latest_ase_charges()
    charges_summary_lookup: Dict[str, Dict[str, Any]] = {}
    total_tickets_all = 0
    total_revenue_all = 0.0

    if charges_path is not None:
        charges_summary_lookup = load_ase_charges_summary(charges_path)
        total_tickets_all = sum(int(entry.get("ticket_count", 0) or 0) for entry in charges_summary_lookup.values())
        total_revenue_all = sum(_safe_number(entry.get("total_fine_amount")) for entry in charges_summary_lookup.values())

    rows = _fetch_rows(
        conn,
        """
        SELECT
            location_code,
            ward,
            status,
            location,
            ticket_count,
            total_fine_amount,
            years,
            months,
            monthly_counts,
            ST_X(geom)::DOUBLE PRECISION AS longitude,
            ST_Y(geom)::DOUBLE PRECISION AS latitude
        FROM ase_camera_locations
        WHERE geom IS NOT NULL
        """,
    )

    features: List[dict] = []
    glow_features: List[dict] = []
    top_locations: List[dict] = []
    ward_totals: dict[str, dict[str, float]] = defaultdict(lambda: {"ticketCount": 0, "totalRevenue": 0.0})
    status_counter: Counter[str] = Counter()
    locations_by_id: Dict[str, Dict[str, Any]] = {}

    total_tickets_with_geometry = 0
    total_revenue_with_geometry = 0.0

    unmatched_lookup = dict(charges_summary_lookup)

    for row in rows:
        location_code = str(row.get("location_code") or "").strip()
        metrics = unmatched_lookup.pop(location_code, None)

        ticket_count = int(metrics.get("ticket_count", 0)) if metrics else int(row.get("ticket_count") or 0)
        total_fine_amount = _safe_number(metrics.get("total_fine_amount")) if metrics else _safe_number(row.get("total_fine_amount"))

        total_tickets_with_geometry += ticket_count
        total_revenue_with_geometry += total_fine_amount

        ward_value = row.get("ward") if row.get("ward") else (metrics.get("ward") if metrics else None)
        ward = _normalise_ward_name(ward_value)
        ward_totals[ward]["ticketCount"] += ticket_count
        ward_totals[ward]["totalRevenue"] += total_fine_amount

        status = (row.get("status") or "Unknown").strip() or "Unknown"
        status_counter[status] += 1

        name = (row.get("location") or str(row.get("location_code") or "Unknown")).strip()
        longitude = _safe_number(row.get("longitude"))
        latitude = _safe_number(row.get("latitude"))

        years = row.get("years") or (metrics.get("years") if metrics else []) or []
        yearly_counts = row.get("yearly_counts") or (metrics.get("yearly_counts") if metrics else {}) or {}

        feature_properties = {
            "locationCode": row.get("location_code"),
            "name": name,
            "ward": ward,
            "status": status,
            "ticketCount": ticket_count,
            "totalRevenue": round(total_fine_amount, 2),
            "years": years,
            "yearlyCounts": yearly_counts,
        }

        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                "properties": feature_properties,
            }
        )

        glow_features.extend(
            _build_glow_segments(
                longitude,
                latitude,
                ticket_count,
                row.get("location_code"),
                {
                    "ward": ward,
                    "status": status,
                },
            )
        )

        locations_by_id[str(row.get("location_code"))] = {
            "id": row.get("location_code"),
            "name": name,
            "longitude": longitude,
            "latitude": latitude,
            "ticketCount": ticket_count,
            "totalRevenue": round(total_fine_amount, 2),
            "ward": ward,
            "status": status,
            "years": feature_properties["years"],
            "yearlyCounts": feature_properties["yearlyCounts"],
        }

        top_locations.append(
            {
                "id": row.get("location_code"),
                "name": name,
                "ward": ward,
                "status": status,
                "ticketCount": ticket_count,
                "totalRevenue": round(total_fine_amount, 2),
                "years": feature_properties["years"],
                "yearlyCounts": feature_properties["yearlyCounts"],
                "longitude": longitude,
                "latitude": latitude,
            }
        )

    top_locations.sort(key=lambda item: (item["ticketCount"], item["totalRevenue"]), reverse=True)

    unmatched_ticket_total = 0
    unmatched_revenue_total = 0.0
    historical_location_codes: List[str] = []

    for code, metrics in unmatched_lookup.items():
        tickets = int(metrics.get("ticket_count", 0) or 0)
        revenue = _safe_number(metrics.get("total_fine_amount"))
        unmatched_ticket_total += tickets
        unmatched_revenue_total += revenue
        historical_location_codes.append(code)

    total_tickets = total_tickets_with_geometry
    total_revenue = total_revenue_with_geometry

    summary = {
        "dataset": "ase_locations",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "locationCount": len(rows),
            "ticketCount": total_tickets,
            "totalRevenue": round(total_revenue, 2),
            "ticketCountWithGeometry": total_tickets_with_geometry,
        },
        "topLocations": top_locations[:10],
        "topGroups": {
            "wards": [
                {
                  "name": name,
                  "ticketCount": int(metrics["ticketCount"]),
                  "totalRevenue": round(metrics["totalRevenue"], 2),
                }
                for name, metrics in sorted(
                  ward_totals.items(),
                  key=lambda item: (item[1]["ticketCount"], item[1]["totalRevenue"]),
                  reverse=True,
                )[:10]
            ],
        },
        "statusBreakdown": [
            {"status": name, "count": count}
            for name, count in status_counter.most_common()
        ],
        "locationsById": locations_by_id,
        "meta": {
            "historicalLocationsWithoutGeometry": sorted(historical_location_codes),
            "historicalTicketCount": unmatched_ticket_total,
        },
    }

    return DatasetSummary(dataset="ase_locations", features=features, summary=summary, glow_features=glow_features)


def write_output(output_dir: Path, filename: str, payload: Dict[str, Any]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / filename
    target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return target


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build red light and ASE dataset summaries")
    parser.add_argument(
        "--output-dir",
        default=Path("map-app/public/data"),
        type=Path,
        help="Directory for generated files (default: map-app/public/data)",
    )
    parser.add_argument(
        "--database-url",
        help="Postgres connection string (defaults to DATABASE_URL/POSTGRES_URL)",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    dsn = _resolve_dsn(args.database_url)
    output_dir = args.output_dir.resolve()

    print("=== Building camera datasets ===")
    print(f"Output directory: {output_dir}")

    with psycopg.connect(dsn) as conn:
        red_light = build_red_light_summary(conn)
        ase = build_ase_summary(conn)

    files = []
    files.append(write_output(output_dir, "red_light_locations.geojson", red_light.to_geojson()))
    files.append(write_output(output_dir, "red_light_summary.json", red_light.summary))
    files.append(write_output(output_dir, "red_light_glow_lines.geojson", red_light.glow_geojson()))
    files.append(write_output(output_dir, "ase_locations.geojson", ase.to_geojson()))
    files.append(write_output(output_dir, "ase_summary.json", ase.summary))
    files.append(write_output(output_dir, "ase_glow_lines.geojson", ase.glow_geojson()))

    print(f"Red light cameras: {red_light.summary['totals']['locationCount']:,} sites")
    print(f"ASE cameras: {ase.summary['totals']['locationCount']:,} sites")
    print("Wrote:")
    for file in files:
        print(f"  • {file.relative_to(Path.cwd())}")


if __name__ == "__main__":
    main()
