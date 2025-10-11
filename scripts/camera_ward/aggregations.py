"""Aggregation helpers for ward-level camera datasets."""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from .constants import SUMMARY_PATHS

WARD_CODE_PATTERN = re.compile(r"(\d+(?:\.\d+)?)")


def safe_number(value: Any) -> float:
    """Best-effort conversion of arbitrary input into a float."""

    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalise_ward_name(value: Any) -> str:
    """Normalise a ward identifier into a canonical name."""

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


def normalise_ward_code(value: Optional[object]) -> Optional[int]:
    """Extract a ward code from the provided raw value."""

    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = int(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        match = WARD_CODE_PATTERN.search(text)
        if not match:
            return None
        token = match.group(1)
        try:
            number = int(token)
        except ValueError:
            try:
                number = int(float(token))
            except ValueError:
                return None
    while number > 25 and number % 10 == 0:
        number //= 10
    return number if number > 0 else None


def aggregate_charges(charges_lookup: Dict[str, Dict[str, Any]]) -> Dict[int, Dict[str, float]]:
    """Aggregate per-location charge records into ward-level totals."""

    totals: Dict[int, Dict[str, Any]] = defaultdict(
        lambda: {
            "ticket_count": 0,
            "total_revenue": 0.0,
            "locations": set(),
            "ward_names": set(),
        }
    )

    for location_code, metrics in charges_lookup.items():
        ward_raw = metrics.get("ward") or metrics.get("Ward")
        ward_code = normalise_ward_code(ward_raw)
        if ward_code is None:
            continue
        ward_name = _normalise_ward_name(ward_raw)
        ticket_count = int(metrics.get("ticket_count") or 0)
        total_revenue = safe_number(metrics.get("total_fine_amount"))

        bucket = totals[ward_code]
        bucket["ticket_count"] += ticket_count
        bucket["total_revenue"] += total_revenue
        bucket["locations"].add(location_code)
        bucket["ward_names"].add(ward_name)

    result: Dict[int, Dict[str, float]] = {}
    for ward_code, bucket in totals.items():
        ward_names = bucket.get("ward_names") or {f"Ward {ward_code}"}
        result[ward_code] = {
            "ward_name": sorted(ward_names)[0],
            "ticket_count": int(bucket.get("ticket_count", 0)),
            "total_revenue": round(safe_number(bucket.get("total_revenue")), 2),
            "location_count": len(bucket.get("locations", set())),
        }

    return result


def merge_ward_totals(
    ase_totals: Dict[int, Dict[str, Any]],
    rlc_totals: Dict[int, Dict[str, Any]],
) -> Dict[int, Dict[str, float]]:
    """Combine ASE and red-light totals into a single per-ward mapping."""

    combined: Dict[int, Dict[str, float]] = {}
    for ward_code in set(ase_totals.keys()) | set(rlc_totals.keys()):
        ase = ase_totals.get(ward_code, {})
        rlc = rlc_totals.get(ward_code, {})
        ward_name = next(
            (name for name in [ase.get("ward_name"), rlc.get("ward_name")] if name),
            f"Ward {ward_code}",
        )
        combined[ward_code] = {
            "ward_name": ward_name,
            "ticket_count": int(ase.get("ticket_count", 0)) + int(rlc.get("ticket_count", 0)),
            "total_revenue": round(
                safe_number(ase.get("total_revenue")) + safe_number(rlc.get("total_revenue")),
                2,
            ),
            "location_count": int(ase.get("location_count", 0)) + int(rlc.get("location_count", 0)),
            "ase_ticket_count": int(ase.get("ticket_count", 0)),
            "rlc_ticket_count": int(rlc.get("ticket_count", 0)),
        }
    return combined


def build_summary(totals: Dict[int, Dict[str, float]]) -> dict:
    """Construct the JSON summary payload consumed by the map app."""

    ordered = sorted(
        (
            {
                "wardCode": ward_code,
                "wardName": bucket.get("ward_name") or f"Ward {ward_code}",
                "ticketCount": int(bucket.get("ticket_count", 0)),
                "locationCount": int(bucket.get("location_count", 0)),
                "totalRevenue": round(safe_number(bucket.get("total_revenue")), 2),
            }
            for ward_code, bucket in totals.items()
        ),
        key=lambda item: (item["ticketCount"], item["totalRevenue"]),
        reverse=True,
    )

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "ticketCount": sum(item["ticketCount"] for item in ordered),
            "locationCount": sum(item["locationCount"] for item in ordered),
            "totalRevenue": round(sum(item["totalRevenue"] for item in ordered), 2),
        },
        "topWards": ordered[:10],
        "wards": ordered,
    }


def load_totals_from_summary(
    dataset: str,
) -> Tuple[Optional[dict], Optional[Dict[int, Dict[str, float]]]]:
    """Load an existing summary file and reconstruct ward totals."""

    summary_path = SUMMARY_PATHS.get(dataset)
    if not summary_path or not summary_path.exists():
        return None, None
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None, None

    totals: Dict[int, Dict[str, float]] = {}
    for ward in summary.get("wards", []):
        ward_code = normalise_ward_code(ward.get("wardCode"))
        if ward_code is None:
            continue
        totals[ward_code] = {
            "ward_name": ward.get("wardName") or f"Ward {ward_code}",
            "ticket_count": int(ward.get("ticketCount", 0)),
            "location_count": int(ward.get("locationCount", 0)),
            "total_revenue": round(safe_number(ward.get("totalRevenue", 0)), 2),
        }
    return summary, totals if totals else None