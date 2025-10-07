"""Centreline-based address geocoder.

Single Responsibility: map Toronto street addresses to centreline geometry.
"""

from __future__ import annotations

import difflib
import math
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

# MARK: Data structures


@dataclass(frozen=True)
class GeocodeResult:
    """Represents a successful geocode resolution."""

    street_normalized: str
    latitude: float
    longitude: float
    centreline_id: int
    feature_code: Optional[int]
    feature_code_desc: Optional[str]
    jurisdiction: Optional[str]


@dataclass(frozen=True)
class CentrelineSegment:
    """Simplified view of a centreline record."""

    centreline_id: int
    street_key: str
    street_label: str
    parity_left: str
    parity_right: str
    low_even: Optional[int]
    high_even: Optional[int]
    low_odd: Optional[int]
    high_odd: Optional[int]
    feature_code: Optional[int]
    feature_code_desc: Optional[str]
    jurisdiction: Optional[str]
    centroid: Optional[Tuple[float, float]]

    def contains_address(self, number: int, is_even: bool) -> bool:
        """Return True when the address number fits this segment."""

        if is_even:
            if self.low_even is None or self.high_even is None:
                return False
            return self.low_even <= number <= self.high_even

        if self.low_odd is None or self.high_odd is None:
            return False
        return self.low_odd <= number <= self.high_odd


# MARK: Normalization utilities

_ABBREVIATIONS = {
    "ST": "STREET",
    "STREET": "STREET",
    "RD": "ROAD",
    "ROAD": "ROAD",
    "AVE": "AVENUE",
    "AV": "AVENUE",
    "AVENUE": "AVENUE",
    "BLVD": "BOULEVARD",
    "BL": "BOULEVARD",
    "DR": "DRIVE",
    "PKWY": "PARKWAY",
    "HWY": "HIGHWAY",
    "CRES": "CRESCENT",
    "CR": "CRESCENT",
    "CIR": "CIRCLE",
    "CRT": "COURT",
    "CT": "COURT",
    "PL": "PLACE",
    "PLZ": "PLAZA",
    "SQ": "SQUARE",
    "TER": "TERRACE",
    "TRL": "TRAIL",
    "WAY": "WAY",
    "LANE": "LANE",
    "LN": "LANE",
    "PK": "PARK",
    "PARK": "PARK",
    "MALL": "MALL",
    "EXPY": "EXPRESSWAY",
}

_DIRECTIONS = {
    "E": "EAST",
    "W": "WEST",
    "N": "NORTH",
    "S": "SOUTH",
    "NE": "NORTHEAST",
    "NW": "NORTHWEST",
    "SE": "SOUTHEAST",
    "SW": "SOUTHWEST",
}

_TOKEN_PATTERN = re.compile(r"[^A-Z0-9]+")
_NUMBER_PATTERN = re.compile(r"^(\d+)([A-Z]?)$")


def normalize_text(value: str) -> str:
    """Upper-case alphanumeric text with collapsed whitespace."""

    cleaned = _TOKEN_PATTERN.sub(" ", value.upper())
    return re.sub(r"\s+", " ", cleaned).strip()


def normalize_street_name(raw_name: str) -> str:
    """Normalize a street name with abbreviations expanded."""

    tokens = normalize_text(raw_name).split(" ")
    normalized_tokens: List[str] = []

    for token in tokens:
        if token in _ABBREVIATIONS:
            normalized_tokens.append(_ABBREVIATIONS[token])
            continue
        if token in _DIRECTIONS:
            normalized_tokens.append(_DIRECTIONS[token])
            continue
        normalized_tokens.append(token)

    return " ".join(normalized_tokens)


# MARK: Geocoder core


class CentrelineGeocoder:
    """Geocode addresses against the Toronto Centreline dataset."""

    def __init__(self, centreline_df: pd.DataFrame):
        self._segments_by_key = self._build_index(centreline_df)
        self._street_keys = list(self._segments_by_key.keys())
        self._fuzzy_cache: Dict[str, Optional[str]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def geocode(self, address: str) -> Optional[GeocodeResult]:
        """Return the best geocode result for the provided address."""

        parsed = self._parse_address(address)
        if parsed is not None:
            number, street_key, is_even = parsed
            candidates = self._lookup_segments(street_key)

            if not candidates:
                return None

            matched = self._choose_best_segment(candidates, number, is_even)
            if matched is None or matched.centroid is None:
                matched = self._choose_best_segment(candidates, number, is_even, allow_out_of_range=True)
                if matched is None or matched.centroid is None:
                    return None

            lat, lon = matched.centroid
            return GeocodeResult(
                street_normalized=matched.street_key,
                latitude=lat,
                longitude=lon,
                centreline_id=matched.centreline_id,
                feature_code=matched.feature_code,
                feature_code_desc=matched.feature_code_desc,
                jurisdiction=matched.jurisdiction,
            )

        intersection = self._parse_intersection(address)
        if intersection is not None:
            return self._geocode_intersection(*intersection)

        street_only_key = self._parse_street_only(address)
        if street_only_key is not None:
            return self._geocode_street_only(street_only_key)

        return None

    def batch_geocode(self, addresses: Iterable[str]) -> List[Optional[GeocodeResult]]:
        """Geocode multiple addresses in order."""

        return [self.geocode(address) if isinstance(address, str) else None for address in addresses]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _build_index(self, df: pd.DataFrame) -> Dict[str, List[CentrelineSegment]]:
        index: Dict[str, List[CentrelineSegment]] = {}

        for _, row in df.iterrows():
            street_label = self._compose_label(row)
            street_key = normalize_street_name(street_label)
            centroid = self._compute_centroid(row.get("geometry"))

            segment = CentrelineSegment(
                centreline_id=int(row.get("CENTRELINE_ID", 0)),
                street_key=street_key,
                street_label=street_label,
                parity_left=self._safe_upper(row.get("PARITY_L")),
                parity_right=self._safe_upper(row.get("PARITY_R")),
                low_even=self._safe_int(row.get("LOW_NUM_EVEN")),
                high_even=self._safe_int(row.get("HIGH_NUM_EVEN")),
                low_odd=self._safe_int(row.get("LOW_NUM_ODD")),
                high_odd=self._safe_int(row.get("HIGH_NUM_ODD")),
                feature_code=self._safe_int(row.get("FEATURE_CODE")),
                feature_code_desc=row.get("FEATURE_CODE_DESC"),
                jurisdiction=self._safe_str(row.get("JURISDICTION")),
                centroid=centroid,
            )

            index.setdefault(street_key, []).append(segment)

        return index

    def _parse_address(self, address: str) -> Optional[Tuple[int, str, bool]]:
        if not address or not isinstance(address, str):
            return None

        normalized = normalize_text(address)

        tokens = normalized.split(" ")
        if not tokens:
            return None

        number_token = tokens[0]
        number_match = _NUMBER_PATTERN.match(number_token)
        if not number_match:
            return None

        number = int(number_match.group(1))
        postfix = number_match.group(2)
        is_even = number % 2 == 0

        street_tokens = tokens[1:]

        # Handle cases where additional numeric tokens precede the street name
        while street_tokens:
            next_token = street_tokens[0]
            next_match = _NUMBER_PATTERN.match(next_token)
            if next_match is None:
                break

            number = int(next_match.group(1))
            postfix = next_match.group(2)
            is_even = number % 2 == 0
            street_tokens = street_tokens[1:]

            if postfix:
                street_tokens.insert(0, postfix)

        street_name = " ".join(street_tokens)
        if postfix and postfix not in street_name:
            street_name = f"{street_name} {postfix}".strip()

        street_key = normalize_street_name(street_name)
        return number, street_key, is_even

    @staticmethod
    def _remove_direction(street_key: str) -> Optional[str]:
        tokens = street_key.split(" ")
        if len(tokens) <= 1:
            return None
        if tokens[-1] in _DIRECTIONS.values():
            return " ".join(tokens[:-1])
        return None

    def _choose_best_segment(
        self,
        segments: Iterable[CentrelineSegment],
        number: int,
        is_even: bool,
        allow_out_of_range: bool = False,
    ) -> Optional[CentrelineSegment]:
        best: Optional[CentrelineSegment] = None
        smallest_span = math.inf

        for segment in segments:
            if segment.contains_address(number, is_even):
                span = self._address_span(segment, is_even)
                if span < smallest_span:
                    best = segment
                    smallest_span = span

        if best is not None or not allow_out_of_range:
            return best

        closest_segment: Optional[CentrelineSegment] = None
        smallest_distance = math.inf

        for segment in segments:
            distance = self._address_distance(segment, number)
            if distance < smallest_distance:
                smallest_distance = distance
                closest_segment = segment

        return closest_segment

    def _lookup_segments(self, street_key: str) -> List[CentrelineSegment]:
        candidates = self._segments_by_key.get(street_key)
        if candidates:
            return candidates

        fallback_key = self._remove_direction(street_key)
        if fallback_key:
            candidates = self._segments_by_key.get(fallback_key)
            if candidates:
                return candidates

        base_key = street_key.replace(" ", "")
        for candidate_key in self._street_keys:
            if candidate_key.replace(" ", "") == base_key:
                return self._segments_by_key[candidate_key]

        if street_key in self._fuzzy_cache:
            cached = self._fuzzy_cache[street_key]
            if cached is None:
                return []
            return self._segments_by_key.get(cached, [])

        fuzzy_key = self._find_fuzzy_key(street_key)
        self._fuzzy_cache[street_key] = fuzzy_key
        if fuzzy_key is None:
            return []
        return self._segments_by_key.get(fuzzy_key, [])

    def _find_fuzzy_key(self, street_key: str) -> Optional[str]:
        if not self._street_keys:
            return None

        matches = difflib.get_close_matches(street_key, self._street_keys, n=1, cutoff=0.88)
        if matches:
            return matches[0]

        matches = difflib.get_close_matches(street_key, self._street_keys, n=1, cutoff=0.82)
        return matches[0] if matches else None

    def _parse_intersection(self, address: str) -> Optional[Tuple[str, str]]:
        raw = str(address).upper()
        separators = [
            " / ",
            " & ",
            " AND ",
            " @ ",
            " AT ",
            " N/O ",
            " S/O ",
            " E/O ",
            " W/O ",
            " NORTH OF ",
            " SOUTH OF ",
            " EAST OF ",
            " WEST OF ",
        ]

        for sep in separators:
            if sep in raw:
                parts = [part.strip() for part in raw.split(sep) if part.strip()]
                if len(parts) >= 2:
                    first = normalize_street_name(parts[0])
                    second = normalize_street_name(parts[1])
                    return first, second

        return None

    def _parse_street_only(self, address: str) -> Optional[str]:
        normalized = normalize_text(address)
        if any(char.isdigit() for char in normalized):
            return None
        if not normalized:
            return None
        return normalize_street_name(normalized)

    def _geocode_intersection(self, street_a: str, street_b: str) -> Optional[GeocodeResult]:
        segments_a = [seg for seg in self._lookup_segments(street_a) if seg.centroid]
        segments_b = [seg for seg in self._lookup_segments(street_b) if seg.centroid]

        if not segments_a or not segments_b:
            return None

        best_segment_a: Optional[CentrelineSegment] = None
        best_segment_b: Optional[CentrelineSegment] = None
        smallest_distance = math.inf

        limit = 12
        for seg_a in segments_a[:limit]:
            for seg_b in segments_b[:limit]:
                distance = _distance_between(seg_a.centroid, seg_b.centroid)
                if distance < smallest_distance:
                    smallest_distance = distance
                    best_segment_a = seg_a
                    best_segment_b = seg_b

        if best_segment_a is None or best_segment_b is None:
            return None

        lat = (best_segment_a.centroid[0] + best_segment_b.centroid[0]) / 2
        lon = (best_segment_a.centroid[1] + best_segment_b.centroid[1]) / 2

        primary = best_segment_a
        return GeocodeResult(
            street_normalized=f"{primary.street_key} & {best_segment_b.street_key}",
            latitude=lat,
            longitude=lon,
            centreline_id=primary.centreline_id,
            feature_code=primary.feature_code,
            feature_code_desc=primary.feature_code_desc,
            jurisdiction=primary.jurisdiction,
        )

    def _geocode_street_only(self, street_key: str) -> Optional[GeocodeResult]:
        segments = [seg for seg in self._lookup_segments(street_key) if seg.centroid]
        if not segments:
            return None

        lat = sum(seg.centroid[0] for seg in segments) / len(segments)
        lon = sum(seg.centroid[1] for seg in segments) / len(segments)

        representative = segments[0]
        return GeocodeResult(
            street_normalized=representative.street_key,
            latitude=lat,
            longitude=lon,
            centreline_id=representative.centreline_id,
            feature_code=representative.feature_code,
            feature_code_desc=representative.feature_code_desc,
            jurisdiction=representative.jurisdiction,
        )

    @staticmethod
    def _address_span(segment: CentrelineSegment, is_even: bool) -> float:
        if is_even:
            if segment.low_even is None or segment.high_even is None:
                return math.inf
            return segment.high_even - segment.low_even

        if segment.low_odd is None or segment.high_odd is None:
            return math.inf
        return segment.high_odd - segment.low_odd

    @staticmethod
    def _address_distance(segment: CentrelineSegment, number: int) -> float:
        distances: List[float] = []

        for low, high in (
            (segment.low_even, segment.high_even),
            (segment.low_odd, segment.high_odd),
        ):
            if low is None or high is None:
                continue
            if low <= number <= high:
                return 0.0
            if number < low:
                distances.append(low - number)
            elif number > high:
                distances.append(number - high)

        return min(distances) if distances else math.inf

    @staticmethod
    def _compose_label(row: pd.Series) -> str:
        parts: List[str] = []
        base = row.get("LINEAR_NAME")
        suffix = row.get("LINEAR_NAME_TYPE")
        direction = row.get("LINEAR_NAME_DIR")

        if isinstance(base, str) and base.lower() != "none":
            parts.append(base)
        if isinstance(suffix, str) and suffix.lower() != "none":
            parts.append(suffix)
        if isinstance(direction, str) and direction.lower() != "none":
            parts.append(direction)

        if not parts and isinstance(row.get("LINEAR_NAME_FULL"), str):
            return row.get("LINEAR_NAME_FULL")

        return " ".join(parts)

    @staticmethod
    def _compute_centroid(geometry: Optional[dict]) -> Optional[Tuple[float, float]]:
        if not geometry or not isinstance(geometry, dict):
            return None

        coordinates = geometry.get("coordinates")
        if not coordinates:
            return None

        if geometry.get("type") == "LineString":
            return _centroid_linestring(coordinates)

        if geometry.get("type") == "MultiLineString":
            all_points: List[Tuple[float, float]] = []
            for line in coordinates:
                all_points.extend(line)
            return _centroid_linestring(all_points)

        if geometry.get("type") == "Point":
            lon, lat = coordinates
            return (lat, lon)

        return None

    @staticmethod
    def _safe_int(value: object) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_upper(value: object) -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return ""
        return str(value).upper()

    @staticmethod
    def _safe_str(value: object) -> Optional[str]:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        return str(value)


# MARK: Geometry helpers


def _centroid_linestring(points: Iterable[Iterable[float]]) -> Optional[Tuple[float, float]]:
    coords: List[Tuple[float, float]] = []
    for point in points:
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            continue
        lon, lat = point
        coords.append((float(lat), float(lon)))

    if not coords:
        return None

    lat_avg = sum(lat for lat, _ in coords) / len(coords)
    lon_avg = sum(lon for _, lon in coords) / len(coords)
    return lat_avg, lon_avg


def _distance_between(point_a: Tuple[float, float], point_b: Tuple[float, float]) -> float:
    lat_diff = point_a[0] - point_b[0]
    lon_diff = point_a[1] - point_b[1]
    # Approximate conversion of degrees to meters in Toronto region
    return math.hypot(lat_diff * 111_320, lon_diff * 78_850)


__all__ = ["CentrelineGeocoder", "GeocodeResult", "normalize_street_name"]
