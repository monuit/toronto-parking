"""October forecast generation utilities.

This module produces forward-looking predictions for October 7 and 8
using historical parking ticket data. Forecasts are aggregated by
geocoded location so they can be rendered as a GeoJSON overlay in the
map application.
"""

from __future__ import annotations

import json
import math
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

import pandas as pd

DAY_DEFINITIONS = {
    7: {
        "label": "Oct 7",
        "iso_date": "-10-07",
        "day_name": "forecast_oct07",
    },
    8: {
        "label": "Oct 8",
        "iso_date": "-10-08",
        "day_name": "forecast_oct08",
    },
}


@dataclass(slots=True)
class ForecastConfig:
    """Runtime configuration for the October forecast."""

    target_year: int = 2024
    data_dir: Path = Path("parking_data/extracted")
    geocode_path: Path = Path("output/geocoding_results.json")
    output_path: Path = Path("map-app/public/data/october_forecast.geojson")
    start_year: int = 2010
    end_year: Optional[int] = None
    chunk_size: int = 50_000
    max_forecast_locations: Optional[int] = None

    def historical_years(self) -> Iterable[int]:
        """Yield years included in the historical training window."""
        final_year = (self.end_year or self.target_year - 1)
        return range(self.start_year, final_year + 1)


@dataclass
class _DaySeries:
    counts: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    revenue: Dict[int, float] = field(default_factory=lambda: defaultdict(float))
    infractions: Counter[str] = field(default_factory=Counter)


@dataclass
class _LocationAccumulator:
    lon: float
    lat: float
    location: str
    day_series: Dict[int, _DaySeries] = field(default_factory=lambda: {
        7: _DaySeries(),
        8: _DaySeries(),
    })

    def add(self, *, day: int, year: int, fine: float, infraction: Optional[str]) -> None:
        series = self.day_series[day]
        series.counts[year] += 1
        series.revenue[year] += fine
        if infraction:
            series.infractions[infraction] += 1


class OctoberForecastGenerator:
    """Compute forecasts for October 7/8 across geocoded locations."""

    def __init__(self, config: Optional[ForecastConfig] = None) -> None:
        self.config = config or ForecastConfig()
        self._geocode_lookup = self._load_geocode_lookup(self.config.geocode_path)
        self._accumulators: Dict[str, _LocationAccumulator] = {}
        self._years_with_data: set[int] = set()

    # MARK: Public API -----------------------------------------------------
    def generate_forecast(self) -> Dict[str, object]:
        """Execute the pipeline and return a GeoJSON payload."""
        self._process_historical_data()
        payload = self._build_geojson()
        if self.config.output_path:
            self._write_output(payload, self.config.output_path)
        return payload

    # MARK: Geocode helpers ------------------------------------------------
    @staticmethod
    def _load_geocode_lookup(path: Path) -> Dict[str, Dict[str, float]]:
        if not path.exists():
            raise FileNotFoundError(f"Geocoding results not found at {path}")

        with path.open("r", encoding="utf-8") as handle:
            raw_data = json.load(handle)

        lookup: Dict[str, Dict[str, float]] = {}
        for raw_address, data in raw_data.items():
            normalized = OctoberForecastGenerator._normalise_key(raw_address)
            lookup[normalized] = {
                "lat": data.get("lat"),
                "lon": data.get("lon"),
                "location": raw_address.split(",")[0].strip(),
            }
        return lookup

    @staticmethod
    def _normalise_key(address: str) -> str:
        cleaned = unicodedata.normalize("NFKD", address).upper()
        cleaned = cleaned.replace(", TORONTO, ON, CANADA", "")
        cleaned = cleaned.replace("  ", " ").strip()
        return cleaned

    def _resolve_geocode(self, location: str) -> Optional[Dict[str, float]]:
        if not location:
            return None
        candidates = [
            location,
            f"{location}, TORONTO, ON, CANADA",
            location.replace(" AVE", " AV"),
        ]
        for candidate in candidates:
            key = self._normalise_key(candidate)
            if key in self._geocode_lookup:
                return self._geocode_lookup[key]
        return None

    # MARK: Processing -----------------------------------------------------
    def _process_historical_data(self) -> None:
        for year in self.config.historical_years():
            self._process_year(year)

    def _process_year(self, year: int) -> None:
        target_map = {
            int(f"{year}1007"): 7,
            int(f"{year}1008"): 8,
        }
        for csv_path in self._iter_october_files(year):
            try:
                reader = pd.read_csv(
                    csv_path,
                    dtype={
                        "date_of_infraction": "Int64",
                        "set_fine_amount": "float32",
                        "location2": str,
                        "infraction_code": str,
                    },
                    usecols=[
                        "date_of_infraction",
                        "set_fine_amount",
                        "location2",
                        "infraction_code",
                    ],
                    chunksize=self.config.chunk_size,
                    low_memory=False,
                )
            except FileNotFoundError:
                continue

            for chunk in reader:
                filtered = chunk[chunk["date_of_infraction"].isin(target_map.keys())]
                if filtered.empty:
                    continue
                for _, row in filtered.iterrows():
                    date_value = int(row["date_of_infraction"])
                    day = target_map.get(date_value)
                    if not day:
                        continue
                    location = str(row.get("location2", "")).strip()
                    geocode = self._resolve_geocode(location)
                    if not geocode:
                        continue

                    fine_amount = float(row.get("set_fine_amount", 0.0) or 0.0)
                    infraction = row.get("infraction_code")
                    coord_key = f"{geocode['lon']:.6f}|{geocode['lat']:.6f}"

                    if coord_key not in self._accumulators:
                        self._accumulators[coord_key] = _LocationAccumulator(
                            lon=geocode["lon"],
                            lat=geocode["lat"],
                            location=geocode["location"],
                        )
                    self._accumulators[coord_key].add(
                        day=day,
                        year=year,
                        fine=fine_amount,
                        infraction=str(infraction).strip() if infraction else None,
                    )
                    self._years_with_data.add(year)

    def _iter_october_files(self, year: int) -> Iterator[Path]:
        base = Path(self.config.data_dir) / str(year)
        if not base.exists():
            return
        # Newer years ship as monthly files, older ones as single CSV.
        monthly = sorted(base.glob(f"*{year}_10*.csv"))
        if monthly:
            for path in monthly:
                yield path
            return
        for path in base.glob("*.csv"):
            yield path

    # MARK: Output ---------------------------------------------------------
    def _build_geojson(self) -> Dict[str, object]:
        features: List[Dict[str, object]] = []
        historical_years = sorted(self._years_with_data)
        if not historical_years:
            return {
                "type": "FeatureCollection",
                "generatedAt": datetime.utcnow().isoformat() + "Z",
                "targetYear": self.config.target_year,
                "dates": [],
                "features": [],
            }

        min_year, max_year = historical_years[0], historical_years[-1]
        total_years = max(1, len(historical_years))

        for coord_key, accumulator in self._accumulators.items():
            predictions = {}
            metadata = {}
            for day, series in accumulator.day_series.items():
                if not series.counts:
                    continue
                prediction = self._compute_prediction(
                    counts=series.counts,
                    revenue=series.revenue,
                    min_year=min_year,
                    max_year=max_year,
                    total_years=total_years,
                )
                trend = self._compute_trend(series.counts)
                date_string = f"{self.config.target_year}{DAY_DEFINITIONS[day]['iso_date']}"
                predictions[date_string] = {
                    **prediction,
                    "trend": trend,
                    "topInfraction": self._top_infraction(series.infractions),
                    "yearSpan": {
                        "observations": len(series.counts),
                        "firstYear": min(series.counts),
                        "lastYear": max(series.counts),
                    },
                }
                metadata[DAY_DEFINITIONS[day]["day_name"]] = prediction["tickets"]

            if not predictions:
                continue

            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [accumulator.lon, accumulator.lat],
                    },
                    "properties": {
                        "location": accumulator.location,
                        "predictions": predictions,
                        "metadata": metadata,
                    },
                }
            )

        if self.config.max_forecast_locations:
            features = sorted(
                features,
                key=lambda feat: sum(
                    pred.get("tickets", 0.0)
                    for pred in feat["properties"]["predictions"].values()
                ),
                reverse=True,
            )[: self.config.max_forecast_locations]

        return {
            "type": "FeatureCollection",
            "generatedAt": datetime.utcnow().isoformat() + "Z",
            "targetYear": self.config.target_year,
            "dates": [
                f"{self.config.target_year}{DAY_DEFINITIONS[day]['iso_date']}"
                for day in DAY_DEFINITIONS
            ],
            "historical": {
                "firstYear": min_year,
                "lastYear": max_year,
                "observations": len(features),
            },
            "features": features,
        }

    @staticmethod
    def _compute_prediction(
        *,
        counts: Dict[int, int],
        revenue: Dict[int, float],
        min_year: int,
        max_year: int,
        total_years: int,
    ) -> Dict[str, float]:
        years = sorted(counts)
        if not years:
            return {
                "tickets": 0.0,
                "revenue": 0.0,
                "avgFine": 0.0,
                "confidence": 0.0,
            }

        weighted_ticket_sum = 0.0
        weighted_revenue_sum = 0.0
        weight_total = 0.0
        for year in years:
            recency_ratio = (year - min_year) / max(1, (max_year - min_year))
            weight = 0.6 + 0.4 * recency_ratio
            weight_total += weight
            weighted_ticket_sum += counts[year] * weight
            weighted_revenue_sum += revenue.get(year, 0.0) * weight

        predicted_tickets = weighted_ticket_sum / weight_total if weight_total else 0.0
        predicted_revenue = weighted_revenue_sum / weight_total if weight_total else 0.0
        avg_fine = predicted_revenue / predicted_tickets if predicted_tickets else 0.0

        variability = OctoberForecastGenerator._variability(counts)
        coverage = len(years) / max(1, total_years)
        confidence = OctoberForecastGenerator._confidence(coverage, variability)

        return {
            "tickets": round(predicted_tickets, 2),
            "revenue": round(predicted_revenue, 2),
            "avgFine": round(avg_fine, 2),
            "confidence": round(confidence, 3),
        }

    @staticmethod
    def _variability(values: Dict[int, int]) -> float:
        observations = list(values.values())
        if len(observations) <= 1:
            return 0.0
        mean = sum(observations) / len(observations)
        if mean == 0:
            return 0.0
        variance = sum((obs - mean) ** 2 for obs in observations) / len(observations)
        std_dev = math.sqrt(max(variance, 0.0))
        return std_dev / mean

    @staticmethod
    def _confidence(coverage: float, variability: float) -> float:
        coverage_score = min(max(coverage, 0.0), 1.0)
        variability_score = 1.0 - min(max(variability, 0.0), 1.0)
        raw = 0.4 + 0.4 * coverage_score + 0.2 * variability_score
        return max(0.25, min(0.95, raw))

    @staticmethod
    def _compute_trend(counts: Dict[int, int]) -> Dict[str, float]:
        years = sorted(counts)
        if len(years) < 3:
            return {"direction": "steady", "change": 0.0}
        recent = [counts[year] for year in years[-3:]]
        prior = [counts[year] for year in years[:-3]] or [recent[0]]
        recent_avg = sum(recent) / len(recent)
        prior_avg = sum(prior) / len(prior)
        if prior_avg == 0:
            change = 0.0
        else:
            change = (recent_avg - prior_avg) / prior_avg
        direction = "up" if change > 0.05 else "down" if change < -0.05 else "steady"
        return {
            "direction": direction,
            "change": round(change, 3),
        }

    @staticmethod
    def _top_infraction(counter: Counter[str]) -> Optional[str]:
        if not counter:
            return None
        infraction, _ = counter.most_common(1)[0]
        return infraction

    @staticmethod
    def _write_output(payload: Dict[str, object], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)


def main() -> None:
    generator = OctoberForecastGenerator()
    payload = generator.generate_forecast()
    print(
        f"Generated {len(payload.get('features', []))} forecast locations "
        f"for {payload.get('dates', [])}"
    )


if __name__ == "__main__":
    main()
