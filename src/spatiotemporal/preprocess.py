"""Preprocessing utilities for neural spatiotemporal forecasting datasets."""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import geohash2
import numpy as np
import pandas as pd

from .config import SpatioTemporalDataConfig


USE_COLUMNS = (
    "date_of_infraction",
    "time_of_infraction",
    "location2",
    "set_fine_amount",
    "infraction_code",
    "officer",
)


@dataclass(slots=True)
class _NodeAggregate:
    latitude: float
    longitude: float
    label: str
    geohash: str
    tickets: int = 0
    revenue: float = 0.0
    infractions: Counter[str] = field(default_factory=Counter)


def _normalize_address(text: str) -> Optional[str]:
    if text is None:
        return None
    cleaned = str(text).strip()
    if not cleaned or cleaned.lower() == "nan":
        return None
    return cleaned.upper()


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""

    rad = math.radians
    d_lat = rad(lat2 - lat1)
    d_lon = rad(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(rad(lat1)) * math.cos(rad(lat2)) * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(1e-12, 1 - a)))
    return 6371.0 * c


class SpatioTemporalPreprocessor:
    """Generate graph-ready time series tensors from raw parking CSVs."""

    def __init__(self, config: Optional[SpatioTemporalDataConfig] = None) -> None:
        self.config = config or SpatioTemporalDataConfig()
        self._geocode_map = self._load_geocode_map(self.config.geocode_path)

    # MARK: Public API --------------------------------------------------
    def run(self) -> Dict[str, object]:
        if not self.config.data_dir.exists():
            raise FileNotFoundError(f"Parking data directory missing: {self.config.data_dir}")

        years = self._list_years(self.config.data_dir)
        if not years:
            raise RuntimeError("No yearly folders discovered in parking data directory")

        aggregates: Dict[Tuple[pd.Timestamp, str], Dict[str, object]] = defaultdict(
            lambda: {
                "tickets": 0,
                "revenue": 0.0,
                "fine_sum": 0.0,
                "fine_count": 0,
                "infractions": Counter(),
            }
        )
        node_stats: Dict[str, _NodeAggregate] = {}

        for year in self.config.iter_years(available_years=years):
            for csv_path in self._iter_ticket_files(year):
                for chunk in self._iter_chunks(csv_path):
                    if chunk.empty:
                        continue
                    chunk = chunk.dropna(subset=["location2", "date_of_infraction"])
                    chunk["location2"] = chunk["location2"].astype(str)
                    chunk["date"] = pd.to_datetime(
                        chunk["date_of_infraction"].astype(str), errors="coerce"
                    )
                    chunk = chunk.dropna(subset=["date"])
                    chunk["date"] = chunk["date"].dt.floor(self.config.time_granularity)

                    for record in chunk.to_dict("records"):
                        normalized = _normalize_address(record.get("location2"))
                        if not normalized:
                            continue
                        geocode = self._resolve_geocode(normalized)
                        if geocode is None:
                            continue

                        lat = geocode["latitude"]
                        lon = geocode["longitude"]
                        label = geocode["label"]
                        geohash = geohash2.encode(lat, lon, self.config.geohash_precision)
                        date_key: pd.Timestamp = record["date"]

                        key = (date_key, geohash)
                        bucket = aggregates[key]
                        bucket["tickets"] += 1
                        fine_increment = None
                        fine = record.get("set_fine_amount")
                        if fine is not None and not pd.isna(fine):
                            fine_increment = float(fine)
                            bucket["revenue"] += fine_increment
                            bucket["fine_sum"] += fine_increment
                            bucket["fine_count"] += 1
                        infraction_value = record.get("infraction_code")
                        infraction_code = None
                        if infraction_value is not None and not pd.isna(infraction_value):
                            infraction_code = str(infraction_value)
                            bucket["infractions"][infraction_code] += 1

                        node = node_stats.get(geohash)
                        if node is None:
                            node = _NodeAggregate(
                                latitude=lat,
                                longitude=lon,
                                label=label,
                                geohash=geohash,
                            )
                            node_stats[geohash] = node
                        node.tickets += 1
                        if fine_increment is not None:
                            node.revenue += fine_increment
                        if infraction_code:
                            node.infractions[infraction_code] += 1

        if not aggregates:
            raise RuntimeError("No aggregated samples were produced; check preprocessing configuration")

        filtered_nodes = self._filter_nodes(node_stats)
        timeseries_df = self._build_timeseries_dataframe(aggregates, filtered_nodes)
        metadata = self._write_outputs(timeseries_df, filtered_nodes)
        return metadata

    # MARK: Iterators ---------------------------------------------------
    def _iter_ticket_files(self, year: int) -> Iterator[Path]:
        year_dir = self.config.data_dir / str(year)
        if not year_dir.exists():
            return iter(())
        fixed = sorted(year_dir.glob("*_fixed.csv"))
        if fixed:
            for path in fixed:
                yield path
            return
        for path in sorted(year_dir.glob("*.csv")):
            yield path

    def _iter_chunks(self, csv_path: Path) -> Iterator[pd.DataFrame]:
        try:
            reader = pd.read_csv(
                csv_path,
                usecols=lambda col: col in USE_COLUMNS,
                dtype={"location2": str},
                chunksize=self.config.chunk_size,
                low_memory=False,
            )
        except FileNotFoundError:
            return iter(())
        except pd.errors.ParserError:
            reader = pd.read_csv(
                csv_path,
                usecols=lambda col: col in USE_COLUMNS,
                dtype={"location2": str},
                chunksize=self.config.chunk_size,
                low_memory=False,
                engine="python",
            )
        for chunk in reader:
            yield chunk

    # MARK: Helper utilities -------------------------------------------
    @staticmethod
    def _list_years(data_dir: Path) -> List[int]:
        years: List[int] = []
        for entry in sorted(data_dir.iterdir()):
            if not entry.is_dir():
                continue
            try:
                years.append(int(entry.name))
            except ValueError:
                continue
        return years

    def _load_geocode_map(self, path: Path) -> Dict[str, Dict[str, float]]:
        if not path.exists():
            raise FileNotFoundError(f"Geocode lookup not found: {path}")

        lookup: Dict[str, Dict[str, float]] = {}
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                address = _normalize_address(payload.get("address"))
                latitude = payload.get("latitude")
                longitude = payload.get("longitude")
                if not address or latitude is None or longitude is None:
                    continue
                lookup[address] = {
                    "latitude": float(latitude),
                    "longitude": float(longitude),
                    "label": payload.get("address", "Unknown"),
                }
        return lookup

    def _resolve_geocode(self, address: str) -> Optional[Dict[str, float]]:
        if address in self._geocode_map:
            return self._geocode_map[address]
        fallback = f"{address}, TORONTO, ON"
        return self._geocode_map.get(fallback)

    def _filter_nodes(self, nodes: Dict[str, _NodeAggregate]) -> Dict[str, _NodeAggregate]:
        if not nodes:
            return {}
        eligible = [
            node
            for node in nodes.values()
            if node.tickets >= self.config.min_total_tickets
        ]
        eligible.sort(key=lambda node: node.tickets, reverse=True)
        if self.config.max_nodes is not None:
            eligible = eligible[: self.config.max_nodes]
        return {node.geohash: node for node in eligible}

    def _build_timeseries_dataframe(
        self,
        aggregates: Dict[Tuple[pd.Timestamp, str], Dict[str, object]],
        nodes: Dict[str, _NodeAggregate],
    ) -> pd.DataFrame:
        records: List[Dict[str, object]] = []
        for (date, geohash), payload in aggregates.items():
            if geohash not in nodes:
                continue
            avg_fine = (
                payload["fine_sum"] / payload["fine_count"]
                if payload["fine_count"]
                else 0.0
            )
            records.append(
                {
                    "date": date,
                    "geohash": geohash,
                    "tickets": payload["tickets"],
                    "revenue": payload["revenue"],
                    "avg_fine": avg_fine,
                    "top_infraction": payload["infractions"].most_common(1)[0][0]
                    if payload["infractions"]
                    else None,
                }
            )
        df = pd.DataFrame.from_records(records)
        if df.empty:
            raise RuntimeError("Filtered timeseries dataframe is empty")
        df = df.sort_values(["date", "geohash"]).reset_index(drop=True)
        return df

    def _write_outputs(
        self,
        timeseries_df: pd.DataFrame,
        nodes: Dict[str, _NodeAggregate],
    ) -> Dict[str, object]:
        output_dir = self.config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        timeseries_path = output_dir / "ticket_timeseries.csv.gz"
        nodes_path = output_dir / "nodes.csv"
        adjacency_path = output_dir / "adjacency.json"
        manifest_path = output_dir / "manifest.json"

        timeseries_df.to_csv(timeseries_path, index=False, compression="gzip")

        node_rows = []
        for node in nodes.values():
            node_rows.append(
                {
                    "geohash": node.geohash,
                    "latitude": node.latitude,
                    "longitude": node.longitude,
                    "label": node.label,
                    "tickets": node.tickets,
                    "revenue": node.revenue,
                    "infraction_breadth": len(node.infractions),
                }
            )
        nodes_df = pd.DataFrame(node_rows)
        nodes_df.to_csv(nodes_path, index=False)

        adjacency_payload = self._build_adjacency(nodes_df)
        with adjacency_path.open("w", encoding="utf-8") as handle:
            json.dump(adjacency_payload, handle, ensure_ascii=False)

        manifest = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "config": self.config.serialise(),
            "timeseries_path": str(timeseries_path),
            "nodes_path": str(nodes_path),
            "adjacency_path": str(adjacency_path),
            "dates": {
                "min": timeseries_df["date"].min().isoformat(),
                "max": timeseries_df["date"].max().isoformat(),
            },
            "geohash_count": len(nodes_df),
            "sample_count": int(len(timeseries_df)),
        }

        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2)

        return manifest

    def _build_adjacency(self, nodes_df: pd.DataFrame) -> Dict[str, object]:
        coords = nodes_df[["latitude", "longitude"]].to_numpy(dtype=float)
        geohashes = nodes_df["geohash"].tolist()
        total_nodes = len(geohashes)
        if total_nodes == 0:
            return {"edges": [], "metadata": {"knn_k": self.config.knn_k}}

        distances = np.full((total_nodes, total_nodes), np.inf, dtype=float)
        for i in range(total_nodes):
            lat1, lon1 = coords[i]
            for j in range(i + 1, total_nodes):
                lat2, lon2 = coords[j]
                d = _haversine_km(lat1, lon1, lat2, lon2)
                distances[i, j] = d
                distances[j, i] = d

        edges: List[Dict[str, object]] = []
        k = min(self.config.knn_k, max(1, total_nodes - 1))
        for source_idx in range(total_nodes):
            nearest = np.argpartition(distances[source_idx], k)[:k]
            for target_idx in nearest:
                if target_idx == source_idx:
                    continue
                distance = float(distances[source_idx, target_idx])
                if math.isinf(distance) or distance <= 0:
                    continue
                weight = 1.0 / (1.0 + distance)
                edges.append(
                    {
                        "source": geohashes[source_idx],
                        "target": geohashes[target_idx],
                        "weight": round(weight, 6),
                        "distance_km": round(distance, 3),
                    }
                )

        return {
            "edges": edges,
            "metadata": {
                "knn_k": self.config.knn_k,
                "total_nodes": total_nodes,
            },
        }
