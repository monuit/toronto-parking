"""Dataset loader and sequence utilities for spatiotemporal models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset


TIMESERIES_FILENAME = "ticket_timeseries.csv.gz"
NODES_FILENAME = "nodes.csv"
ADJACENCY_FILENAME = "adjacency.json"
MANIFEST_FILENAME = "manifest.json"


@dataclass(slots=True)
class DatasetPayload:
    feature_cube: np.ndarray
    target_cube: np.ndarray
    feature_names: List[str]
    target_names: List[str]
    dates: List[pd.Timestamp]
    node_index: Dict[str, int]
    nodes_frame: pd.DataFrame
    edge_index: np.ndarray
    edge_weight: np.ndarray


class ParkingSpatioTemporalDataset:
    """Materialise tensors from preprocessed CSV/JSON outputs."""

    def __init__(
        self,
        root: Path | str = Path("output/spatiotemporal"),
        *,
        feature_names: Optional[Sequence[str]] = None,
        target_names: Optional[Sequence[str]] = None,
    ) -> None:
        self.root = Path(root)
        self.feature_names = list(feature_names or ["tickets", "revenue", "avg_fine"])
        self.target_names = list(target_names or ["tickets"])
        self._payload: Optional[DatasetPayload] = None

    # MARK: Public API --------------------------------------------------
    def load(self) -> DatasetPayload:
        if self._payload is not None:
            return self._payload

        timeseries_path = self.root / TIMESERIES_FILENAME
        nodes_path = self.root / NODES_FILENAME
        adjacency_path = self.root / ADJACENCY_FILENAME
        manifest_path = self.root / MANIFEST_FILENAME

        if not timeseries_path.exists():
            raise FileNotFoundError(f"Timeseries not found: {timeseries_path}")
        if not nodes_path.exists():
            raise FileNotFoundError(f"Node metadata not found: {nodes_path}")
        if not adjacency_path.exists():
            raise FileNotFoundError(f"Adjacency not found: {adjacency_path}")

        series_df = pd.read_csv(timeseries_path, parse_dates=["date"])
        nodes_df = pd.read_csv(nodes_path)
        with adjacency_path.open("r", encoding="utf-8") as handle:
            adjacency_payload = json.load(handle)

        node_index = {row.geohash: idx for idx, row in nodes_df.iterrows()}
        feature_cube, dates = self._build_feature_cube(series_df, node_index)
        target_cube = feature_cube[..., self._target_indices]
        edge_index, edge_weight = self._build_adjacency_tensors(adjacency_payload, node_index)

        self._payload = DatasetPayload(
            feature_cube=feature_cube,
            target_cube=target_cube,
            feature_names=list(self.feature_names),
            target_names=list(self.target_names),
            dates=dates,
            node_index=node_index,
            nodes_frame=nodes_df,
            edge_index=edge_index,
            edge_weight=edge_weight,
        )

        if manifest_path.exists():
            # Touch manifest to ensure consumers know load succeeded (optional side effect)
            manifest_path.touch()
        return self._payload

    # MARK: Internal helpers ------------------------------------------
    @property
    def _target_indices(self) -> np.ndarray:
        return np.array([self.feature_names.index(name) for name in self.target_names], dtype=int)

    def _build_feature_cube(
        self,
        series_df: pd.DataFrame,
        node_index: Dict[str, int],
    ) -> tuple[np.ndarray, List[pd.Timestamp]]:
        required_columns = {"date", "geohash", *self.feature_names}
        missing = required_columns - set(series_df.columns)
        if missing:
            raise ValueError(f"Timeseries dataframe missing columns: {sorted(missing)}")

        dates = sorted(series_df["date"].unique())
        num_dates = len(dates)
        num_nodes = len(node_index)
        num_features = len(self.feature_names)

        cube = np.zeros((num_dates, num_nodes, num_features), dtype=np.float32)
        date_index = {date: idx for idx, date in enumerate(dates)}

        for _, row in series_df.iterrows():
            date_idx = date_index[row["date"]]
            node_idx = node_index.get(row["geohash"])
            if node_idx is None:
                continue
            for feature_idx, feature_name in enumerate(self.feature_names):
                cube[date_idx, node_idx, feature_idx] = float(row.get(feature_name, 0.0) or 0.0)

        return cube, dates

    def _build_adjacency_tensors(
        self,
        payload: Dict[str, object],
        node_index: Dict[str, int],
    ) -> tuple[np.ndarray, np.ndarray]:
        edges = payload.get("edges", [])
        if not isinstance(edges, list):
            raise ValueError("Adjacency payload missing 'edges' list")

        edge_index: List[List[int]] = [[], []]
        edge_weight: List[float] = []
        for entry in edges:
            source = node_index.get(entry.get("source"))
            target = node_index.get(entry.get("target"))
            weight = entry.get("weight")
            if source is None or target is None or weight is None:
                continue
            edge_index[0].append(source)
            edge_index[1].append(target)
            edge_weight.append(float(weight))

        if not edge_weight:
            # Fallback to identity self-loops if KNN failed
            total_nodes = len(node_index)
            edge_index = [list(range(total_nodes)), list(range(total_nodes))]
            edge_weight = [1.0] * total_nodes

        edge_index_tensor = np.asarray(edge_index, dtype=np.int64)
        edge_weight_tensor = np.asarray(edge_weight, dtype=np.float32)
        return edge_index_tensor, edge_weight_tensor


class ParkingSequenceDataset(Dataset):
    """Return sliding window sequences for seq2seq forecasting."""

    def __init__(
        self,
        feature_cube: np.ndarray,
        target_cube: np.ndarray,
        *,
        input_length: int,
        forecast_horizon: int,
        start_index: int = 0,
        end_index: Optional[int] = None,
    ) -> None:
        if feature_cube.ndim != 3:
            raise ValueError("feature_cube must be (time, nodes, features)")

        self.feature_cube = feature_cube
        self.target_cube = target_cube
        self.input_length = int(input_length)
        self.forecast_horizon = int(forecast_horizon)
        self.start_index = int(start_index)
        total_sequences = feature_cube.shape[0] - self.input_length - self.forecast_horizon + 1
        if total_sequences <= 0:
            raise ValueError("Insufficient timesteps for requested input/horizon")
        self.end_index = int(end_index) if end_index is not None else total_sequences
        self.end_index = min(self.end_index, total_sequences)

    def __len__(self) -> int:
        return max(0, self.end_index - self.start_index)

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        base = self.start_index + idx
        seq_start = base
        seq_end = base + self.input_length
        target_end = seq_end + self.forecast_horizon

        x = self.feature_cube[seq_start:seq_end]
        y = self.target_cube[seq_end:target_end]

        return {
            "x": torch.from_numpy(x.copy()),
            "y": torch.from_numpy(y.copy()),
        }
