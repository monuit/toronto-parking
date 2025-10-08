"""Configuration dataclasses for spatiotemporal forecasting pipelines."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence


@dataclass(slots=True)
class SpatioTemporalDataConfig:
    """Runtime configuration for preprocessing spatiotemporal datasets."""

    data_dir: Path = Path("parking_data/extracted")
    geocode_path: Path = Path("output/geocoded_addresses_combined.jsonl")
    output_dir: Path = Path("output/spatiotemporal")
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    chunk_size: int = 50_000
    geohash_precision: int = 6
    time_granularity: str = "1D"
    knn_k: int = 8
    min_observations_per_node: int = 64
    min_total_tickets: int = 250
    max_nodes: Optional[int] = None
    include_revenue: bool = True
    include_infraction_code: bool = False

    def iter_years(self, *, available_years: Iterable[int]) -> Iterable[int]:
        for year in available_years:
            if self.start_year is not None and year < self.start_year:
                continue
            if self.end_year is not None and year > self.end_year:
                continue
            yield year

    def serialise(self) -> Dict[str, object]:
        payload = asdict(self)
        for key in ("data_dir", "geocode_path", "output_dir"):
            payload[key] = str(payload[key])
        return payload


@dataclass(slots=True)
class SpatioTemporalModelConfig:
    """Hyperparameters controlling the DCRNN backbone."""

    input_length: int = 28
    forecast_horizon: int = 7
    hidden_dim: int = 64
    rnn_layers: int = 2
    dropout: float = 0.1
    learning_rate: float = 1e-3
    weight_decay: float = 1e-4
    gradient_clip_val: float = 1.0
    gradient_clip_algorithm: str = "norm"
    loss_fn: str = "l1"
    target_features: Sequence[str] = field(default_factory=lambda: ["tickets"])


@dataclass(slots=True)
class SpatioTemporalTrainingConfig:
    """Training runtime parameters shared across Lightning entry points."""

    max_epochs: int = 50
    accelerator: str = "cpu"
    devices: int | str = 1
    deterministic: bool = True
    log_every_n_steps: int = 50
    val_check_interval: float = 1.0
    enable_checkpointing: bool = True
    default_root_dir: Path = Path("output/spatiotemporal/checkpoints")
    precision: str | int = "32-true"
    accumulate_grad_batches: int = 1

    def serialise(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["default_root_dir"] = str(payload["default_root_dir"])
        return payload
