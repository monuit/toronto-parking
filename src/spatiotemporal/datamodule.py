"""PyTorch Lightning DataModule wrapping the spatiotemporal dataset."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence

import torch
from torch.utils.data import DataLoader
import pytorch_lightning as pl

from .dataset import ParkingSequenceDataset, ParkingSpatioTemporalDataset


class ParkingSpatioTemporalDataModule(pl.LightningDataModule):
    """Prepare train/val/test splits for spatiotemporal forecasting."""

    def __init__(
        self,
        *,
        data_root: Path | str = Path("output/spatiotemporal"),
        feature_names: Optional[Sequence[str]] = None,
        target_names: Optional[Sequence[str]] = None,
        input_length: int = 28,
        forecast_horizon: int = 7,
        batch_size: int = 8,
        num_workers: int = 0,
        train_ratio: float = 0.7,
        val_ratio: float = 0.15,
        shuffle: bool = True,
    ) -> None:
        super().__init__()
        self.dataset_builder = ParkingSpatioTemporalDataset(
            root=data_root,
            feature_names=feature_names,
            target_names=target_names,
        )
        self.input_length = int(input_length)
        self.forecast_horizon = int(forecast_horizon)
        self.batch_size = int(batch_size)
        self.num_workers = int(num_workers)
        self.train_ratio = float(train_ratio)
        self.val_ratio = float(val_ratio)
        self.shuffle = shuffle

        self.payload = None
        self.train_dataset: Optional[ParkingSequenceDataset] = None
        self.val_dataset: Optional[ParkingSequenceDataset] = None
        self.test_dataset: Optional[ParkingSequenceDataset] = None
        self.edge_index: Optional[torch.Tensor] = None
        self.edge_weight: Optional[torch.Tensor] = None
        self.feature_names: Optional[Sequence[str]] = None
        self.target_names: Optional[Sequence[str]] = None

    def setup(self, stage: Optional[str] = None) -> None:
        if self.payload is not None:
            return

        payload = self.dataset_builder.load()
        self.payload = payload
        self.edge_index = torch.from_numpy(payload.edge_index)
        self.edge_weight = torch.from_numpy(payload.edge_weight)
        self.feature_names = payload.feature_names
        self.target_names = payload.target_names

        total_sequences = payload.feature_cube.shape[0] - self.input_length - self.forecast_horizon + 1
        if total_sequences <= 0:
            raise ValueError("Insufficient timesteps for requested configuration")

        train_end = max(1, int(total_sequences * self.train_ratio))
        val_end = max(train_end + 1, int(total_sequences * (self.train_ratio + self.val_ratio)))
        val_end = min(val_end, total_sequences)

        val_start = max(0, train_end - self.input_length)
        test_start = max(0, val_end - self.input_length)

        self.train_dataset = ParkingSequenceDataset(
            payload.feature_cube,
            payload.target_cube,
            input_length=self.input_length,
            forecast_horizon=self.forecast_horizon,
            start_index=0,
            end_index=train_end,
        )
        self.val_dataset = ParkingSequenceDataset(
            payload.feature_cube,
            payload.target_cube,
            input_length=self.input_length,
            forecast_horizon=self.forecast_horizon,
            start_index=val_start,
            end_index=val_end,
        )
        self.test_dataset = ParkingSequenceDataset(
            payload.feature_cube,
            payload.target_cube,
            input_length=self.input_length,
            forecast_horizon=self.forecast_horizon,
            start_index=test_start,
            end_index=total_sequences,
        )

    # MARK: Lightning hooks -------------------------------------------
    def train_dataloader(self) -> DataLoader:
        if self.train_dataset is None:
            raise RuntimeError("DataModule.setup() must be called before requesting dataloaders")
        return DataLoader(
            self.train_dataset,
            batch_size=self.batch_size,
            shuffle=self.shuffle,
            num_workers=self.num_workers,
            drop_last=True,
        )

    def val_dataloader(self) -> DataLoader:
        if self.val_dataset is None:
            raise RuntimeError("DataModule.setup() must be called before requesting dataloaders")
        return DataLoader(
            self.val_dataset,
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.num_workers,
            drop_last=False,
        )

    def test_dataloader(self) -> DataLoader:
        if self.test_dataset is None:
            raise RuntimeError("DataModule.setup() must be called before requesting dataloaders")
        return DataLoader(
            self.test_dataset,
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.num_workers,
            drop_last=False,
        )
