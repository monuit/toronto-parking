"""Command-line training entry point for neural spatiotemporal models."""

from __future__ import annotations

import argparse
from pathlib import Path

import pytorch_lightning as pl
from pytorch_lightning.callbacks import LearningRateMonitor, ModelCheckpoint

from .config import SpatioTemporalModelConfig, SpatioTemporalTrainingConfig
from .datamodule import ParkingSpatioTemporalDataModule
from .models import DCRNNLightningModule


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a DCRNN spatiotemporal model")
    parser.add_argument("--data-root", type=Path, default=Path("output/spatiotemporal"))
    parser.add_argument("--input-length", type=int, default=28)
    parser.add_argument("--forecast-horizon", type=int, default=7)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-epochs", type=int, default=50)
    parser.add_argument("--hidden-dim", type=int, default=64)
    parser.add_argument("--rnn-layers", type=int, default=2)
    parser.add_argument("--dropout", type=float, default=0.1)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--accelerator", type=str, default="cpu")
    parser.add_argument("--devices", type=str, default="1")
    parser.add_argument("--default-root-dir", type=Path, default=Path("output/spatiotemporal/checkpoints"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    devices = args.devices
    if isinstance(devices, str) and devices.isdigit():
        devices = int(devices)

    model_config = SpatioTemporalModelConfig(
        input_length=args.input_length,
        forecast_horizon=args.forecast_horizon,
        hidden_dim=args.hidden_dim,
        rnn_layers=args.rnn_layers,
        dropout=args.dropout,
        learning_rate=args.learning_rate,
        weight_decay=args.weight_decay,
    )
    training_config = SpatioTemporalTrainingConfig(
        max_epochs=args.max_epochs,
        accelerator=args.accelerator,
        devices=devices,
        default_root_dir=args.default_root_dir,
    )

    datamodule = ParkingSpatioTemporalDataModule(
        data_root=args.data_root,
        input_length=model_config.input_length,
        forecast_horizon=model_config.forecast_horizon,
        batch_size=args.batch_size,
    )
    datamodule.setup("fit")

    if datamodule.edge_index is None or datamodule.edge_weight is None:
        raise RuntimeError("Edge metadata missing after datamodule setup")
    if datamodule.feature_names is None or datamodule.target_names is None:
        raise RuntimeError("Feature metadata missing after datamodule setup")

    target_indices = [
        datamodule.feature_names.index(name) for name in datamodule.target_names
    ]

    model = DCRNNLightningModule(
        edge_index=datamodule.edge_index,
        edge_weight=datamodule.edge_weight,
        feature_dim=len(datamodule.feature_names),
        target_indices=target_indices,
        model_config=model_config,
    )

    callbacks = []
    if training_config.enable_checkpointing:
        callbacks.append(
            ModelCheckpoint(
                monitor="val_loss",
                mode="min",
                save_top_k=1,
                filename="dcrnn-{epoch:03d}-{val_loss:.4f}",
            )
        )
    callbacks.append(LearningRateMonitor(logging_interval="epoch"))

    trainer = pl.Trainer(
        max_epochs=training_config.max_epochs,
        accelerator=training_config.accelerator,
        devices=training_config.devices,
        deterministic=training_config.deterministic,
        log_every_n_steps=training_config.log_every_n_steps,
        val_check_interval=training_config.val_check_interval,
        default_root_dir=str(training_config.default_root_dir),
        enable_checkpointing=training_config.enable_checkpointing,
        precision=training_config.precision,
        callbacks=callbacks,
        accumulate_grad_batches=training_config.accumulate_grad_batches,
    )

    trainer.fit(model=model, datamodule=datamodule)


if __name__ == "__main__":
    main()
