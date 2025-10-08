"""PyTorch Lightning wrapper around the DCRNN architecture."""

from __future__ import annotations

from typing import Sequence

import torch
import pytorch_lightning as pl
from torch import nn
from torch.optim import AdamW
from torchmetrics import MeanSquaredError, MeanAbsoluteError
from torch_geometric_temporal.nn.recurrent import DCRNN

from ..config import SpatioTemporalModelConfig


class DCRNNLightningModule(pl.LightningModule):
    """Sequence-to-sequence DCRNN for multi-step parking ticket forecasting."""

    def __init__(
        self,
        *,
        edge_index: torch.Tensor,
        edge_weight: torch.Tensor,
        feature_dim: int,
        target_indices: Sequence[int],
        model_config: SpatioTemporalModelConfig | None = None,
    ) -> None:
        super().__init__()
        self.model_config = model_config or SpatioTemporalModelConfig()
        self.feature_dim = int(feature_dim)
        self.target_indices = torch.tensor(list(target_indices), dtype=torch.long)
        self.target_dim = len(self.target_indices)

        self.recurrent_layers = nn.ModuleList(
            [
                DCRNN(
                    node_features=self.feature_dim if idx == 0 else self.model_config.hidden_dim,
                    out_channels=self.model_config.hidden_dim,
                    K=2,
                )
                for idx in range(self.model_config.rnn_layers)
            ]
        )
        self.projection = nn.Linear(self.model_config.hidden_dim, self.target_dim)
        self.dropout_layer = nn.Dropout(self.model_config.dropout)

        self.loss_fn = self._build_loss(self.model_config.loss_fn)
        self.rmse = MeanSquaredError(squared=False)
        self.mae = MeanAbsoluteError()

        self.register_buffer("edge_index", edge_index.long())
        self.register_buffer("edge_weight", edge_weight.float())

    # MARK: Forward -----------------------------------------------------
    def forward(self, x: torch.Tensor, horizon: int) -> torch.Tensor:
        """Autoregressively predict the next `horizon` steps."""

        # x: (batch, seq_len, nodes, features)
        batch_size, seq_len, num_nodes, _ = x.shape
        hidden_states = [None] * len(self.recurrent_layers)

        # Warm-up encoder pass
        for step in range(seq_len):
            step_input = x[:, step]
            for layer_idx, layer in enumerate(self.recurrent_layers):
                hidden_states[layer_idx] = layer(
                    step_input,
                    self.edge_index,
                    self.edge_weight,
                    hidden_states[layer_idx],
                )
                step_input = hidden_states[layer_idx]

        decoder_input = x[:, -1].clone()
        predictions: list[torch.Tensor] = []

        for _ in range(horizon):
            step_input = decoder_input
            for layer_idx, layer in enumerate(self.recurrent_layers):
                hidden_states[layer_idx] = layer(
                    step_input,
                    self.edge_index,
                    self.edge_weight,
                    hidden_states[layer_idx],
                )
                step_input = hidden_states[layer_idx]
            step_hidden = self.dropout_layer(step_input)
            step_prediction = self.projection(step_hidden)
            predictions.append(step_prediction.unsqueeze(1))
            decoder_input = decoder_input.clone()
            decoder_input[:, :, self.target_indices] = step_prediction

        return torch.cat(predictions, dim=1)

    # MARK: Lightning hooks -------------------------------------------
    def training_step(self, batch, batch_idx):
        preds, targets = self._shared_step(batch, stage="train")
        loss = self.loss_fn(preds, targets)
        self.log("train_loss", loss, prog_bar=True, on_step=True, on_epoch=True)
        return loss

    def validation_step(self, batch, batch_idx):
        preds, targets = self._shared_step(batch, stage="val")
        loss = self.loss_fn(preds, targets)
        self.log("val_loss", loss, prog_bar=True, on_epoch=True)
        self._log_metrics(preds, targets, prefix="val")
        return loss

    def test_step(self, batch, batch_idx):
        preds, targets = self._shared_step(batch, stage="test")
        loss = self.loss_fn(preds, targets)
        self.log("test_loss", loss, prog_bar=True)
        self._log_metrics(preds, targets, prefix="test")
        return loss

    def predict_step(self, batch, batch_idx, dataloader_idx: int = 0):
        preds, _ = self._shared_step(batch, stage="predict")
        return preds

    def configure_optimizers(self):
        optimizer = AdamW(
            self.parameters(),
            lr=self.model_config.learning_rate,
            weight_decay=self.model_config.weight_decay,
        )
        return optimizer

    # MARK: Helpers -----------------------------------------------------
    def _shared_step(self, batch, stage: str) -> tuple[torch.Tensor, torch.Tensor]:
        x = batch["x"].float()
        y = batch["y"].float()
        preds = self.forward(x, y.shape[1])
        return preds, y

    def _build_loss(self, name: str) -> nn.Module:
        mapping = {
            "l1": nn.L1Loss(),
            "mae": nn.L1Loss(),
            "l2": nn.MSELoss(),
            "mse": nn.MSELoss(),
        }
        loss = mapping.get(name.lower()) if isinstance(name, str) else None
        if loss is None:
            raise ValueError(f"Unsupported loss function: {name}")
        return loss

    def _log_metrics(self, preds: torch.Tensor, targets: torch.Tensor, *, prefix: str) -> None:
        preds_flat = preds.reshape(-1, self.target_dim)
        targets_flat = targets.reshape(-1, self.target_dim)
        rmse_val = self.rmse(preds_flat, targets_flat)
        mae_val = self.mae(preds_flat, targets_flat)
        self.log(f"{prefix}_rmse", rmse_val, prog_bar=True, on_epoch=True)
        self.log(f"{prefix}_mae", mae_val, prog_bar=True, on_epoch=True)
