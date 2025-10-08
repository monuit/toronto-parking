"""Generate multi-day forecasts from a trained spatiotemporal checkpoint."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import torch

from .config import SpatioTemporalModelConfig
from .datamodule import ParkingSpatioTemporalDataModule
from .models import DCRNNLightningModule


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run inference with a trained DCRNN model")
    parser.add_argument("checkpoint", type=Path, help="Path to the Lightning checkpoint")
    parser.add_argument("--data-root", type=Path, default=Path("output/spatiotemporal"))
    parser.add_argument("--input-length", type=int, default=28)
    parser.add_argument("--forecast-horizon", type=int, default=7)
    parser.add_argument("--output-path", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    datamodule = ParkingSpatioTemporalDataModule(
        data_root=args.data_root,
        input_length=args.input_length,
        forecast_horizon=args.forecast_horizon,
        batch_size=1,
        shuffle=False,
    )
    datamodule.setup("predict")

    if datamodule.payload is None:
        raise RuntimeError("Dataset payload not initialised")
    if datamodule.edge_index is None or datamodule.edge_weight is None:
        raise RuntimeError("Graph metadata missing")
    if datamodule.feature_names is None or datamodule.target_names is None:
        raise RuntimeError("Feature metadata missing")

    target_indices = [datamodule.feature_names.index(name) for name in datamodule.target_names]

    model = DCRNNLightningModule.load_from_checkpoint(
        checkpoint_path=str(args.checkpoint),
        edge_index=datamodule.edge_index,
        edge_weight=datamodule.edge_weight,
        feature_dim=len(datamodule.feature_names),
        target_indices=target_indices,
        model_config=SpatioTemporalModelConfig(
            input_length=args.input_length,
            forecast_horizon=args.forecast_horizon,
        ),
        strict=False,
    )
    model.eval()
    model.to(torch.device("cpu"))

    context = datamodule.payload.feature_cube[-args.input_length :]
    context_tensor = torch.from_numpy(context).unsqueeze(0)

    with torch.no_grad():
        predictions = model(context_tensor, args.forecast_horizon).squeeze(0).cpu().numpy()

    forecast_dates = _build_forecast_dates(datamodule.payload.dates[-1], args.forecast_horizon)
    output_payload = _build_output_payload(
        predictions,
        forecast_dates,
        datamodule.payload,
        datamodule.target_names,
    )

    output_dir = args.output_path.parent if args.output_path else Path("output/spatiotemporal/forecasts")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_path or (output_dir / f"forecast_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json")

    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(output_payload, handle, ensure_ascii=False, indent=2)

    print(f"Saved forecast to {output_path}")


def _build_forecast_dates(last_date: pd.Timestamp, horizon: int) -> list[str]:
    start = (last_date + pd.Timedelta(days=1)).normalize()
    dates = pd.date_range(start, periods=horizon, freq="D")
    return [date.isoformat() for date in dates]


def _build_output_payload(
    predictions: np.ndarray,
    forecast_dates: list[str],
    payload,
    target_names,
) -> dict:
    nodes_df = payload.nodes_frame
    index_to_geohash = {idx: geohash for geohash, idx in payload.node_index.items()}

    features: list[dict] = []
    for node_idx in range(predictions.shape[1]):
        geohash = index_to_geohash.get(node_idx)
        if geohash is None:
            continue
        node_row = nodes_df.loc[nodes_df["geohash"] == geohash].iloc[0]
        feature_forecasts = []
        for step_idx, date in enumerate(forecast_dates):
            step_entry = {target: float(predictions[step_idx, node_idx, t_idx]) for t_idx, target in enumerate(target_names)}
            feature_forecasts.append({"date": date, **step_entry})
        features.append(
            {
                "geohash": geohash,
                "latitude": float(node_row["latitude"]),
                "longitude": float(node_row["longitude"]),
                "label": node_row.get("label"),
                "predictions": feature_forecasts,
            }
        )

    return {
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "horizon": len(forecast_dates),
        "dates": forecast_dates,
        "targetFeatures": list(target_names),
        "features": features,
    }


if __name__ == "__main__":
    main()
