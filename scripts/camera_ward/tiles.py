"""Helpers for generating precomputed ward tiles."""

from __future__ import annotations

import subprocess
from typing import Iterable

from .constants import REPO_ROOT


def build_precomputed_tiles(datasets: Iterable[str]) -> None:
    """Invoke the Node script that builds precomputed ward tiles for datasets."""

    dataset_list = list(datasets)
    script_path = REPO_ROOT / "map-app" / "scripts" / "build-ward-tiles.mjs"
    if not dataset_list or not script_path.exists():
        return

    command = ["node", str(script_path)]
    for dataset in dataset_list:
        command.extend(["--dataset", dataset])

    try:
        subprocess.run(command, check=True)
    except FileNotFoundError:
        print("Node executable not found; skipping prebuilt ward tiles.")
    except subprocess.CalledProcessError as error:
        print(f"Failed to generate prebuilt ward tiles: {error}")
        raise
