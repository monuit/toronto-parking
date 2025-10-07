"""Utility script for extracting manageable samples from large JSONL datasets."""

from __future__ import annotations

import argparse
import random
from pathlib import Path
from typing import Iterable, Iterator, List, TextIO


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Source JSONL file to sample from.")
    parser.add_argument("--output", required=True, help="Destination path for the sampled JSONL file.")
    parser.add_argument(
        "--sample-size",
        type=int,
        help="Number of examples to include. Used with the 'reservoir' or 'head' strategy.",
    )
    parser.add_argument(
        "--fraction",
        type=float,
        help="Fraction of examples to retain (0.0 - 1.0). Used with the 'fraction' strategy.",
    )
    parser.add_argument(
        "--strategy",
        choices=("reservoir", "head", "fraction"),
        default="reservoir",
        help="Sampling strategy to apply. Defaults to 'reservoir'.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1337,
        help="Random seed used for the 'reservoir' or 'fraction' strategy (default: 1337).",
    )
    parser.add_argument(
        "--strip-empty",
        action="store_true",
        help="Skip empty or whitespace-only lines instead of copying them to the output.",
    )
    return parser.parse_args()


def _iter_non_empty(path: Path, strip_empty: bool) -> Iterator[str]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if strip_empty and not line.strip():
                continue
            yield line


def _write_head(lines: Iterable[str], sample_size: int, destination: TextIO) -> int:
    count = 0
    for line in lines:
        if count >= sample_size:
            break
        destination.write(line)
        count += 1
    return count


def _sample_reservoir(lines: Iterable[str], sample_size: int, *, seed: int) -> List[str]:
    if sample_size <= 0:
        return []

    rng = random.Random(seed)
    reservoir: List[str] = []
    for count, line in enumerate(lines, start=1):
        if len(reservoir) < sample_size:
            reservoir.append(line)
            continue
        replacement_index = rng.randint(1, count)
        if replacement_index <= sample_size:
            reservoir[replacement_index - 1] = line
    return reservoir


def _write_fraction(lines: Iterable[str], fraction: float, *, seed: int, destination: TextIO) -> int:
    if not 0.0 < fraction <= 1.0:
        raise ValueError("fraction must be in the range (0.0, 1.0]")

    rng = random.Random(seed)
    kept = 0
    for line in lines:
        if rng.random() <= fraction:
            destination.write(line)
            kept += 1
    return kept


def sample_jsonl(
    *,
    input_path: Path,
    output_path: Path,
    strategy: str,
    sample_size: int | None,
    fraction: float | None,
    seed: int,
    strip_empty: bool,
) -> int:
    if strategy in {"reservoir", "head"} and sample_size is None:
        raise ValueError("--sample-size is required when using the 'reservoir' or 'head' strategy")
    if strategy == "fraction" and fraction is None:
        raise ValueError("--fraction is required when using the 'fraction' strategy")

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as destination:
        lines = _iter_non_empty(input_path, strip_empty)
        if strategy == "head":
            return _write_head(lines, sample_size or 0, destination)
        if strategy == "reservoir":
            sampled = _sample_reservoir(lines, sample_size or 0, seed=seed)
            destination.writelines(sampled)
            return len(sampled)
        return _write_fraction(lines, fraction or 0.0, seed=seed, destination=destination)


def main() -> None:
    args = _parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    try:
        count = sample_jsonl(
            input_path=input_path,
            output_path=output_path,
            strategy=args.strategy,
            sample_size=args.sample_size,
            fraction=args.fraction,
            seed=args.seed,
            strip_empty=args.strip_empty,
        )
    except Exception as exc:  # pragma: no cover - cli guard
        raise SystemExit(f"Failed to sample JSONL: {exc}") from exc

    print(f"Wrote {count:,} examples to {output_path}")


if __name__ == "__main__":
    main()
