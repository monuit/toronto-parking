"""Generate fine-tuning datasets and evaluation splits from parking ticket CSVs."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from src.fine_tuning.conversion import ConversionProgress, ConversionSpec, convert_csv_to_jsonl
from src.fine_tuning.cli import (
    DEFAULT_COMPLETION_TEMPLATE,
    DEFAULT_METADATA_FIELDS,
    DEFAULT_PROMPT_TEMPLATE,
    DEFAULT_SYSTEM_PROMPT,
)

TRAIN_EXCLUDED_MONTHS = {3, 10, 11, 12}
SEASONAL_MONTH = 3
TEST_CASE_MONTHS = {10, 11, 12}


@dataclass(frozen=True)
class SplitConfig:
    name: str
    months: Sequence[int]
    row_filter: Callable[[Mapping[str, object]], bool]


@dataclass(frozen=True)
class SplitSummary:
    name: str
    path: Path
    examples: int
    skipped: int
    checksum: str
    months: Sequence[int]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        default="parking_data/extracted",
        help="Root directory containing per-year parking CSV exports.",
    )
    parser.add_argument(
        "--output-root",
        default="output/fine_tuning",
        help="Destination directory for generated JSONL datasets.",
    )
    parser.add_argument(
        "--years",
        nargs="*",
        type=int,
        help="Subset of years to process (defaults to all directories under data-root).",
    )
    parser.add_argument(
        "--spec-config",
        help="Optional JSON file overriding ConversionSpec defaults.",
    )
    parser.add_argument(
        "--skip-aggregate",
        action="store_true",
        help="Skip building aggregated cross-year splits (train/test_case/seasonal).",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Print periodic progress updates while converting CSVs.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=5000,
        help="Number of processed rows between progress updates (default: 5000).",
    )
    return parser.parse_args()


def _load_spec(config_path: Optional[str]) -> ConversionSpec:
    payload: Dict[str, object] = {}
    if config_path:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Spec configuration file not found: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))

    return ConversionSpec(
        prompt_template=str(payload.get("prompt_template", DEFAULT_PROMPT_TEMPLATE)),
        completion_template=str(payload.get("completion_template", DEFAULT_COMPLETION_TEMPLATE)),
        system_prompt=str(payload.get("system_prompt", DEFAULT_SYSTEM_PROMPT)),
        output_format=str(payload.get("output_format", "chat")),
        metadata_fields=tuple(payload.get("metadata_fields", DEFAULT_METADATA_FIELDS)),
        drop_if_missing=tuple(payload.get("drop_if_missing", ("infraction_description", "location2"))),
        metadata_key=str(payload.get("metadata_key", "metadata")),
        timezone=str(payload.get("timezone", "America/Toronto")),
        dedupe_examples=bool(payload.get("dedupe_examples", False)),
        lower_case_prompt=bool(payload.get("lower_case_prompt", False)),
        lower_case_completion=bool(payload.get("lower_case_completion", False)),
        include_metadata_in_chat=bool(payload.get("include_metadata_in_chat", False)),
    )


def _month_from_row(row: Mapping[str, object]) -> Optional[int]:
    raw = row.get("date_of_infraction")
    if raw is None:
        return None
    text = str(raw).strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return int(text[4:6])
    except ValueError:
        return None


def _include_if_month(months: Sequence[int]) -> Callable[[Mapping[str, object]], bool]:
    allowed = set(months)

    def _filter(row: Mapping[str, object]) -> bool:
        month = _month_from_row(row)
        return month is not None and month in allowed

    return _filter


def _exclude_months(excluded: Sequence[int]) -> Callable[[Mapping[str, object]], bool]:
    blocked = set(excluded)

    def _filter(row: Mapping[str, object]) -> bool:
        month = _month_from_row(row)
        return month is not None and month not in blocked

    return _filter


def _collect_years(data_root: Path, explicit_years: Optional[Sequence[int]]) -> List[int]:
    if explicit_years:
        return sorted(set(int(year) for year in explicit_years))

    years: List[int] = []
    if not data_root.exists():
        return years
    for child in data_root.iterdir():
        if child.is_dir() and child.name.isdigit():
            years.append(int(child.name))
    return sorted(years)


def _collect_year_csvs(data_root: Path, year: int) -> List[Path]:
    year_dir = data_root / str(year)
    if not year_dir.exists():
        return []
    candidates: List[Path] = []
    for path in year_dir.iterdir():
        if not path.is_file():
            continue

        suffix = path.suffix.lower()
        if suffix == ".csv":
            candidates.append(path)
            continue

        if suffix.lstrip(".").isdigit() and path.stem.lower().startswith("parking_tags_data"):
            candidates.append(path)

    return sorted(candidates)


def _hash_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_manifest(manifest_path: Path, payload: Dict[str, object]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _generate_splits(
    csv_paths: Iterable[Path],
    spec: ConversionSpec,
    output_dir: Path,
    *,
    progress_callback: Optional[Callable[[str, ConversionProgress], None]] = None,
    progress_interval: int = 5000,
) -> List[SplitSummary]:
    split_definitions = [
        SplitConfig(
            name="train",
            months=tuple(month for month in range(1, 13) if month not in TRAIN_EXCLUDED_MONTHS),
            row_filter=_exclude_months(TRAIN_EXCLUDED_MONTHS),
        ),
        SplitConfig(
            name="test_case",
            months=tuple(sorted(TEST_CASE_MONTHS)),
            row_filter=_include_if_month(TEST_CASE_MONTHS),
        ),
        SplitConfig(
            name="seasonal",
            months=(SEASONAL_MONTH,),
            row_filter=_include_if_month({SEASONAL_MONTH}),
        ),
    ]

    summaries: List[SplitSummary] = []
    for split in split_definitions:
        destination = output_dir / f"{split.name}.jsonl"
        if progress_callback:
            def _handle_progress(event: ConversionProgress, name: str = split.name) -> None:
                progress_callback(name, event)
        else:
            _handle_progress = None

        summary = convert_csv_to_jsonl(
            inputs=csv_paths,
            output_path=destination,
            spec=spec,
            row_filter=split.row_filter,
            progress_callback=_handle_progress,
            progress_interval=progress_interval,
        )
        checksum = _hash_file(destination) if destination.exists() else ""
        summaries.append(
            SplitSummary(
                name=split.name,
                path=destination,
                examples=summary.written_examples,
                skipped=summary.skipped_examples,
                checksum=checksum,
                months=split.months,
            )
        )
    return summaries


def _build_year_manifest(
    year: int,
    csv_paths: Iterable[Path],
    splits: Iterable[SplitSummary],
) -> Dict[str, object]:
    return {
        "year": year,
        "generated_at": datetime.now(UTC).isoformat(),
        "source_files": [str(path.resolve()) for path in csv_paths],
        "splits": {
            summary.name: {
                "path": str(summary.path.resolve()),
                "examples": summary.examples,
                "skipped": summary.skipped,
                "checksum": summary.checksum,
                "months": list(summary.months),
            }
            for summary in splits
        },
    }


def _aggregate_split(paths: Sequence[Path], destination: Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with destination.open("w", encoding="utf-8") as handle:
        for path in paths:
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as source:
                for line in source:
                    handle.write(line)
                    if line.strip():
                        total += 1
    return total

class _ProgressPrinter:
    def __init__(self, *, base_path: Path) -> None:
        self.base_path = base_path
        self._last_report: Dict[tuple[str, Path], int] = {}

    def __call__(self, split: str, progress: ConversionProgress) -> None:
        key = (split, progress.csv_path)
        if self._last_report.get(key) == progress.processed_rows:
            return
        self._last_report[key] = progress.processed_rows

        try:
            path_repr = str(progress.csv_path.relative_to(self.base_path))
        except ValueError:
            path_repr = str(progress.csv_path)

        print(
            f"[{split}] {path_repr}: processed={progress.processed_rows:,} "
            f"written={progress.written_examples:,} skipped={progress.skipped_examples:,}"
        )


def main() -> None:
    args = _parse_args()
    data_root = Path(args.data_root)
    output_root = Path(args.output_root)
    years = _collect_years(data_root, args.years)
    spec = _load_spec(args.spec_config)

    progress_handler: Optional[_ProgressPrinter]
    if args.progress:
        progress_handler = _ProgressPrinter(base_path=data_root)
    else:
        progress_handler = None

    aggregate_sources: Dict[str, List[Path]] = {"train": [], "test_case": [], "seasonal": []}
    aggregate_counts: Dict[str, int] = {"train": 0, "test_case": 0, "seasonal": 0}

    for year in years:
        csv_paths = _collect_year_csvs(data_root, year)
        if not csv_paths:
            continue

        year_output = output_root / str(year)
        year_output.mkdir(parents=True, exist_ok=True)
        splits = _generate_splits(
            csv_paths,
            spec,
            year_output,
            progress_callback=progress_handler,
            progress_interval=args.progress_interval,
        )

        manifest = _build_year_manifest(year, csv_paths, splits)
        _write_manifest(year_output / "manifest.json", manifest)

        for summary in splits:
            aggregate_sources[summary.name].append(summary.path)
            aggregate_counts[summary.name] += summary.examples

    if not args.skip_aggregate:
        aggregate_dir = output_root / "aggregated"
        aggregate_dir.mkdir(parents=True, exist_ok=True)
        aggregate_manifest = {
            "generated_at": datetime.now(UTC).isoformat(),
            "source_years": years,
            "splits": {},
        }
        for name, paths in aggregate_sources.items():
            if not paths:
                continue
            destination = aggregate_dir / f"{name}.jsonl"
            total = _aggregate_split(paths, destination)
            aggregate_manifest["splits"][name] = {
                "path": str(destination.resolve()),
                "examples": total,
                "checksum": _hash_file(destination),
                "source_files": [str(path.resolve()) for path in paths],
            }
        _write_manifest(aggregate_dir / "manifest.json", aggregate_manifest)


if __name__ == "__main__":
    main()
