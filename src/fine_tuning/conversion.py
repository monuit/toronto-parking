"""Utilities for converting parking ticket CSV exports into JSONL suitable for
language-model fine-tuning.

The converter is intentionally configurable so that teams can experiment with
prompt/completion definitions without duplicating parsing logic.  It understands
Toronto's parking ticket schema (``Parking_Tags_Data_YYYY.csv``) and exposes a
small CLI for day-to-day use.
"""

from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Sequence
from typing import Tuple
from zoneinfo import ZoneInfo

DEFAULT_TIME_FALLBACK = time(12, 0)
DEFAULT_TIME_FORMAT = "%H:%M"
DEFAULT_DATE_FORMAT = "%Y-%m-%d"
DEFAULT_MONTH_FORMAT = "%Y-%m"
TORONTO_TIMEZONE = "America/Toronto"

_MAX_CSV_FIELD_SIZE = min(getattr(sys, "maxsize", 25_000_000), 25_000_000)
csv.field_size_limit(_MAX_CSV_FIELD_SIZE)


class _SafeDict(dict):
    """Dictionary that leaves template placeholders untouched when missing."""

    def __missing__(self, key: str) -> str:  # pragma: no cover - trivial
        return "{" + key + "}"


@dataclass
class ConversionSpec:
    """Configuration describing how to map CSV rows into JSONL records."""

    prompt_template: str
    completion_template: str
    system_prompt: Optional[str] = None
    output_format: str = "chat"  # ``chat`` (messages) or ``completion``
    metadata_fields: Sequence[str] = field(default_factory=tuple)
    drop_if_missing: Sequence[str] = field(default_factory=lambda: ("infraction_description", "location2"))
    metadata_key: str = "metadata"
    timezone: str = TORONTO_TIMEZONE
    dedupe_examples: bool = False
    lower_case_prompt: bool = False
    lower_case_completion: bool = False
    include_metadata_in_chat: bool = False

    def __post_init__(self) -> None:
        if self.output_format not in {"chat", "completion"}:
            raise ValueError("output_format must be 'chat' or 'completion'")

    @cached_property
    def tzinfo(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


@dataclass
class ConversionSummary:
    """Represents aggregate statistics from a conversion run."""

    input_files: List[Path]
    output_path: Path
    written_examples: int
    skipped_examples: int

    def to_dict(self) -> Dict[str, Any]:  # pragma: no cover - convenience
        return {
            "input_files": [str(path) for path in self.input_files],
            "output_path": str(self.output_path),
            "written_examples": self.written_examples,
            "skipped_examples": self.skipped_examples,
        }


@dataclass
class ConversionProgress:
    """Represents incremental progress while converting a single CSV file."""

    csv_path: Path
    processed_rows: int
    written_examples: int
    skipped_examples: int


def convert_csv_to_jsonl(
    inputs: Iterable[str | Path],
    output_path: str | Path,
    spec: ConversionSpec,
    *,
    limit: Optional[int] = None,
    row_filter: Optional[Callable[[Mapping[str, Any]], bool]] = None,
    progress_callback: Optional[Callable[[ConversionProgress], None]] = None,
    progress_interval: int = 5000,
) -> ConversionSummary:
    """Convert one or more CSV files into a JSONL data set.

    Parameters
    ----------
    inputs:
        Iterable containing CSV file paths, directories, or glob patterns.
    output_path:
        Location for the generated JSONL file.  The parent directory will be
        created automatically if required.
    spec:
        :class:`ConversionSpec` describing templates and metadata behaviour.
    limit:
        Optional maximum number of records to write.  Useful for smoke tests.
    """

    resolved_inputs = _resolve_inputs(inputs)
    if not resolved_inputs:
        raise FileNotFoundError("No CSV files found for conversion inputs")

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    seen_examples: set[Tuple[str, str]] = set()
    written = 0
    skipped = 0

    if progress_interval <= 0:
        progress_interval = 1

    with output.open("w", encoding="utf-8") as handle:
        for csv_path in resolved_inputs:
            processed_in_file = 0
            written_in_file = 0
            skipped_in_file = 0

            for payload in _iter_csv_rows(csv_path):
                processed_in_file += 1

                if row_filter and not row_filter(payload):
                    if progress_callback and processed_in_file % progress_interval == 0:
                        progress_callback(
                            ConversionProgress(
                                csv_path=csv_path,
                                processed_rows=processed_in_file,
                                written_examples=written_in_file,
                                skipped_examples=skipped_in_file,
                            )
                        )
                    continue
                if _should_skip(payload, spec):
                    skipped += 1
                    skipped_in_file += 1
                    if progress_callback and processed_in_file % progress_interval == 0:
                        progress_callback(
                            ConversionProgress(
                                csv_path=csv_path,
                                processed_rows=processed_in_file,
                                written_examples=written_in_file,
                                skipped_examples=skipped_in_file,
                            )
                        )
                    continue

                context = _build_context(payload, spec)
                if not context:
                    skipped += 1
                    skipped_in_file += 1
                    if progress_callback and processed_in_file % progress_interval == 0:
                        progress_callback(
                            ConversionProgress(
                                csv_path=csv_path,
                                processed_rows=processed_in_file,
                                written_examples=written_in_file,
                                skipped_examples=skipped_in_file,
                            )
                        )
                    continue

                prompt = spec.prompt_template.format_map(_SafeDict(context))
                completion = spec.completion_template.format_map(_SafeDict(context))

                if spec.lower_case_prompt:
                    prompt = prompt.lower()
                if spec.lower_case_completion:
                    completion = completion.lower()

                if spec.dedupe_examples:
                    signature = (prompt, completion)
                    if signature in seen_examples:
                        skipped += 1
                        continue
                    seen_examples.add(signature)

                record = _build_output_record(spec, prompt, completion, context)
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                written += 1
                written_in_file += 1

                if progress_callback and processed_in_file % progress_interval == 0:
                    progress_callback(
                        ConversionProgress(
                            csv_path=csv_path,
                            processed_rows=processed_in_file,
                            written_examples=written_in_file,
                            skipped_examples=skipped_in_file,
                        )
                    )

                if limit is not None and written >= limit:
                    if progress_callback:
                        progress_callback(
                            ConversionProgress(
                                csv_path=csv_path,
                                processed_rows=processed_in_file,
                                written_examples=written_in_file,
                                skipped_examples=skipped_in_file,
                            )
                        )
                    break
            if limit is not None and written >= limit:
                break

            if progress_callback:
                progress_callback(
                    ConversionProgress(
                        csv_path=csv_path,
                        processed_rows=processed_in_file,
                        written_examples=written_in_file,
                        skipped_examples=skipped_in_file,
                    )
                )

    return ConversionSummary(
        input_files=resolved_inputs,
        output_path=output,
        written_examples=written,
        skipped_examples=skipped,
    )


def _matches_ticket_file(path: Path) -> bool:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return True
    if suffix.startswith(".") and suffix.lstrip(".").isdigit():
        return path.stem.lower().startswith("parking_tags_data")
    return False


def _resolve_inputs(inputs: Iterable[str | Path]) -> List[Path]:
    """Resolve directories and glob patterns into a sorted file list."""

    paths: List[Path] = []
    for entry in inputs:
        raw = Path(entry)
        if raw.is_dir():
            for match in sorted(p for p in raw.rglob("*") if p.is_file() and _matches_ticket_file(p)):
                paths.append(match)
            continue

        # Glob expansion (supports wildcards in filenames)
        if any(ch in str(raw) for ch in "*?[]"):
            for match in raw.parent.glob(raw.name):
                if match.is_file() and _matches_ticket_file(match):
                    paths.append(match)
            continue

        if raw.is_file() and _matches_ticket_file(raw):
            paths.append(raw)

    # Deduplicate while preserving order
    seen: set[Path] = set()
    unique_paths: List[Path] = []
    for path in paths:
        if path not in seen:
            unique_paths.append(path)
            seen.add(path)
    return unique_paths


def _iter_csv_rows(csv_path: Path) -> Iterator[MutableMapping[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def _should_skip(row: Mapping[str, Any], spec: ConversionSpec) -> bool:
    for field in spec.drop_if_missing:
        value = row.get(field)
        if value is None or str(value).strip() == "":
            return True
    return False


def _build_output_record(
    spec: ConversionSpec,
    prompt: str,
    completion: str,
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    if spec.output_format == "chat":
        messages: List[Dict[str, str]] = []
        if spec.system_prompt:
            messages.append({"role": "system", "content": spec.system_prompt})
        messages.append({"role": "user", "content": prompt})
        messages.append({"role": "assistant", "content": completion})
        record: Dict[str, Any] = {"messages": messages}
    else:
        record = {"prompt": prompt, "completion": completion}

    if spec.metadata_fields:
        metadata_payload = {
            field: context.get(field)
            for field in spec.metadata_fields
            if field in context and context.get(field) is not None
        }
        if metadata_payload:
            if spec.output_format == "chat":
                if spec.include_metadata_in_chat:
                    record[spec.metadata_key] = metadata_payload
            else:
                record[spec.metadata_key] = metadata_payload
    return record


def _build_context(row: MutableMapping[str, str], spec: ConversionSpec) -> Optional[Dict[str, Any]]:
    """Compose an enriched context dictionary for template rendering."""

    cleaned: Dict[str, Any] = {
        key: (value.strip() if isinstance(value, str) else value)
        for key, value in row.items()
    }

    ticket_date = _parse_date(cleaned.get("date_of_infraction"))
    if ticket_date is None:
        return None

    ticket_time = _parse_time(cleaned.get("time_of_infraction")) or DEFAULT_TIME_FALLBACK
    timestamp = datetime.combine(ticket_date, ticket_time, tzinfo=spec.tzinfo)

    primary_street = cleaned.get("location2") or "Unknown"
    formatted_location = _format_location(cleaned)
    infraction_desc = cleaned.get("infraction_description", "").title()

    context: Dict[str, Any] = {
        **cleaned,
        "ticket_id": cleaned.get("tag_number_masked"),
        "date_iso": ticket_date.strftime(DEFAULT_DATE_FORMAT),
        "year": ticket_date.year,
        "month": ticket_date.strftime(DEFAULT_MONTH_FORMAT),
        "month_name": timestamp.strftime("%B"),
        "day_of_week": timestamp.strftime("%A"),
        "dow_index": timestamp.weekday(),
        "is_weekend": timestamp.weekday() >= 5,
        "time_local": ticket_time.strftime(DEFAULT_TIME_FORMAT),
        "hour": ticket_time.hour,
        "minute": ticket_time.minute,
        "primary_street": primary_street,
        "formatted_location": formatted_location,
        "infraction_description_pretty": infraction_desc,
        "set_fine_amount": _safe_int(cleaned.get("set_fine_amount")),
        "infraction_code": cleaned.get("infraction_code"),
    }

    return context


def _parse_date(raw: Optional[str]) -> Optional[date]:
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError:
        return None


def _parse_time(raw: Optional[str]) -> Optional[time]:
    if not raw:
        return None
    # Some files have values like ``930`` or ``0930`` or ``24:00`` sentinel ``2400``.
    digits = str(raw).strip()
    if digits == "2400":
        digits = "0000"
    digits = digits.zfill(4)
    try:
        hour = int(digits[:2])
        minute = int(digits[2:])
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return time(hour=hour, minute=minute)


def _safe_int(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except ValueError:
        return None


def _format_location(row: Mapping[str, Any]) -> str:
    segments: List[str] = []
    location2 = (row.get("location2") or "").strip()
    if location2:
        segments.append(location2)

    cross1 = (row.get("location3") or "").strip()
    cross2 = (row.get("location4") or "").strip()
    descriptor = (row.get("location1") or "").strip()

    if cross1 and cross2:
        cross_part = f"{cross1} / {cross2}"
    else:
        cross_part = cross1 or cross2

    if cross_part:
        segments.append(cross_part)

    if descriptor and descriptor not in {"AT", "NR", "N/R", "S/O", "N/O", "E/O", "W/O"}:
        segments.append(descriptor)

    return ", ".join(segments) if segments else "Unknown"


__all__ = [
    "ConversionSpec",
    "ConversionSummary",
    "ConversionProgress",
    "convert_csv_to_jsonl",
]
