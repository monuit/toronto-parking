"""Dataset pipeline for OpenAI fine-tuning corpora.

This module streams historical parking ticket data (2008-2024) and produces
train/eval JSONL splits tailored for OpenAI fine-tuning.  It also derives a
companion dataset approximating officer patrol priorities by aggregating ticket
hotspots per temporal slot.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from .conversion import ConversionSpec

DEFAULT_EVAL_MONTHS: Tuple[int, ...] = (3, 10, 11, 12)
DEFAULT_METADATA_FIELDS: Tuple[str, ...] = (
    "ticket_id",
    "date_iso",
    "primary_street",
    "infraction_code",
    "set_fine_amount",
    "hour",
    "month",
    "day_of_week",
)
DEFAULT_SYSTEM_PROMPT = (
    "You are a helpful analyst who predicts the correct parking infraction "
    "description from structured ticket context."
)
DEFAULT_OFFICER_SYSTEM_PROMPT = (
    "You are a patrol planner assigning officers to the most impactful "
    "parking enforcement hotspots for the upcoming shift."
)
DEFAULT_PROMPT_TEMPLATE = (
    "Location: {formatted_location}\n"
    "Primary street: {primary_street}\n"
    "Date: {date_iso}\n"
    "Day of week: {day_of_week}\n"
    "Time: {time_local}\n"
    "Infraction code: {infraction_code}\n"
    "Set fine amount: ${set_fine_amount}\n\n"
    "Respond with the official infraction description."
)
DEFAULT_COMPLETION_TEMPLATE = "{infraction_description_pretty}"


@dataclass(slots=True)
class DatasetSplitConfig:
    """Configuration controlling dataset generation."""

    start_year: int = 2008
    end_year: int = 2024
    eval_months: Tuple[int, ...] = DEFAULT_EVAL_MONTHS
    data_dir: Path = Path("parking_data/extracted")
    output_dir: Path = Path("output/fine_tuning")
    geocode_path: Optional[Path] = Path("output/geocoding_results.json")
    officer_top_k: int = 3
    dedupe_examples: bool = True
    max_examples_per_year: Optional[int] = None
    metadata_fields: Tuple[str, ...] = DEFAULT_METADATA_FIELDS

    def iter_years(self) -> Iterable[int]:
        for year in range(self.start_year, self.end_year + 1):
            yield year

    def to_serialisable(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["data_dir"] = str(self.data_dir)
        payload["output_dir"] = str(self.output_dir)
        if payload["geocode_path"] is not None:
            payload["geocode_path"] = str(payload["geocode_path"])
        return payload


@dataclass
class _TicketWriter:
    train_handle: any
    eval_handle: any
    train_seen: set | None
    eval_seen: set | None
    train_examples: int = 0
    eval_examples: int = 0


@dataclass
class _LocationAggregate:
    count: int = 0
    revenue: float = 0.0
    lat: Optional[float] = None
    lon: Optional[float] = None


@dataclass
class _OfficerAccumulator:
    top_k: int
    system_prompt: str
    train_buckets: Dict[Tuple[int, int, int], Dict[str, _LocationAggregate]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    eval_buckets: Dict[Tuple[int, int, int], Dict[str, _LocationAggregate]] = field(
        default_factory=lambda: defaultdict(dict)
    )

    def add(
        self,
        *,
        dataset: str,
        month: int,
        dow_index: int,
        hour: int,
        location_label: str,
        fine_amount: Optional[int],
        lat: Optional[float],
        lon: Optional[float],
    ) -> None:
        target = self.train_buckets if dataset == "train" else self.eval_buckets
        bucket = target[(month, dow_index, hour)]
        record = bucket.get(location_label)
        if not record:
            record = _LocationAggregate(lat=lat, lon=lon)
            bucket[location_label] = record
        record.count += 1
        if fine_amount:
            record.revenue += fine_amount
        if record.lat is None and lat is not None:
            record.lat = lat
        if record.lon is None and lon is not None:
            record.lon = lon

    def emit(self, output_dir: Path) -> Dict[str, int]:
        stats = {"train": 0, "eval": 0}
        for label, buckets in (("train", self.train_buckets), ("eval", self.eval_buckets)):
            if not buckets:
                continue
            path = output_dir / f"officer_{label}.jsonl"
            with path.open("w", encoding="utf-8") as handle:
                for (month, dow, hour), locations in sorted(buckets.items()):
                    top_locations = sorted(
                        locations.items(),
                        key=lambda item: (item[1].count, item[1].revenue),
                        reverse=True,
                    )[: self.top_k]
                    if not top_locations:
                        continue
                    total = sum(loc.count for _, loc in top_locations)
                    prompt = (
                        f"Month {month:02d}, day-of-week index {dow}, hour {hour:02d}:00.\n"
                        "Plan the next patrol using historical ticket hotspots."
                    )
                    completion_lines: List[str] = []
                    metadata_locations: List[Dict[str, object]] = []
                    for rank, (name, aggregate) in enumerate(top_locations, start=1):
                        line = f"{rank}. {name} (~{aggregate.count} tickets)"
                        if aggregate.lat is not None and aggregate.lon is not None:
                            line += f" at ({aggregate.lat:.5f}, {aggregate.lon:.5f})"
                        completion_lines.append(line)
                        metadata_locations.append(
                            {
                                "label": name,
                                "count": aggregate.count,
                                "revenue": round(aggregate.revenue, 2),
                                "lat": aggregate.lat,
                                "lon": aggregate.lon,
                            }
                        )
                    completion = "\n".join(completion_lines)
                    record = {
                        "messages": [
                            {"role": "system", "content": self.system_prompt},
                            {"role": "user", "content": prompt},
                            {"role": "assistant", "content": completion},
                        ],
                        "metadata": {
                            "month": month,
                            "dow_index": dow,
                            "hour": hour,
                            "top_locations": metadata_locations,
                            "total_hotspots": total,
                        },
                    }
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                    stats[label] += 1
        return stats


class FineTuningDatasetBuilder:
    """Build train/eval corpora for ticket prediction and officer planning."""

    def __init__(
        self,
        config: DatasetSplitConfig,
        *,
        ticket_spec: Optional[ConversionSpec] = None,
    ) -> None:
        spec = ticket_spec or ConversionSpec(
            prompt_template=DEFAULT_PROMPT_TEMPLATE,
            completion_template=DEFAULT_COMPLETION_TEMPLATE,
            system_prompt=DEFAULT_SYSTEM_PROMPT,
            metadata_fields=DEFAULT_METADATA_FIELDS,
            output_format="chat",
        )
        # Ensure the spec honours config-level dedupe preference.
        self.ticket_spec = replace(
            spec,
            dedupe_examples=config.dedupe_examples,
            metadata_fields=config.metadata_fields,
        )
        self.config = config
        self.eval_months = set(config.eval_months)
        self.officer_accumulator = _OfficerAccumulator(
            top_k=config.officer_top_k,
            system_prompt=DEFAULT_OFFICER_SYSTEM_PROMPT,
        )
        self._geocode_lookup = self._load_geocode_lookup(config.geocode_path)

    # MARK: Public API -----------------------------------------------------
    def build(self) -> Dict[str, object]:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        ticket_paths = {
            "train": self.config.output_dir / "tickets_train.jsonl",
            "eval": self.config.output_dir / "tickets_eval.jsonl",
        }

        seen_sets = {
            "train": set() if self.ticket_spec.dedupe_examples else None,
            "eval": set() if self.ticket_spec.dedupe_examples else None,
        }

        with ticket_paths["train"].open("w", encoding="utf-8") as train_handle:
            with ticket_paths["eval"].open("w", encoding="utf-8") as eval_handle:
                writer = _TicketWriter(
                    train_handle=train_handle,
                    eval_handle=eval_handle,
                    train_seen=seen_sets["train"],
                    eval_seen=seen_sets["eval"],
                )
                for year in self.config.iter_years():
                    processed = self._process_year(year, writer)
                    if processed == 0:
                        continue

        officer_stats = self.officer_accumulator.emit(self.config.output_dir)
        manifest = {
            "generated_at": datetime.now(UTC).isoformat(),
            "config": self.config.to_serialisable(),
            "ticket_examples": {
                "train": writer.train_examples,
                "eval": writer.eval_examples,
            },
            "officer_examples": officer_stats,
        }
        manifest_path = self.config.output_dir / "dataset_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest

    # MARK: Internal helpers -----------------------------------------------
    def _process_year(self, year: int, writer: _TicketWriter) -> int:
        csv_files = list(self._iter_year_files(year))
        if not csv_files:
            return 0

        examples_emitted = 0
        limit = self.config.max_examples_per_year

        for csv_path in csv_files:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    context = self._build_context(row)
                    if not context:
                        continue

                    year_value = context.get("year")
                    month_value = context.get("month")
                    if year_value != year or month_value is None:
                        continue

                    month_int = int(str(month_value).split("-")[-1])
                    dataset = "eval" if month_int in self.eval_months else "train"

                    record = self._render_record(context, dataset, writer)
                    if not record:
                        continue

                    if dataset == "train":
                        writer.train_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                        writer.train_examples += 1
                    else:
                        writer.eval_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                        writer.eval_examples += 1

                    self.officer_accumulator.add(
                        dataset=dataset,
                        month=month_int,
                        dow_index=context["dow_index"],
                        hour=context["hour"],
                        location_label=context["formatted_location"],
                        fine_amount=context.get("set_fine_amount"),
                        lat=context.get("lat"),
                        lon=context.get("lon"),
                    )

                    examples_emitted += 1
                    if limit and examples_emitted >= limit:
                        return examples_emitted
        return examples_emitted

    def _render_record(
        self,
        context: Dict[str, object],
        dataset: str,
        writer: _TicketWriter,
    ) -> Optional[Dict[str, object]]:
        prompt = self.ticket_spec.prompt_template.format_map(_SafeDict(context))
        completion = self.ticket_spec.completion_template.format_map(_SafeDict(context))

        if self.ticket_spec.lower_case_prompt:
            prompt = prompt.lower()
        if self.ticket_spec.lower_case_completion:
            completion = completion.lower()

        if dataset == "train" and writer.train_seen is not None:
            signature = (prompt, completion)
            if signature in writer.train_seen:
                return None
            writer.train_seen.add(signature)
        elif dataset == "eval" and writer.eval_seen is not None:
            signature = (prompt, completion)
            if signature in writer.eval_seen:
                return None
            writer.eval_seen.add(signature)

        record = self._build_output_record(prompt, completion, context)
        return record

    def _build_output_record(
        self,
        prompt: str,
        completion: str,
        context: Mapping[str, object],
    ) -> Dict[str, object]:
        if self.ticket_spec.output_format == "chat":
            messages: List[Dict[str, str]] = []
            if self.ticket_spec.system_prompt:
                messages.append({"role": "system", "content": self.ticket_spec.system_prompt})
            messages.append({"role": "user", "content": prompt})
            messages.append({"role": "assistant", "content": completion})
            record: Dict[str, object] = {"messages": messages}
        else:
            record = {"prompt": prompt, "completion": completion}

        if self.ticket_spec.metadata_fields:
            record[self.ticket_spec.metadata_key] = {
                field: context.get(field)
                for field in self.ticket_spec.metadata_fields
                if field in context
            }
        return record

    def _iter_year_files(self, year: int) -> Iterator[Path]:
        base = self.config.data_dir / str(year)
        if not base.exists():
            return iter(())
        # Some years use zipped archives with extracted CSV.
        csv_candidates = sorted(base.glob("*.csv"))
        if csv_candidates:
            return iter(csv_candidates)
        return iter(())

    def _build_context(self, row: MutableMapping[str, str]) -> Optional[Dict[str, object]]:
        cleaned: Dict[str, object] = {
            key: (value.strip() if isinstance(value, str) else value)
            for key, value in row.items()
        }

        ticket_date = _parse_date(cleaned.get("date_of_infraction"))
        if ticket_date is None:
            return None
        ticket_time = _parse_time(cleaned.get("time_of_infraction")) or time(12, 0)
        timestamp = datetime.combine(ticket_date, ticket_time, tzinfo=self.ticket_spec.tzinfo)

        location_label = cleaned.get("location2") or "Unknown"
        formatted_location = _format_location(cleaned)
        infraction_desc = (cleaned.get("infraction_description") or "").title()

        geocode = self._resolve_geocode(formatted_location) or {}

        context: Dict[str, object] = {
            **cleaned,
            "ticket_id": cleaned.get("tag_number_masked"),
            "date_iso": ticket_date.strftime("%Y-%m-%d"),
            "year": ticket_date.year,
            "month": ticket_date.strftime("%Y-%m"),
            "month_name": timestamp.strftime("%B"),
            "day_of_week": timestamp.strftime("%A"),
            "dow_index": timestamp.weekday(),
            "is_weekend": timestamp.weekday() >= 5,
            "time_local": ticket_time.strftime("%H:%M"),
            "hour": ticket_time.hour,
            "minute": ticket_time.minute,
            "primary_street": location_label,
            "formatted_location": formatted_location,
            "infraction_description_pretty": infraction_desc,
            "set_fine_amount": _safe_int(cleaned.get("set_fine_amount")),
            "infraction_code": cleaned.get("infraction_code"),
            "lat": geocode.get("lat"),
            "lon": geocode.get("lon"),
        }
        return context

    def _resolve_geocode(self, location: Optional[str]) -> Optional[Dict[str, float]]:
        if not location or not self._geocode_lookup:
            return None
        key = _normalise_key(location)
        return self._geocode_lookup.get(key)

    @staticmethod
    def _load_geocode_lookup(path: Optional[Path]) -> Dict[str, Dict[str, float]]:
        if not path or not path.exists():
            return {}
        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        lookup: Dict[str, Dict[str, float]] = {}
        for raw_address, data in raw_data.items():
            key = _normalise_key(raw_address)
            if not isinstance(data, Mapping):
                continue
            lat = data.get("lat")
            lon = data.get("lon")
            if lat is None or lon is None:
                continue
            lookup[key] = {
                "lat": float(lat),
                "lon": float(lon),
            }
        return lookup


class _SafeDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _parse_date(raw: Optional[object]) -> Optional[date]:
    if raw is None:
        return None
    digits = str(raw).strip()
    if len(digits) != 8:
        return None
    try:
        return datetime.strptime(digits, "%Y%m%d").date()
    except ValueError:
        return None


def _parse_time(raw: Optional[object]) -> Optional[time]:
    if raw is None:
        return None
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


def _safe_int(raw: Optional[object]) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except ValueError:
        return None


def _format_location(row: Mapping[str, object]) -> str:
    segments: List[str] = []
    location2 = str(row.get("location2") or "").strip()
    if location2:
        segments.append(location2)

    cross1 = str(row.get("location3") or "").strip()
    cross2 = str(row.get("location4") or "").strip()
    descriptor = str(row.get("location1") or "").strip()

    if cross1 and cross2:
        cross_part = f"{cross1} / {cross2}"
    else:
        cross_part = cross1 or cross2

    if cross_part:
        segments.append(cross_part)

    ignored = {"AT", "NR", "N/R", "S/O", "N/O", "E/O", "W/O"}
    if descriptor and descriptor not in ignored:
        segments.append(descriptor)

    return ", ".join(segments) if segments else "Unknown"


def _normalise_key(address: str) -> str:
    return " ".join(address.upper().split())


__all__ = [
    "DatasetSplitConfig",
    "FineTuningDatasetBuilder",
    "DEFAULT_EVAL_MONTHS",
]
