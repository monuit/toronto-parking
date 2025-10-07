"""Command line interface for ``fine_tuning`` utilities."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .conversion import ConversionSpec, convert_csv_to_jsonl

DEFAULT_SYSTEM_PROMPT = (
    "You are a helpful analyst who predicts the correct parking infraction "
    "description from structured ticket context."
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
DEFAULT_METADATA_FIELDS = (
    "ticket_id",
    "date_iso",
    "primary_street",
    "infraction_code",
    "set_fine_amount",
)


def build_spec_from_args(args: argparse.Namespace) -> ConversionSpec:
    config_payload: Dict[str, Any] = {}
    if args.config:
        config_payload = _load_config(args.config)

    prompt_template = args.prompt_template or config_payload.get(
        "prompt_template", DEFAULT_PROMPT_TEMPLATE
    )
    completion_template = args.completion_template or config_payload.get(
        "completion_template", DEFAULT_COMPLETION_TEMPLATE
    )
    system_prompt = args.system_prompt or config_payload.get(
        "system_prompt", DEFAULT_SYSTEM_PROMPT
    )
    metadata_fields: Iterable[str]
    if args.metadata_field:
        metadata_fields = args.metadata_field
    else:
        metadata_fields = config_payload.get("metadata_fields", DEFAULT_METADATA_FIELDS)

    drop_if_missing: List[str]
    if args.drop_missing:
        drop_if_missing = args.drop_missing
    else:
        drop_if_missing = config_payload.get("drop_if_missing", [])

    spec_kwargs = {
        "prompt_template": prompt_template,
        "completion_template": completion_template,
        "system_prompt": system_prompt,
        "output_format": args.format or config_payload.get("output_format", "chat"),
        "metadata_fields": tuple(metadata_fields),
        "drop_if_missing": tuple(drop_if_missing) if drop_if_missing else (),
        "metadata_key": config_payload.get("metadata_key", "metadata"),
        "timezone": config_payload.get("timezone", "America/Toronto"),
        "dedupe_examples": args.dedupe or config_payload.get("dedupe_examples", False),
        "lower_case_prompt": args.lowercase_prompt
        or config_payload.get("lower_case_prompt", False),
        "lower_case_completion": args.lowercase_completion
        or config_payload.get("lower_case_completion", False),
        "include_metadata_in_chat": config_payload.get("include_metadata_in_chat", False),
    }

    return ConversionSpec(**spec_kwargs)


def _load_config(path: str | Path) -> Dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    try:
        return json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Failed to parse config JSON: {config_path}") from exc


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Convert Parking Tags CSV exports into OpenAI-friendly JSONL",
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="CSV files, directories, or glob patterns to include",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Destination JSONL file",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=("chat", "completion"),
        default="chat",
        help="Output JSONL format",
    )
    parser.add_argument("--system-prompt", help="Override the default system prompt")
    parser.add_argument("--prompt-template", help="Custom prompt template string")
    parser.add_argument("--completion-template", help="Custom completion template string")
    parser.add_argument(
        "--metadata-field",
        action="append",
        help="Additional fields to copy into the metadata object",
    )
    parser.add_argument(
        "--drop-missing",
        action="append",
        help="Skip records that do not have the specified column",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of examples to emit (useful for smoke tests)",
    )
    parser.add_argument(
        "--dedupe",
        action="store_true",
        help="Drop duplicate prompt/completion pairs",
    )
    parser.add_argument(
        "--lowercase-prompt",
        action="store_true",
        help="Lower-case the rendered prompt before writing",
    )
    parser.add_argument(
        "--lowercase-completion",
        action="store_true",
        help="Lower-case the rendered completion before writing",
    )
    parser.add_argument(
        "--config",
        help="Optional JSON configuration file describing advanced settings",
    )

    args = parser.parse_args(argv)
    spec = build_spec_from_args(args)
    summary = convert_csv_to_jsonl(
        inputs=args.inputs,
        output_path=args.output,
        spec=spec,
        limit=args.limit,
    )

    print(json.dumps(summary.to_dict(), indent=2))


if __name__ == "__main__":  # pragma: no cover
    main()
