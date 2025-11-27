from __future__ import annotations

import json
from pathlib import Path

from src.fine_tuning.conversion import ConversionSpec, convert_csv_to_jsonl

SAMPLE_CSV = Path(__file__).parent / "data" / "sample_tickets.csv"


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_chat_conversion_basic(tmp_path):
    output_path = tmp_path / "tickets.jsonl"
    spec = ConversionSpec(
        prompt_template="Where did the ticket occur? {formatted_location}",
        completion_template="{infraction_description_pretty}",
        system_prompt="Reply with the official description.",
        metadata_fields=("ticket_id", "date_iso", "primary_street"),
    )

    summary = convert_csv_to_jsonl(
        inputs=[SAMPLE_CSV],
        output_path=output_path,
        spec=spec,
    )

    assert summary.written_examples == 3
    assert summary.skipped_examples == 0

    payloads = _read_jsonl(output_path)

    first = payloads[0]
    assert first["messages"][0]["role"] == "system"
    assert first["messages"][1]["role"] == "user"
    assert "4700 KEELE ST" in first["messages"][1]["content"]
    assert first["messages"][2]["content"] == "Park On Private Property"
    assert "metadata" not in first


def test_chat_conversion_with_opt_in_metadata(tmp_path):
    output_path = tmp_path / "tickets_with_metadata.jsonl"
    spec = ConversionSpec(
        prompt_template="Where did the ticket occur? {formatted_location}",
        completion_template="{infraction_description_pretty}",
        system_prompt="Reply with the official description.",
        metadata_fields=("ticket_id", "date_iso"),
        include_metadata_in_chat=True,
    )

    convert_csv_to_jsonl(
        inputs=[SAMPLE_CSV],
        output_path=output_path,
        spec=spec,
    )

    payloads = _read_jsonl(output_path)
    assert "metadata" in payloads[0]
    assert payloads[0]["metadata"]["ticket_id"]


def test_completion_conversion_with_dedupe(tmp_path):
    output_path = tmp_path / "tickets_completion.jsonl"
    spec = ConversionSpec(
        prompt_template="Predict the ticket description",
        completion_template="Parking Violation",
        output_format="completion",
        metadata_fields=(),
        dedupe_examples=True,
    )

    summary = convert_csv_to_jsonl(
        inputs=[SAMPLE_CSV],
        output_path=output_path,
        spec=spec,
    )

    assert summary.written_examples == 1
    assert summary.skipped_examples == 2

    payloads = _read_jsonl(output_path)
    assert payloads == [{"prompt": "Predict the ticket description", "completion": "Parking Violation"}]


def test_row_filter_limits_examples(tmp_path):
    output_path = tmp_path / "tickets_filtered.jsonl"
    spec = ConversionSpec(
        prompt_template="Predict code {infraction_code}",
        completion_template="{infraction_description_pretty}",
        metadata_fields=(),
    )

    summary = convert_csv_to_jsonl(
        inputs=[SAMPLE_CSV],
        output_path=output_path,
        spec=spec,
        row_filter=lambda row: row.get("infraction_code") == "207",
    )

    assert summary.written_examples == 1
    payloads = _read_jsonl(output_path)
    assert payloads[0]["messages"][0]["content"] == "Predict code 207"


def test_progress_callback_reports_updates(tmp_path):
    output_path = tmp_path / "tickets_progress.jsonl"
    spec = ConversionSpec(
        prompt_template="Predict code {infraction_code}",
        completion_template="{infraction_description_pretty}",
        metadata_fields=(),
    )

    processed = []

    def _collector(event):
        processed.append((event.csv_path, event.processed_rows, event.written_examples, event.skipped_examples))

    summary = convert_csv_to_jsonl(
        inputs=[SAMPLE_CSV],
        output_path=output_path,
        spec=spec,
        progress_callback=_collector,
        progress_interval=1,
    )

    assert summary.written_examples == 3
    assert processed, "Expected at least one progress event"
    last_event = processed[-1]
    assert last_event[1] >= summary.written_examples
    assert last_event[2] == summary.written_examples
