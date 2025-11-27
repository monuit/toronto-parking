from __future__ import annotations

import csv
import json
from pathlib import Path

from src.fine_tuning.dataset_builder import DatasetSplitConfig, FineTuningDatasetBuilder


def _write_sample_csv(path: Path) -> None:
    rows = [
        {
            "date_of_infraction": "20100305",
            "time_of_infraction": "0930",
            "location1": "NR",
            "location2": "123 MAIN ST",
            "location3": "",
            "location4": "",
            "infraction_description": "PARK ON SIDEWALK",
            "infraction_code": "A01",
            "set_fine_amount": "30",
            "tag_number_masked": "TAG001",
        },
        {
            "date_of_infraction": "20100507",
            "time_of_infraction": "1100",
            "location1": "",
            "location2": "456 KING ST",
            "location3": "",
            "location4": "",
            "infraction_description": "EXPIRED METER",
            "infraction_code": "B02",
            "set_fine_amount": "40",
            "tag_number_masked": "TAG002",
        },
        {
            "date_of_infraction": "20101007",
            "time_of_infraction": "1400",
            "location1": "",
            "location2": "789 QUEEN ST",
            "location3": "YONGE ST",
            "location4": "",
            "infraction_description": "NO PARKING",
            "infraction_code": "C03",
            "set_fine_amount": "50",
            "tag_number_masked": "TAG003",
        },
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_dataset_builder_creates_expected_splits(tmp_path):
    data_dir = tmp_path / "data"
    year_dir = data_dir / "2010"
    year_dir.mkdir(parents=True)
    csv_path = year_dir / "Parking_Tags_Data_2010.csv"
    _write_sample_csv(csv_path)

    geocode_lookup = {
        "123 MAIN ST": {"lat": 43.7, "lon": -79.4},
        "456 KING ST": {"lat": 43.65, "lon": -79.38},
        "789 QUEEN ST, YONGE ST": {"lat": 43.66, "lon": -79.39},
    }
    geocode_path = tmp_path / "geocodes.json"
    geocode_path.write_text(json.dumps(geocode_lookup), encoding="utf-8")

    output_dir = tmp_path / "out"
    config = DatasetSplitConfig(
        start_year=2010,
        end_year=2010,
        data_dir=data_dir,
        output_dir=output_dir,
        geocode_path=geocode_path,
        officer_top_k=2,
    )
    builder = FineTuningDatasetBuilder(config)
    manifest = builder.build()

    assert manifest["ticket_examples"]["train"] == 1
    assert manifest["ticket_examples"]["eval"] == 2
    assert (output_dir / "dataset_manifest.json").exists()

    train_path = output_dir / "tickets_train.jsonl"
    eval_path = output_dir / "tickets_eval.jsonl"
    officer_eval_path = output_dir / "officer_eval.jsonl"

    train_records = train_path.read_text(encoding="utf-8").strip().splitlines()
    eval_records = eval_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(train_records) == 1
    assert len(eval_records) == 2

    eval_payload = json.loads(eval_records[0])
    assert eval_payload["messages"][1]["role"] == "user"
    assert eval_payload["messages"][2]["role"] == "assistant"

    officer_records = officer_eval_path.read_text(encoding="utf-8").strip().splitlines()
    assert officer_records, "Officer eval dataset should be populated"
    officer_payload = json.loads(officer_records[0])
    metadata_locations = officer_payload["metadata"]["top_locations"]
    assert metadata_locations[0]["label"].startswith("123") or metadata_locations[0]["label"].startswith("789")
    assert "~" in officer_payload["messages"][2]["content"]