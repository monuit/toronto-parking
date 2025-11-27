from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.fine_tuning.automation import DatasetBundle, EvalRequest, FineTuneAutomation, FineTuneRequest
from src.fine_tuning.run_registry import RunRegistry


class FakeClient:
    def __init__(self) -> None:
        self.uploads: list[dict[str, object]] = []
        self.jobs: list[dict[str, object]] = []
        self.eval_jobs: list[dict[str, object]] = []
        self._job_status: dict[str, str] = {}
        self._job_models: dict[str, str] = {}
        self.include_model = True

    def upload_file(self, *, file_path: Path, purpose: str) -> dict[str, object]:
        identifier = f"file-{len(self.uploads) + 1}"
        record = {
            "id": identifier,
            "purpose": purpose,
            "created_at": datetime.now(UTC).timestamp(),
            "bytes": file_path.stat().st_size,
        }
        self.uploads.append({"id": identifier, "path": str(file_path), "purpose": purpose})
        return record

    def create_fine_tune_job(self, payload: dict[str, object]) -> dict[str, object]:
        identifier = f"ft-{len(self.jobs) + 1}"
        record = {"id": identifier, "status": "queued", **payload}
        self.jobs.append(record)
        self._job_status[identifier] = "succeeded"
        self._job_models[identifier] = f"{identifier}-model"
        return record

    def retrieve_fine_tune_job(self, job_id: str) -> dict[str, object]:
        payload = {"id": job_id, "status": self._job_status.get(job_id, "queued")}
        if self.include_model and job_id in self._job_models:
            payload["fine_tuned_model"] = self._job_models[job_id]
        return payload

    def list_fine_tune_events(self, job_id: str) -> dict[str, object]:
        return {"data": []}

    def create_eval_job(self, payload: dict[str, object]) -> dict[str, object]:
        identifier = f"eval-{len(self.eval_jobs) + 1}"
        record = {"id": identifier, "status": "queued", **payload}
        self.eval_jobs.append(record)
        return record


@pytest.fixture()
def dataset_files(tmp_path: Path) -> tuple[Path, Path]:
    train = tmp_path / "tickets_train.jsonl"
    eval_path = tmp_path / "tickets_eval.jsonl"
    train.write_text('{"role": "train"}\n', encoding="utf-8")
    eval_path.write_text('{"role": "eval"}\n', encoding="utf-8")
    return train, eval_path


def test_launch_reuses_uploaded_files(tmp_path: Path, dataset_files: tuple[Path, Path]) -> None:
    registry = RunRegistry(tmp_path / "runs.json")
    client = FakeClient()
    automation = FineTuneAutomation(client, registry)
    request = FineTuneRequest(
        model="gpt-4.1-mini",
        datasets=DatasetBundle(train_path=dataset_files[0], eval_path=dataset_files[1]),
    )

    first = automation.launch(request)
    assert first.job_id == "ft-1"
    assert len(client.uploads) == 2

    second = automation.launch(request)
    assert second.job_id == "ft-2"
    # Upload count should not increase because files are cached in the registry.
    assert len(client.uploads) == 2


def test_launch_eval_records_registry(tmp_path: Path, dataset_files: tuple[Path, Path]) -> None:
    registry = RunRegistry(tmp_path / "runs.json")
    client = FakeClient()
    automation = FineTuneAutomation(client, registry)

    request = EvalRequest(model="gpt-4o-mini", dataset_path=dataset_files[1])
    result = automation.launch_eval(request)

    assert result.eval_id == "eval-1"
    assert client.eval_jobs[0]["dataset_file"].startswith("file-")
    assert registry.evals


def test_missing_dataset_raises(tmp_path: Path) -> None:
    registry = RunRegistry(tmp_path / "runs.json")
    client = FakeClient()
    automation = FineTuneAutomation(client, registry)
    request = FineTuneRequest(
        model="gpt-4.1-mini",
        datasets=DatasetBundle(train_path=tmp_path / "missing.jsonl", eval_path=tmp_path / "other.jsonl"),
    )
    with pytest.raises(FileNotFoundError):
        automation.launch(request)


def test_bulk_upload_handles_multiple_files(tmp_path: Path, dataset_files: tuple[Path, Path]) -> None:
    registry = RunRegistry(tmp_path / "runs.json")
    client = FakeClient()
    automation = FineTuneAutomation(client, registry)

    mapping = automation.bulk_upload(dataset_files)
    assert len(mapping) == 2
    assert len(client.uploads) == 2

    # Second upload should reuse cached records.
    mapping_again = automation.bulk_upload([dataset_files[0]])
    assert mapping_again[next(iter(mapping_again))].startswith("file-")
    assert len(client.uploads) == 2


def test_run_full_cycle_triggers_eval(tmp_path: Path, dataset_files: tuple[Path, Path]) -> None:
    registry = RunRegistry(tmp_path / "runs.json")
    client = FakeClient()
    automation = FineTuneAutomation(client, registry)
    request = FineTuneRequest(
        model="gpt-4.1-mini",
        datasets=DatasetBundle(train_path=dataset_files[0], eval_path=dataset_files[1]),
    )

    result = automation.run_full_cycle(request, poll_interval=0.0)

    assert result.evaluation.eval_id == "eval-1"
    assert client.eval_jobs[0]["model"] == "ft-1-model"
    assert result.fine_tune_job["fine_tuned_model"] == "ft-1-model"


def test_run_full_cycle_without_model_raises(tmp_path: Path, dataset_files: tuple[Path, Path]) -> None:
    registry = RunRegistry(tmp_path / "runs.json")
    client = FakeClient()
    client.include_model = False
    automation = FineTuneAutomation(client, registry)
    request = FineTuneRequest(
        model="gpt-4.1-mini",
        datasets=DatasetBundle(train_path=dataset_files[0], eval_path=dataset_files[1]),
    )

    with pytest.raises(RuntimeError):
        automation.run_full_cycle(request, poll_interval=0.0)
