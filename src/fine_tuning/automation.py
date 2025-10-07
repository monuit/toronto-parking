"""Automation helpers for OpenAI fine-tuning and evaluation workflows."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol

from .run_registry import FileRecord, RunRegistry


class OpenAIClient(Protocol):
    """Minimal interface required for orchestrating fine-tunes."""

    def upload_file(self, *, file_path: Path, purpose: str) -> Mapping[str, Any]:
        ...

    def create_fine_tune_job(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        ...

    def retrieve_fine_tune_job(self, job_id: str) -> Mapping[str, Any]:
        ...

    def list_fine_tune_events(self, job_id: str) -> Mapping[str, Any]:
        ...

    def create_eval_job(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        ...


@dataclass(slots=True)
class DatasetBundle:
    train_path: Path
    eval_path: Path


@dataclass(slots=True)
class FineTuneRequest:
    model: str
    datasets: DatasetBundle
    suffix: Optional[str] = None
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FineTuneResult:
    job_id: str
    status: str
    model: str
    training_file_id: str
    validation_file_id: str
    created_at: Optional[str] = None


@dataclass(slots=True)
class EvalRequest:
    model: str
    dataset_path: Path
    reference: Optional[str] = None
    metrics: List[str] = field(default_factory=lambda: ["mae", "rmse", "hotspot_rank"])
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EvalResult:
    eval_id: str
    status: str
    model: str
    dataset_file_id: str
    created_at: Optional[str] = None


@dataclass(slots=True)
class WorkflowResult:
    fine_tune: FineTuneResult
    fine_tune_job: Dict[str, Any]
    evaluation: EvalResult


class FineTuneAutomation:
    """High-level orchestrator for launching and tracking fine-tunes."""

    TERMINAL_STATUSES = {"succeeded", "cancelled", "failed"}

    def __init__(
        self,
        client: OpenAIClient,
        registry: RunRegistry,
        *,
        file_purpose: str = "fine-tune",
    ) -> None:
        self.client = client
        self.registry = registry
        self.file_purpose = file_purpose

    # MARK: public API ----------------------------------------------------
    def launch(self, request: FineTuneRequest) -> FineTuneResult:
        training_file_id = self._ensure_uploaded(request.datasets.train_path)
        validation_file_id = self._ensure_uploaded(request.datasets.eval_path)

        payload: Dict[str, Any] = {
            "training_file": training_file_id,
            "validation_file": validation_file_id,
            "model": request.model,
            **({"suffix": request.suffix} if request.suffix else {}),
        }
        if request.hyperparameters:
            payload["hyperparameters"] = request.hyperparameters

        job = dict(self.client.create_fine_tune_job(payload))
        job_id = str(job.get("id"))
        status = str(job.get("status", "unknown"))
        created_at = _to_iso8601(job.get("created_at"))

        self.registry.append_fine_tune(  # type: ignore[arg-type]
            {
                "job_id": job_id,
                "status": status,
                "model": request.model,
                "training_file_id": training_file_id,
                "validation_file_id": validation_file_id,
                "hyperparameters": request.hyperparameters,
                "metadata": request.metadata,
            }
        )

        return FineTuneResult(
            job_id=job_id,
            status=status,
            model=request.model,
            training_file_id=training_file_id,
            validation_file_id=validation_file_id,
            created_at=created_at,
        )

    def bulk_upload(self, files: Iterable[Path], *, purpose: Optional[str] = None) -> Dict[str, str]:
        uploaded: Dict[str, str] = {}
        for path in files:
            file_id = self._ensure_uploaded(path, purpose=purpose)
            uploaded[str(Path(path).resolve())] = file_id
        return uploaded

    def monitor_until_complete(
        self,
        job_id: str,
        *,
        poll_interval: float = 30.0,
        timeout: Optional[float] = None,
    ) -> Mapping[str, Any]:
        start = datetime.now(UTC)
        deadline = start + timedelta(seconds=timeout) if timeout else None

        while True:
            job = dict(self.client.retrieve_fine_tune_job(job_id))
            status = str(job.get("status", "unknown"))
            if status in self.TERMINAL_STATUSES:
                self.registry.append_fine_tune(
                    {
                        "job_id": job_id,
                        "status": status,
                        "event": "terminal",
                        "fine_tuned_model": job.get("fine_tuned_model"),
                    }
                )
                return job
            if deadline and datetime.now(UTC) >= deadline:
                raise TimeoutError(f"Fine-tune job {job_id} did not finish within timeout")
            time.sleep(poll_interval)

    def fetch_events(self, job_id: str) -> List[Mapping[str, Any]]:
        payload = self.client.list_fine_tune_events(job_id)
        events = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(events, list):
            return []
        return [dict(event) for event in events]

    def launch_eval(self, request: EvalRequest) -> EvalResult:
        dataset_file_id = self._ensure_uploaded(request.dataset_path, purpose="fine-tune-eval")
        payload = {
            "model": request.model,
            "dataset_file": dataset_file_id,
            "metrics": request.metrics,
        }
        if request.reference:
            payload["reference"] = request.reference

        job = dict(self.client.create_eval_job(payload))
        eval_id = str(job.get("id"))
        status = str(job.get("status", "unknown"))
        created_at = _to_iso8601(job.get("created_at"))

        self.registry.append_eval(  # type: ignore[arg-type]
            {
                "eval_id": eval_id,
                "status": status,
                "model": request.model,
                "dataset_file_id": dataset_file_id,
                "metrics": request.metrics,
                "metadata": request.metadata,
            }
        )

        return EvalResult(
            eval_id=eval_id,
            status=status,
            model=request.model,
            dataset_file_id=dataset_file_id,
            created_at=created_at,
        )

    def run_full_cycle(
        self,
        request: FineTuneRequest,
        *,
        eval_dataset: Optional[Path] = None,
        metrics: Optional[List[str]] = None,
        reference: Optional[str] = None,
        poll_interval: float = 30.0,
        timeout: Optional[float] = None,
    ) -> WorkflowResult:
        fine_tune = self.launch(request)
        job = self.monitor_until_complete(
            fine_tune.job_id,
            poll_interval=poll_interval,
            timeout=timeout,
        )
        model_id = _extract_model_id(job)
        if not model_id:
            raise RuntimeError("Fine-tune job completed without returning a fine-tuned model identifier")

        eval_path = eval_dataset or request.datasets.eval_path
        eval_request = EvalRequest(
            model=model_id,
            dataset_path=eval_path,
            reference=reference,
            metrics=metrics or ["mae", "rmse", "hotspot_rank"],
            metadata=request.metadata,
        )
        evaluation = self.launch_eval(eval_request)
        return WorkflowResult(fine_tune=fine_tune, fine_tune_job=dict(job), evaluation=evaluation)

    # MARK: internal helpers ---------------------------------------------
    def _ensure_uploaded(self, path: Path, *, purpose: Optional[str] = None) -> str:
        resolved = path.expanduser().resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"Dataset path does not exist: {resolved}")

        checksum = _hash_file(resolved)
        cached = self.registry.get_file(checksum)
        if cached:
            return cached.openai_file_id

        response = dict(self.client.upload_file(file_path=resolved, purpose=purpose or self.file_purpose))
        file_id = str(response.get("id"))
        uploaded_at = _to_iso8601(response.get("created_at")) or datetime.now(UTC).isoformat()
        byte_count = int(response.get("bytes", resolved.stat().st_size))

        record = FileRecord(
            checksum=checksum,
            path=str(resolved),
            purpose=purpose or self.file_purpose,
            openai_file_id=file_id,
            uploaded_at=uploaded_at,
            bytes=byte_count,
        )
        self.registry.record_file(record)
        return file_id


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _to_iso8601(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC).isoformat()
        except (OverflowError, OSError):
            return None
    return None


def _extract_model_id(job: Mapping[str, Any]) -> Optional[str]:
    candidate = job.get("fine_tuned_model")
    if candidate:
        return str(candidate)
    result = job.get("result")
    if isinstance(result, Mapping):
        model = result.get("fine_tuned_model")
        if model:
            return str(model)
    return None


class LiveOpenAIClient:
    """Thin wrapper around the official OpenAI SDK (``openai`` package)."""

    def __init__(self, *, api_key: Optional[str] = None) -> None:
        try:
            from openai import OpenAI  # type: ignore
        except ImportError as exc:  # pragma: no cover - import guard
            raise RuntimeError(
                "The 'openai' package is required for LiveOpenAIClient. Install it via 'pip install openai'."
            ) from exc
        self._client = OpenAI(api_key=api_key)

    def upload_file(self, *, file_path: Path, purpose: str) -> Mapping[str, Any]:
        with file_path.open("rb") as handle:
            result = self._client.files.create(file=handle, purpose=purpose)
        return _model_dump(result)

    def create_fine_tune_job(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        result = self._client.fine_tuning.jobs.create(**payload)
        return _model_dump(result)

    def retrieve_fine_tune_job(self, job_id: str) -> Mapping[str, Any]:
        result = self._client.fine_tuning.jobs.retrieve(job_id)
        return _model_dump(result)

    def list_fine_tune_events(self, job_id: str) -> Mapping[str, Any]:
        result = self._client.fine_tuning.jobs.list_events(fine_tuning_job_id=job_id)
        return _model_dump(result)

    def create_eval_job(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        if not hasattr(self._client, "evaluations"):
            raise RuntimeError("The installed openai SDK does not support evaluation jobs yet")
        result = self._client.evaluations.create(**payload)
        return _model_dump(result)


def _model_dump(result: Any) -> Dict[str, Any]:
    if result is None:
        return {}
    if hasattr(result, "model_dump"):
        return dict(result.model_dump())  # type: ignore[call-arg]
    if isinstance(result, Mapping):
        return dict(result)
    if hasattr(result, "__dict__"):
        return dict(result.__dict__)
    return {"id": getattr(result, "id", None)}


__all__ = [
    "OpenAIClient",
    "DatasetBundle",
    "FineTuneRequest",
    "FineTuneResult",
    "EvalRequest",
    "EvalResult",
    "FineTuneAutomation",
    "WorkflowResult",
    "LiveOpenAIClient",
]
