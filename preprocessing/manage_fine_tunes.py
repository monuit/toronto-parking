"""CLI for launching and monitoring OpenAI fine-tune and evaluation jobs."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict

from src.fine_tuning.automation import (
    DatasetBundle,
    EvalRequest,
    FineTuneAutomation,
    FineTuneRequest,
    LiveOpenAIClient,
    WorkflowResult,
)
from src.fine_tuning.run_registry import RunRegistry


def _parse_hyperparameters(values: list[str]) -> Dict[str, object]:
    params: Dict[str, object] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"Invalid hyperparameter format: {item}")
        key, raw_value = item.split("=", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if raw_value.lower() in {"true", "false"}:
            params[key] = raw_value.lower() == "true"
            continue
        try:
            if "." in raw_value:
                params[key] = float(raw_value)
            else:
                params[key] = int(raw_value)
            continue
        except ValueError:
            params[key] = raw_value
    return params


def _build_automation(args: argparse.Namespace) -> FineTuneAutomation:
    registry_path = Path(args.registry)
    registry = RunRegistry(registry_path)
    api_key = args.api_key or os.getenv("OPENAI_API_KEY")
    client = LiveOpenAIClient(api_key=api_key)
    return FineTuneAutomation(client, registry)


def handle_launch(args: argparse.Namespace) -> None:
    automation = _build_automation(args)
    datasets = DatasetBundle(train_path=Path(args.train), eval_path=Path(args.eval))
    hyperparameters = _parse_hyperparameters(args.hyperparameters or [])
    metadata = {"note": args.note} if args.note else {}

    request = FineTuneRequest(
        model=args.model,
        datasets=datasets,
        suffix=args.suffix,
        hyperparameters=hyperparameters,
        metadata=metadata,
    )
    result = automation.launch(request)
    print(json.dumps(result.__dict__, indent=2))


def handle_monitor(args: argparse.Namespace) -> None:
    automation = _build_automation(args)
    job = automation.monitor_until_complete(
        args.job_id,
        poll_interval=args.poll_interval,
        timeout=args.timeout,
    )
    print(json.dumps(job, indent=2))


def handle_eval(args: argparse.Namespace) -> None:
    automation = _build_automation(args)
    request = EvalRequest(
        model=args.model,
        dataset_path=Path(args.dataset),
        reference=args.reference,
        metrics=args.metric or ["mae", "rmse", "hotspot_rank"],
        metadata={"note": args.note} if args.note else {},
    )
    result = automation.launch_eval(request)
    print(json.dumps(result.__dict__, indent=2))


def handle_bulk_upload(args: argparse.Namespace) -> None:
    automation = _build_automation(args)
    files = [Path(path) for path in args.files]
    mapping = automation.bulk_upload(files, purpose=args.purpose)
    print(json.dumps(mapping, indent=2))


def _workflow_result_to_dict(result: WorkflowResult) -> Dict[str, object]:
    return {
        "fine_tune": result.fine_tune.__dict__,
        "fine_tune_job": result.fine_tune_job,
        "evaluation": result.evaluation.__dict__,
    }


def handle_status(args: argparse.Namespace) -> None:
    registry_path = Path(args.registry)
    registry = RunRegistry(registry_path)
    payload: Dict[str, object] = {
        "registry": str(registry_path.resolve()),
        "files_uploaded": len(registry.files),
        "fine_tune_events": len(registry.fine_tunes),
        "evaluation_events": len(registry.evals),
    }
    if args.verbose or args.show_files:
        payload["files"] = list(registry.files.values())
    if args.verbose or args.show_runs:
        payload["fine_tunes"] = registry.fine_tunes
        payload["evals"] = registry.evals
    print(json.dumps(payload, indent=2))


def handle_run_full_cycle(args: argparse.Namespace) -> None:
    automation = _build_automation(args)
    datasets = DatasetBundle(train_path=Path(args.train), eval_path=Path(args.validation))
    hyperparameters = _parse_hyperparameters(args.hyperparameters or [])
    metadata = {"note": args.note} if args.note else {}

    request = FineTuneRequest(
        model=args.model,
        datasets=datasets,
        suffix=args.suffix,
        hyperparameters=hyperparameters,
        metadata=metadata,
    )

    eval_dataset = Path(args.eval_dataset) if args.eval_dataset else None
    metrics = args.metric if args.metric else None
    result = automation.run_full_cycle(
        request,
        eval_dataset=eval_dataset,
        metrics=metrics,
        reference=args.reference,
        poll_interval=args.poll_interval,
        timeout=args.timeout,
    )
    print(json.dumps(_workflow_result_to_dict(result), indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="output/fine_tuning/runs.json")
    parser.add_argument("--api-key", dest="api_key")

    subparsers = parser.add_subparsers(dest="command", required=True)

    launch = subparsers.add_parser("launch", help="Launch a fine-tune job")
    launch.add_argument("--train", required=True)
    launch.add_argument("--eval", required=True)
    launch.add_argument("--model", required=True)
    launch.add_argument("--suffix")
    launch.add_argument("--hyperparameters", nargs="*")
    launch.add_argument("--note")
    launch.set_defaults(func=handle_launch)

    monitor = subparsers.add_parser("monitor", help="Poll a fine-tune job until it finishes")
    monitor.add_argument("--job-id", required=True)
    monitor.add_argument("--poll-interval", type=float, default=30.0)
    monitor.add_argument("--timeout", type=float)
    monitor.set_defaults(func=handle_monitor)

    evaluate = subparsers.add_parser("eval", help="Launch an evaluation run")
    evaluate.add_argument("--model", required=True)
    evaluate.add_argument("--dataset", required=True)
    evaluate.add_argument("--reference")
    evaluate.add_argument("--metric", action="append")
    evaluate.add_argument("--note")
    evaluate.set_defaults(func=handle_eval)

    bulk = subparsers.add_parser("bulk-upload", help="Upload one or more dataset files")
    bulk.add_argument("files", nargs="+", help="JSONL files to upload")
    bulk.add_argument("--purpose", default="fine-tune")
    bulk.set_defaults(func=handle_bulk_upload)

    status = subparsers.add_parser("status", help="Show tracked uploads and job history")
    status.add_argument("--show-files", action="store_true", help="Include uploaded file details")
    status.add_argument("--show-runs", action="store_true", help="Include fine-tune and eval events")
    status.add_argument("--verbose", action="store_true", help="Equivalent to --show-files --show-runs")
    status.set_defaults(func=handle_status)

    run_full = subparsers.add_parser("run-full-cycle", help="Launch fine-tune then evaluation")
    run_full.add_argument("--train", required=True)
    run_full.add_argument("--validation", required=True)
    run_full.add_argument("--eval-dataset")
    run_full.add_argument("--model", required=True)
    run_full.add_argument("--suffix")
    run_full.add_argument("--hyperparameters", nargs="*")
    run_full.add_argument("--metric", action="append")
    run_full.add_argument("--reference")
    run_full.add_argument("--note")
    run_full.add_argument("--poll-interval", type=float, default=30.0)
    run_full.add_argument("--timeout", type=float)
    run_full.set_defaults(func=handle_run_full_cycle)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
