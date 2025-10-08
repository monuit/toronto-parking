"""Command line interface for the ETL pipeline."""

from __future__ import annotations

import argparse
import importlib
import logging
from typing import Iterable, Sequence, Type

from .config import DatasetConfig, ETLConfig
from .ckan import CKANClient
from .postgres import PostgresClient
from .state import ETLStateStore
from .storage import ArtefactStore


LOGGER = logging.getLogger(__name__)


def _load_handler(path: str):
    module_name, class_name = path.split(":")
    module = importlib.import_module(module_name)
    handler_cls = getattr(module, class_name)
    return handler_cls


def _run_dataset(
    dataset: DatasetConfig,
    *,
    ckan: CKANClient,
    store: ArtefactStore,
    pg: PostgresClient,
    state_store: ETLStateStore,
) -> None:
    handler_cls = _load_handler(dataset.handler)
    handler = handler_cls(
        dataset,
        ckan=ckan,
        store=store,
        pg=pg,
        state_store=state_store,
    )
    LOGGER.info("Running ETL for %s", dataset.slug)
    handler.run()


def run_pipeline(selected: Sequence[str] | None = None) -> None:
    config = ETLConfig.default()
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    pg = PostgresClient(
        config.database.dsn,
        application_name=config.database.application_name,
        connect_timeout=config.database.connect_timeout,
        statement_timeout_ms=config.database.statement_timeout_ms,
    )
    store = ArtefactStore(config.storage.raw_root, config.storage.staging_root)
    ckan = CKANClient(config.base_url, user_agent=config.user_agent)
    state_store = ETLStateStore(pg)

    try:
        for dataset in config.datasets:
            if selected and dataset.slug not in selected:
                continue
            _run_dataset(dataset, ckan=ckan, store=store, pg=pg, state_store=state_store)
    finally:
        ckan.close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Toronto Parking ETL pipeline")
    parser.add_argument(
        "--datasets",
        nargs="*",
        help="Run a subset of datasets by slug",
    )
    args = parser.parse_args(argv)
    run_pipeline(args.datasets)


if __name__ == "__main__":  # pragma: no cover
    main()
