"""General helpers for ETL tasks."""

from __future__ import annotations

import csv
import gzip
import hashlib
from pathlib import Path
from typing import Iterable, Iterator, Sequence


def sha1sum(path: Path) -> str:
    digest = hashlib.sha1(usedforsecurity=False)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_csv(path: Path, *, encoding: str = "utf-8", newline: str = "") -> Iterator[dict[str, str]]:
    if path.suffix == ".gz":
        fh = gzip.open(path, mode="rt", encoding=encoding, newline=newline)
    else:
        fh = path.open("r", encoding=encoding, newline=newline)
    with fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield row


__all__ = ["sha1sum", "iter_csv"]
