"""Autonomous downloader for the City of Toronto Automated Speed Enforcement (ASE) charges dataset.

This script performs the following steps:
1. Retrieves package metadata for the ASE charges dataset from Toronto's CKAN API.
2. Identifies the first downloadable resource (XLSX file) associated with the package.
3. Creates a local staging directory (``external_data/ase_charges``) using an explicit name.
4. Downloads the XLSX resource into that directory with the explicit filename
    ``automated-speed-enforcement.xlsx``.
5. Converts the downloaded workbook into a CSV file named
    ``automated-speed-enforcement.csv`` in the same directory.
6. Loads the workbook with pandas, reshapes it to a long format, and prints a
    clean textual sample and summary statistics to stdout.

Run this script whenever you need an up-to-date copy of the ASE charges dataset.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
PACKAGE_ENDPOINT = "/api/3/action/package_show"
DATASET_ID = "automated-speed-enforcement-ase-charges"
RESOURCE_FORMAT_PRIORITY = ("XLSX", "CSV")
OUTPUT_DIRECTORY = Path("external_data/ase_charges")
OUTPUT_XLSX_FILENAME = "automated-speed-enforcement.xlsx"
OUTPUT_CSV_FILENAME = "automated-speed-enforcement.csv"
DOWNLOAD_TIMEOUT_SECONDS = 60


class ASEDownloadError(RuntimeError):
    """Raised when the ASE charges dataset cannot be retrieved."""


@dataclass
class ResourceInfo:
    identifier: str
    name: str
    format: str
    datastore_active: bool
    url: str | None


@dataclass
class PackageInfo:
    title: str
    notes: str
    metadata_modified: str
    resources: list[ResourceInfo]


def fetch_package(dataset_id: str = DATASET_ID) -> PackageInfo:
    """Fetch the CKAN package metadata for the requested dataset."""
    response = requests.get(
        BASE_URL + PACKAGE_ENDPOINT,
        params={"id": dataset_id},
        timeout=DOWNLOAD_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise ASEDownloadError(f"Package lookup failed for dataset '{dataset_id}'.")

    result = payload["result"]
    resources = [
        ResourceInfo(
            identifier=res["id"],
            name=res.get("name", ""),
            format=res.get("format", "").upper(),
            datastore_active=res.get("datastore_active", False),
            url=res.get("url"),
        )
        for res in result.get("resources", [])
    ]
    return PackageInfo(
        title=result.get("title", dataset_id),
        notes=result.get("notes", ""),
        metadata_modified=result.get("metadata_modified", ""),
        resources=resources,
    )


def choose_resource(resources: Iterable[ResourceInfo]) -> ResourceInfo:
    """Pick the highest priority resource based on format preference."""
    sorted_resources = sorted(
        (res for res in resources if res.url),
        key=lambda res: RESOURCE_FORMAT_PRIORITY.index(res.format)
        if res.format in RESOURCE_FORMAT_PRIORITY
        else len(RESOURCE_FORMAT_PRIORITY),
    )
    if not sorted_resources:
        raise ASEDownloadError("No downloadable resources found for ASE dataset.")
    chosen = sorted_resources[0]
    if chosen.datastore_active:
        raise ASEDownloadError(
            "Selected resource is datastore_active. Expected a static download."
        )
    return chosen


def ensure_output_directory(directory: Path = OUTPUT_DIRECTORY) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def download_resource(resource: ResourceInfo, destination_dir: Path) -> Path:
    destination = destination_dir / OUTPUT_XLSX_FILENAME
    response = requests.get(resource.url, timeout=DOWNLOAD_TIMEOUT_SECONDS)
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def write_csv_version(raw_df: pd.DataFrame, destination_dir: Path) -> Path:
    destination = destination_dir / OUTPUT_CSV_FILENAME
    raw_df.to_csv(destination, index=False, encoding="utf-8")
    return destination


def reshape_dataset(raw_df: pd.DataFrame) -> pd.DataFrame:
    static_columns = [
        "Site Code",
        "Location*",
        "Ward",
        "Enforcement Start Date",
        "Enforcement End Date",
    ]
    month_columns = [col for col in raw_df.columns if col not in static_columns]
    tidy_df = raw_df.melt(
        id_vars=static_columns,
        value_vars=month_columns,
        var_name="Month",
        value_name="Tickets",
    )
    tidy_df["Tickets"] = (
        tidy_df["Tickets"]
        .astype(str)
        .str.strip()
        .replace({"-": pd.NA, "": pd.NA, "nan": pd.NA, "None": pd.NA})
    )
    tidy_df = tidy_df.dropna(subset=["Tickets"])
    tidy_df["Month"] = pd.to_datetime(tidy_df["Month"])
    tidy_df["Tickets"] = tidy_df["Tickets"].astype(int)
    tidy_df = tidy_df.sort_values(["Month", "Site Code"]).reset_index(drop=True)
    return tidy_df


def print_sample(tidy_df: pd.DataFrame, limit: int = 5) -> None:
    sample = tidy_df.head(limit)
    print("\nSample rows (long format):")
    print(sample.to_string(index=False))

    summary = {
        "rows": int(tidy_df.shape[0]),
        "sites": int(tidy_df["Site Code"].nunique()),
        "date_range": f"{tidy_df['Month'].min().date()} to {tidy_df['Month'].max().date()}",
        "median_tickets": int(tidy_df["Tickets"].median()),
    }
    print("\nSummary statistics:")
    for key, value in summary.items():
        print(f"  - {key}: {value}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download the Toronto Automated Speed Enforcement (ASE) charges dataset "
            "and print a concise sample."
        )
    )
    parser.add_argument(
        "--dataset-id",
        default=DATASET_ID,
        help="CKAN dataset identifier to retrieve (default: automated-speed-enforcement-ase-charges)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIRECTORY,
        help="Directory where the dataset will be stored (default: external_data/ase_charges)",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5,
        help="Number of sample rows to print from the reshaped dataset",
    )
    args = parser.parse_args()

    try:
        print(f"Fetching package metadata for dataset: {args.dataset_id}")
        package_info = fetch_package(args.dataset_id)
        print(f"Title: {package_info.title}")
        print(f"Metadata modified: {package_info.metadata_modified}")

        chosen_resource = choose_resource(package_info.resources)
        print(
            "Selected resource: "
            f"{chosen_resource.name or chosen_resource.identifier} ({chosen_resource.format})"
        )

        output_dir = ensure_output_directory(args.output_dir)
        dataset_path = download_resource(chosen_resource, output_dir)
        print(f"Downloaded workbook to: {dataset_path.resolve()}")

        raw_dataset = pd.read_excel(dataset_path)
        csv_path = write_csv_version(raw_dataset, output_dir)
        print(f"Exported CSV to: {csv_path.resolve()}")

        tidy_df = reshape_dataset(raw_dataset)
        print_sample(tidy_df, limit=args.sample_rows)

    except (requests.RequestException, ASEDownloadError) as exc:
        raise SystemExit(f"ASE dataset download failed: {exc}") from exc


if __name__ == "__main__":
    main()
