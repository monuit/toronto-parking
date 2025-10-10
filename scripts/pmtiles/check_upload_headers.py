from __future__ import annotations

import os
from pathlib import Path
import time

import requests
from dotenv import load_dotenv


def main() -> None:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    base = (os.getenv("MINIO_PUBLIC_ENDPOINT") or "").rstrip("/")
    prefix = os.getenv("PMTILES_PREFIX", "pmtiles/")
    keys = [
        "ase_ward_choropleth.pmtiles",
        "red_light_ward_choropleth.pmtiles",
        "cameras_combined_ward_choropleth.pmtiles",
    ]
    for key in keys:
        urls = [
            f"{base}/{key}",
        ]
        if prefix:
            urls.append(f"{base}/{prefix}{key}")
        for url in urls:
            response = requests.head(url, timeout=10)
            print(
                url,
                response.status_code,
                response.headers.get("Accept-Ranges"),
                response.headers.get("Cache-Control"),
            )
            if response.status_code >= 400:
                print("  body:", response.text[:120])
            else:
                start = time.perf_counter()
                get_response = requests.get(url, timeout=15)
                elapsed_ms = (time.perf_counter() - start) * 1000
                print(
                  "  download",
                  f"{elapsed_ms:.2f}ms",
                  "bytes=",
                  len(get_response.content),
                )


if __name__ == "__main__":
    main()
