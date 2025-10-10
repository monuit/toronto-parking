from __future__ import annotations

import json
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv


def main() -> None:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    endpoint = os.getenv("MINIO_PUBLIC_ENDPOINT")
    access = os.getenv("MINIO_ROOT_USER")
    secret = os.getenv("MINIO_ROOT_PASSWORD")
    region = os.getenv("MINIO_REGION", "us-east-1")
    bucket = os.getenv("PMTILES_BUCKET", "pmtiles")
    prefix = os.getenv("PMTILES_PREFIX", "pmtiles/")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name=region,
    )

    suffix = "*"
    resource_prefix = f"{prefix}{suffix}" if prefix else suffix
    object_resource = f"arn:aws:s3:::{bucket}/*"
    bucket_resource = f"arn:aws:s3:::{bucket}"

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowPublicReadObjects",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": object_resource,
            },
            {
                "Sid": "AllowBucketList",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:ListBucket"],
                "Resource": bucket_resource,
            },
        ],
    }

    client.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
    print("bucket_policy_applied")


if __name__ == "__main__":
    main()
