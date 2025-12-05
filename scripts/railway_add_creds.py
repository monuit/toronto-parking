#!/usr/bin/env python3
"""
Add PostGIS and Redis credentials to Railway App Service
"""

import json
import requests

# Railway API
RAILWAY_API = "https://backboard.railway.app/graphql/v2"
RAILWAY_TOKEN = "a9b69932-5252-4c8d-b338-6a685d1e2674"

# Project details
PROJECT_ID = "18bce3c7-a46a-473c-9f3c-00bc32e6322b"
ENV_ID = "8326720e-6bf2-47f3-b336-d04be106d7ec"
APP_SERVICE_ID = "15593dae-9d71-4b15-8e39-ecc1cbee11d3"

# Additional credentials to add
CREDENTIALS = {
    # PostGIS credentials (individual components)
    "PGHOST": "centerbeam.proxy.rlwy.net",
    "PGPORT": "21753",
    "PGDATABASE": "railway",
    "PGUSER": "postgres",
    "PGPASSWORD": "c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d",

    # Redis credentials (individual components)
    "REDISHOST": "maglev.proxy.rlwy.net",
    "REDISPORT": "49629",
    "REDISUSER": "default",
    "REDISPASSWORD": "ztGwRtsLNAVNTtPXvMrUCSiRPzNbxHsb",
}


def graphql_query(query: str, variables: dict = None) -> dict:
    """Execute a GraphQL query against Railway API."""
    headers = {
        "Authorization": f"Bearer {RAILWAY_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    response = requests.post(RAILWAY_API, headers=headers, json=payload)

    if response.status_code != 200:
        print(f"HTTP Error: {response.status_code}")
        print(response.text)
        return None

    result = response.json()
    if "errors" in result:
        print(f"GraphQL Errors: {json.dumps(result['errors'], indent=2)}")

    return result


def upsert_variable(name: str, value: str) -> bool:
    """Create or update an environment variable."""
    mutation = """
    mutation($input: VariableUpsertInput!) {
        variableUpsert(input: $input)
    }
    """

    result = graphql_query(mutation, {
        "input": {
            "projectId": PROJECT_ID,
            "environmentId": ENV_ID,
            "serviceId": APP_SERVICE_ID,
            "name": name,
            "value": value
        }
    })

    return result is not None and "errors" not in result


def main():
    print("=" * 60)
    print("Adding PostGIS & Redis Credentials")
    print("=" * 60)

    success = 0
    failed = 0

    for name, value in CREDENTIALS.items():
        display = "***" if "PASSWORD" in name else value
        if upsert_variable(name, value):
            print(f"  ✓ {name}={display}")
            success += 1
        else:
            print(f"  ✗ {name} - FAILED")
            failed += 1

    print("\n" + "=" * 60)
    print(f"✓ Added {success} credentials")
    if failed:
        print(f"✗ Failed: {failed}")
    print("=" * 60)


if __name__ == "__main__":
    main()
