#!/usr/bin/env python3
"""
Configure all Railway App Service Environment Variables

Updates the toronto-parking app service with all necessary environment variables.
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

# All environment variables to set
ENV_VARS = {
    # Database
    "DATABASE_URL": "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway",

    # Redis
    "REDIS_URL": "redis://default:ztGwRtsLNAVNTtPXvMrUCSiRPzNbxHsb@maglev.proxy.rlwy.net:49629",

    # Cache settings
    "CACHE_TTL_S": "86400",
    "GLOW_TILE_CACHE_TTL": "86400",
    "GLOW_TILE_CACHE_VERSION": "v1",

    # Map/Redis namespacing
    "MAP_DATA_REDIS_NAMESPACE": "toronto:map-data",
    "MAP_TILE_PREWARM": "1",
    "MAP_TILE_REDIS_MAX_BYTES": "2000000",
    "MAP_TILE_REDIS_TTL": "86400",

    # MapLibre/MapTiler
    "MAPLIBRE_API_KEY": "jCjbWSQnQEcKQqwab93w",
    "MAPTILER_PROXY_BACKOFF_MS": "500",
    "MAPTILER_PROXY_FALLBACK_TIMEOUT_MS": "20000",
    "MAPTILER_PROXY_MAX_RETRIES": "2",
    "MAPTILER_PROXY_MODE": "direct",
    "MAPTILER_PROXY_PATH": "/api/maptiler",
    "MAPTILER_PROXY_TIMEOUT_MS": "12000",

    # Node
    "NODE_ENV": "production",

    # PMTiles (disabled for now)
    "PMTILES_ENABLED": "false",
    "PMTILES_PREFIX": "pmtiles",

    # SQL timeout
    "SQL_STATEMENT_TIMEOUT_MS": "250",

    # Vite frontend config
    "VITE_MAPTILER_PROXY_PATH": "/api/maptiler",
    "VITE_PM_TILES_PREFIX": "pmtiles",
    "VITE_TILES_BASE_URL": "/tiles",
    "VITE_TILES_MODE": "mvt",
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


def get_service_variables() -> dict:
    """Get current variables for the app service."""
    query = """
    query($projectId: String!, $environmentId: String!, $serviceId: String!) {
        variables(projectId: $projectId, environmentId: $environmentId, serviceId: $serviceId)
    }
    """
    result = graphql_query(query, {
        "projectId": PROJECT_ID,
        "environmentId": ENV_ID,
        "serviceId": APP_SERVICE_ID
    })

    if result and result.get("data"):
        return result["data"].get("variables", {})
    return {}


def main():
    print("=" * 60)
    print("Railway App Service - Full Configuration")
    print("=" * 60)

    # Get current variables
    print("\n1. Getting current variables...")
    current = get_service_variables()
    print(f"   Current count: {len(current)}")

    # Set all variables
    print(f"\n2. Setting {len(ENV_VARS)} environment variables...")

    success = 0
    failed = 0

    for name, value in ENV_VARS.items():
        current_val = current.get(name)
        if current_val == value:
            print(f"   {name}: (unchanged)")
            success += 1
        else:
            if upsert_variable(name, value):
                status = "updated" if current_val else "created"
                print(f"   {name}: ✓ {status}")
                success += 1
            else:
                print(f"   {name}: ✗ failed")
                failed += 1

    # Verify
    print(f"\n3. Verifying...")
    final = get_service_variables()

    mismatches = []
    for name, expected in ENV_VARS.items():
        actual = final.get(name)
        if actual != expected:
            mismatches.append(name)

    print(f"\n" + "=" * 60)
    if not mismatches:
        print(f"✓ All {len(ENV_VARS)} variables configured successfully!")
    else:
        print(f"⚠ {len(mismatches)} variables have mismatches: {mismatches}")
    print("=" * 60)

    # Summary
    print(f"\nFinal variable count: {len(final)}")
    print("\nAll variables:")
    print("-" * 60)
    for key in sorted(final.keys()):
        value = final[key]
        if any(s in key.upper() for s in ["PASSWORD", "SECRET", "KEY", "TOKEN", "URL"]):
            display = value[:40] + "..." if len(value) > 40 else value
        else:
            display = value
        print(f"  {key}={display}")

    print("\n" + "=" * 60)
    print("Next: Redeploy the app service to apply changes")
    print("=" * 60)


if __name__ == "__main__":
    main()
