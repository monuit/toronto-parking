#!/usr/bin/env python3
"""
Configure Railway App Service Environment Variables

Updates the toronto-parking app service with the correct database and Redis URLs.
"""

import json
import requests

# Railway API
RAILWAY_API = "https://backboard.railway.app/graphql/v2"
RAILWAY_TOKEN = "a9b69932-5252-4c8d-b338-6a685d1e2674"

# Project details
PROJECT_ID = "18bce3c7-a46a-473c-9f3c-00bc32e6322b"
ENV_ID = "8326720e-6bf2-47f3-b336-d04be106d7ec"

# Service IDs
APP_SERVICE_ID = "15593dae-9d71-4b15-8e39-ecc1cbee11d3"
POSTGIS_SERVICE_ID = "b0a63598-8b62-4ad1-9c0d-b3c182bcc731"
REDIS_SERVICE_ID = "fa9f55d1-30f2-45a8-9ce3-0b5081131376"

# New connection URLs
NEW_DATABASE_URL = "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway"
NEW_REDIS_URL = "redis://default:ztGwRtsLNAVNTtPXvMrUCSiRPzNbxHsb@maglev.proxy.rlwy.net:49629"


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


def get_service_variables(service_id: str, service_name: str) -> dict:
    """Get current variables for a service."""
    query = """
    query($projectId: String!, $environmentId: String!, $serviceId: String!) {
        variables(projectId: $projectId, environmentId: $environmentId, serviceId: $serviceId)
    }
    """
    result = graphql_query(query, {
        "projectId": PROJECT_ID,
        "environmentId": ENV_ID,
        "serviceId": service_id
    })

    if result and result.get("data"):
        return result["data"].get("variables", {})
    return {}


def upsert_variable(service_id: str, name: str, value: str) -> bool:
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
            "serviceId": service_id,
            "name": name,
            "value": value
        }
    })

    return result is not None and "errors" not in result


def main():
    print("=" * 60)
    print("Railway App Service Configuration")
    print("=" * 60)

    # Step 1: Get current app service variables
    print("\n1. Getting current app service variables...")
    current_vars = get_service_variables(APP_SERVICE_ID, "app")

    if current_vars:
        print(f"   Found {len(current_vars)} variables")
        if "DATABASE_URL" in current_vars:
            print(
                f"   Current DATABASE_URL: {current_vars['DATABASE_URL'][:50]}...")
        if "REDIS_URL" in current_vars:
            print(f"   Current REDIS_URL: {current_vars['REDIS_URL'][:50]}...")
    else:
        print("   No variables found or error occurred")

    # Step 2: Update DATABASE_URL
    print("\n2. Updating DATABASE_URL...")
    print(f"   New value: {NEW_DATABASE_URL[:50]}...")

    if upsert_variable(APP_SERVICE_ID, "DATABASE_URL", NEW_DATABASE_URL):
        print("   ✓ DATABASE_URL updated successfully")
    else:
        print("   ✗ Failed to update DATABASE_URL")

    # Step 3: Update REDIS_URL
    print("\n3. Updating REDIS_URL...")
    print(f"   New value: {NEW_REDIS_URL[:50]}...")

    if upsert_variable(APP_SERVICE_ID, "REDIS_URL", NEW_REDIS_URL):
        print("   ✓ REDIS_URL updated successfully")
    else:
        print("   ✗ Failed to update REDIS_URL")

    # Step 4: Verify changes
    print("\n4. Verifying changes...")
    updated_vars = get_service_variables(APP_SERVICE_ID, "app")

    if updated_vars:
        db_ok = updated_vars.get("DATABASE_URL") == NEW_DATABASE_URL
        redis_ok = updated_vars.get("REDIS_URL") == NEW_REDIS_URL

        print(f"   DATABASE_URL: {'✓ Correct' if db_ok else '✗ Mismatch'}")
        print(f"   REDIS_URL: {'✓ Correct' if redis_ok else '✗ Mismatch'}")

        if db_ok and redis_ok:
            print("\n" + "=" * 60)
            print("✓ Configuration complete!")
            print("=" * 60)
            print("\nNext steps:")
            print("  1. Redeploy the app service to apply changes")
            print("  2. Test the application")
        else:
            print("\n⚠ Some variables may not have been updated correctly")
    else:
        print("   Could not verify - error fetching variables")

    # Also print all current variables for reference
    print("\n" + "-" * 60)
    print("All current app service variables:")
    print("-" * 60)
    for key, value in sorted(updated_vars.items()):
        display = "***" if any(s in key.upper()
                               for s in ["PASSWORD", "SECRET", "KEY", "TOKEN"]) else value[:80]
        print(f"  {key}={display}")


if __name__ == "__main__":
    main()
