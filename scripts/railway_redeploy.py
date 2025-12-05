#!/usr/bin/env python3
"""
Trigger Railway App Service Redeploy

Triggers a new deployment of the toronto-parking app service.
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


def get_latest_deployment():
    """Get the latest deployment for the app service."""
    query = """
    query($projectId: String!, $serviceId: String!) {
        deployments(
            first: 1
            input: {
                projectId: $projectId
                serviceId: $serviceId
            }
        ) {
            edges {
                node {
                    id
                    status
                    createdAt
                }
            }
        }
    }
    """
    result = graphql_query(query, {
        "projectId": PROJECT_ID,
        "serviceId": APP_SERVICE_ID
    })

    if result and result.get("data", {}).get("deployments", {}).get("edges"):
        return result["data"]["deployments"]["edges"][0]["node"]
    return None


def trigger_redeploy():
    """Trigger a redeploy using serviceInstanceRedeploy mutation."""
    mutation = """
    mutation($serviceId: String!, $environmentId: String!) {
        serviceInstanceRedeploy(serviceId: $serviceId, environmentId: $environmentId)
    }
    """

    result = graphql_query(mutation, {
        "serviceId": APP_SERVICE_ID,
        "environmentId": ENV_ID
    })

    return result is not None and "errors" not in result


def main():
    print("=" * 60)
    print("Railway App Service - Trigger Redeploy")
    print("=" * 60)

    # Get current deployment status
    print("\n1. Checking current deployment...")
    deployment = get_latest_deployment()
    if deployment:
        print(f"   Latest deployment: {deployment['id'][:20]}...")
        print(f"   Status: {deployment['status']}")
        print(f"   Created: {deployment['createdAt']}")
    else:
        print("   No deployments found")

    # Trigger redeploy
    print("\n2. Triggering redeploy...")
    if trigger_redeploy():
        print("   ✓ Redeploy triggered successfully!")
    else:
        print("   ✗ Failed to trigger redeploy")
        print("   Note: You may need to redeploy manually from the Railway dashboard")
        return

    # Check new deployment
    print("\n3. Checking new deployment...")
    import time
    time.sleep(2)  # Wait a moment for the deployment to be created

    new_deployment = get_latest_deployment()
    if new_deployment:
        print(f"   New deployment: {new_deployment['id'][:20]}...")
        print(f"   Status: {new_deployment['status']}")
        print(f"   Created: {new_deployment['createdAt']}")

    print("\n" + "=" * 60)
    print("✓ Redeploy initiated!")
    print("=" * 60)
    print("\nThe deployment is now in progress. You can monitor it at:")
    print("  https://railway.app/project/18bce3c7-a46a-473c-9f3c-00bc32e6322b")


if __name__ == "__main__":
    main()
