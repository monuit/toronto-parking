#!/usr/bin/env python3
"""
Railway Project Setup Script

Uses Railway GraphQL API to configure project services and import data.
"""

import os
import sys
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Railway API
RAILWAY_API = "https://backboard.railway.app/graphql/v2"
RAILWAY_TOKEN = "a9b69932-5252-4c8d-b338-6a685d1e2674"


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


def introspect_schema():
    """Introspect the GraphQL schema to see available queries/mutations."""
    query = """
    query {
        __schema {
            queryType {
                fields {
                    name
                    description
                    args {
                        name
                        type {
                            name
                            kind
                        }
                    }
                }
            }
            mutationType {
                fields {
                    name
                    description
                }
            }
        }
    }
    """
    return graphql_query(query)


def get_me():
    """Get current user/token info."""
    query = """
    query {
        me {
            id
            name
            email
        }
    }
    """
    return graphql_query(query)


def get_project_token_info():
    """Try to get info about the project this token is scoped to."""
    # Try different queries to discover what the token can access

    # Try getting project directly if we know the ID format
    test_queries = [
        ("me", """query { me { id name email } }"""),
        ("projects", """query {
            projects {
                edges {
                    node {
                        id
                        name
                        description
                        environments { edges { node { id name } } }
                        services { edges { node { id name } } }
                    }
                }
            }
        }"""),
    ]

    for name, query in test_queries:
        print(f"\nTrying {name}...")
        result = graphql_query(query)
        if result:
            if result.get("data") and any(v for v in result["data"].values() if v):
                print(f"Success! {name}:")
                print(json.dumps(result, indent=2))
                return result
            elif result.get("errors"):
                print(
                    f"  Error: {result['errors'][0].get('message', 'Unknown')}")

    return None


def main():
    print("=" * 60)
    print("Railway Project Setup")
    print("=" * 60)

    # to-parking project details
    PROJECT_ID = "18bce3c7-a46a-473c-9f3c-00bc32e6322b"
    ENV_ID = "8326720e-6bf2-47f3-b336-d04be106d7ec"

    SERVICES = {
        "app": "15593dae-9d71-4b15-8e39-ecc1cbee11d3",
        "postgres": "4ec76b6f-84e9-41d1-a487-abb89e517fa4",
        "postgis": "b0a63598-8b62-4ad1-9c0d-b3c182bcc731",
        "redis": "fa9f55d1-30f2-45a8-9ce3-0b5081131376",
    }

    # Get variables for each service
    print("\n1. Getting service variables...")

    for service_name, service_id in SERVICES.items():
        print(f"\n--- {service_name.upper()} ---")
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
            vars_data = result["data"].get("variables", {})
            if vars_data:
                for key, value in vars_data.items():
                    # Mask passwords
                    display_value = "***" if "PASSWORD" in key or "SECRET" in key else value
                    print(f"  {key}={display_value}")
            else:
                print("  (no variables)")


if __name__ == "__main__":
    main()
