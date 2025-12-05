#!/usr/bin/env python3
"""Check Railway service details including domains."""
import json
import requests

RAILWAY_API = "https://backboard.railway.app/graphql/v2"
RAILWAY_TOKEN = "a9b69932-5252-4c8d-b338-6a685d1e2674"
PROJECT_ID = "18bce3c7-a46a-473c-9f3c-00bc32e6322b"
APP_SERVICE_ID = "15593dae-9d71-4b15-8e39-ecc1cbee11d3"

def graphql_query(query: str, variables: dict = None) -> dict:
    headers = {
        "Authorization": f"Bearer {RAILWAY_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    response = requests.post(RAILWAY_API, headers=headers, json=payload)
    return response.json()

# Get service info
query = """
query($id: String!) {
    service(id: $id) {
        id
        name
        serviceInstances {
            edges {
                node {
                    id
                    domains {
                        customDomains {
                            id
                            domain
                        }
                        serviceDomains {
                            id
                            domain
                        }
                    }
                }
            }
        }
    }
}
"""

result = graphql_query(query, {"id": APP_SERVICE_ID})
print("Service Info:")
print("=" * 60)
print(json.dumps(result, indent=2))
