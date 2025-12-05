#!/usr/bin/env python3
"""Re-provision custom domain SSL certificate."""
import json
import requests
import time

RAILWAY_API = "https://backboard.railway.app/graphql/v2"
RAILWAY_TOKEN = "a9b69932-5252-4c8d-b338-6a685d1e2674"
PROJECT_ID = "18bce3c7-a46a-473c-9f3c-00bc32e6322b"
ENV_ID = "8326720e-6bf2-47f3-b336-d04be106d7ec"
APP_SERVICE_ID = "15593dae-9d71-4b15-8e39-ecc1cbee11d3"
DOMAIN_ID = "02edb3f7-4993-47e2-823a-463a593e111d"
CUSTOM_DOMAIN = "to-parking.monuit.dev"

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

print("=" * 60)
print("Re-provisioning SSL Certificate for Custom Domain")
print("=" * 60)

# Step 1: Delete existing domain
print(f"\n1. Deleting existing domain: {CUSTOM_DOMAIN}...")
delete_mutation = """
mutation($id: String!) {
    customDomainDelete(id: $id)
}
"""
result = graphql_query(delete_mutation, {"id": DOMAIN_ID})
if result.get("errors"):
    print(f"   Error: {result['errors']}")
else:
    print("   ✓ Domain deleted")

# Wait a moment
print("\n2. Waiting 5 seconds...")
time.sleep(5)

# Step 2: Re-add domain
print(f"\n3. Re-adding domain: {CUSTOM_DOMAIN}...")
create_mutation = """
mutation($input: CustomDomainCreateInput!) {
    customDomainCreate(input: $input) {
        id
        domain
    }
}
"""
result = graphql_query(create_mutation, {
    "input": {
        "projectId": PROJECT_ID,
        "environmentId": ENV_ID,
        "serviceId": APP_SERVICE_ID,
        "domain": CUSTOM_DOMAIN
    }
})

if result.get("errors"):
    print(f"   Error: {result['errors']}")
else:
    data = result.get("data", {}).get("customDomainCreate", {})
    print(f"   ✓ Domain created: {data.get('domain')}")
    print(f"   New ID: {data.get('id')}")

print("\n" + "=" * 60)
print("Done! SSL certificate should be provisioning now.")
print("Wait 1-5 minutes, then try accessing the site again.")
print("=" * 60)
