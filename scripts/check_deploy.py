#!/usr/bin/env python3
"""Check deployment status."""
import requests
import json

RAILWAY_API = 'https://backboard.railway.app/graphql/v2'
RAILWAY_TOKEN = 'a9b69932-5252-4c8d-b338-6a685d1e2674'
PROJECT_ID = '18bce3c7-a46a-473c-9f3c-00bc32e6322b'
APP_SERVICE_ID = '15593dae-9d71-4b15-8e39-ecc1cbee11d3'

headers = {
    'Authorization': f'Bearer {RAILWAY_TOKEN}',
    'Content-Type': 'application/json'
}

query = """
query($projectId: String!, $serviceId: String!) {
    deployments(
        first: 5
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

payload = {
    'query': query,
    'variables': {
        'projectId': PROJECT_ID,
        'serviceId': APP_SERVICE_ID
    }
}

response = requests.post(RAILWAY_API, headers=headers, json=payload)
result = response.json()

if result.get('data', {}).get('deployments', {}).get('edges'):
    print('Recent deployments:')
    for edge in result['data']['deployments']['edges']:
        d = edge['node']
        print(f"  {d['status']:15} {d['createdAt']} {d['id'][:20]}...")
else:
    print('No deployments found')
    print(json.dumps(result, indent=2))
