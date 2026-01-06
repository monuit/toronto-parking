"""Railway project configuration.

Important: do not hardcode tokens in this repo.
Set a project-scoped token via environment variable instead.
"""

import os

# Railway GraphQL API endpoint
# Note: Use .com not .app
RAILWAY_API = "https://backboard.railway.com/graphql/v2"

# Project-scoped token (only has access to this specific project)
# Uses Project-Access-Token header instead of Authorization: Bearer
#
# Preferred env var: RAILWAY_PROJECT_TOKEN
# Fallback env var:  RAILWAY_TOKEN
RAILWAY_TOKEN = os.getenv("RAILWAY_PROJECT_TOKEN") or os.getenv("RAILWAY_TOKEN") or ""

# Set to True if using a project token, False if using account/team token
IS_PROJECT_TOKEN = True

# Project details
PROJECT_ID = "18bce3c7-a46a-473c-9f3c-00bc32e6322b"
ENVIRONMENT_ID = "8326720e-6bf2-47f3-b336-d04be106d7ec"

# Services in the project
SERVICES = {
    "app": "15593dae-9d71-4b15-8e39-ecc1cbee11d3",
    "postgres": "4ec76b6f-84e9-41d1-a487-abb89e517fa4",
    "postgis": "b0a63598-8b62-4ad1-9c0d-b3c182bcc731",
    "redis": "fa9f55d1-30f2-45a8-9ce3-0b5081131376",
}
