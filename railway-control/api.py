"""Railway GraphQL API client."""

import json
import requests
from config import RAILWAY_API, RAILWAY_TOKEN, IS_PROJECT_TOKEN


def graphql_query(query: str, variables: dict = None) -> dict:
    """Execute a GraphQL query against Railway API."""
    if not RAILWAY_TOKEN:
        print(
            "Missing Railway token. Set env var RAILWAY_PROJECT_TOKEN "
            "(preferred) or RAILWAY_TOKEN."
        )
        return None

    # Project tokens use a different header than account/team tokens
    if IS_PROJECT_TOKEN:
        headers = {
            "Project-Access-Token": RAILWAY_TOKEN,
            "Content-Type": "application/json"
        }
    else:
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


def get_deployments(project_id: str, service_id: str, limit: int = 5) -> list:
    """Get recent deployments for a service."""
    query = """
    query($projectId: String!, $serviceId: String!) {
        deployments(
            first: %d
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
    """ % limit
    
    result = graphql_query(query, {
        "projectId": project_id,
        "serviceId": service_id
    })
    
    if result and result.get("data", {}).get("deployments", {}).get("edges"):
        return [edge["node"] for edge in result["data"]["deployments"]["edges"]]
    return []


def get_active_deployment(project_id: str, service_id: str) -> dict | None:
    """Get the currently active/running deployment for a service."""
    deployments = get_deployments(project_id, service_id, limit=10)
    
    # Find deployment with SUCCESS or DEPLOYING status (active)
    active_statuses = {"SUCCESS", "DEPLOYING", "BUILDING", "INITIALIZING"}
    for d in deployments:
        if d["status"] in active_statuses:
            return d
    return None


def stop_deployment(deployment_id: str) -> bool:
    """Stop a deployment using deploymentStop mutation."""
    query = """
    mutation($id: String!) {
        deploymentStop(id: $id)
    }
    """
    
    result = graphql_query(query, {"id": deployment_id})
    
    if result and result.get("data", {}).get("deploymentStop"):
        return True
    return False


def remove_deployment(deployment_id: str) -> bool:
    """Remove a deployment using deploymentRemove mutation (fallback)."""
    query = """
    mutation($id: String!) {
        deploymentRemove(id: $id)
    }
    """
    
    result = graphql_query(query, {"id": deployment_id})
    
    if result and result.get("data", {}).get("deploymentRemove"):
        return True
    return False


def redeploy_service(service_id: str, environment_id: str) -> dict | None:
    """Trigger a new deployment for a service."""
    query = """
    mutation($serviceId: String!, $environmentId: String!) {
        serviceInstanceRedeploy(serviceId: $serviceId, environmentId: $environmentId)
    }
    """
    
    result = graphql_query(query, {
        "serviceId": service_id,
        "environmentId": environment_id
    })
    
    if result and result.get("data"):
        return result["data"]
    return None


def restart_deployment(deployment_id: str) -> bool:
    """Restart a specific deployment."""
    query = """
    mutation($id: String!) {
        deploymentRestart(id: $id)
    }
    """
    
    result = graphql_query(query, {"id": deployment_id})
    
    if result and result.get("data", {}).get("deploymentRestart"):
        return True
    return False
