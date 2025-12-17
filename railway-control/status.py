#!/usr/bin/env python3
"""Check status of all Railway services."""

from config import ENVIRONMENT_ID
from api import graphql_query


def get_all_services_status():
    """Get all service instances and their latest deployments."""
    query = """
    query($id: String!) {
        environment(id: $id) {
            id
            name
            serviceInstances {
                edges {
                    node {
                        serviceId
                        serviceName
                        latestDeployment {
                            id
                            status
                            createdAt
                        }
                    }
                }
            }
        }
    }
    """
    result = graphql_query(query, {"id": ENVIRONMENT_ID})
    
    if result and result.get("data", {}).get("environment"):
        return result["data"]["environment"]["serviceInstances"]["edges"]
    return []


def main():
    print("=" * 60)
    print("Railway Service Status")
    print("=" * 60)
    
    services = get_all_services_status()
    
    if not services:
        print("❌ Failed to get service status")
        return
    
    for edge in services:
        node = edge["node"]
        service_name = node["serviceName"]
        deployment = node.get("latestDeployment")
        
        print(f"\n📦 {service_name.upper()}")
        print("-" * 40)
        
        if not deployment:
            print("  No deployments found")
            continue
        
        status = deployment["status"]
        created = deployment["createdAt"][:19].replace("T", " ")
        deploy_id = deployment["id"][:20]
        
        # Status emoji
        emoji = {
            "SUCCESS": "✅",
            "DEPLOYING": "🔄",
            "BUILDING": "🔨",
            "FAILED": "❌",
            "CRASHED": "💥",
            "REMOVED": "🗑️",
            "SLEEPING": "😴",
        }.get(status, "❓")
        
        print(f"  {emoji} {status:12} {created} {deploy_id}...")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
