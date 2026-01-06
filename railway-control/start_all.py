#!/usr/bin/env python3
"""
Restart all Railway services in the project.

This will trigger a redeploy for all services.
"""

import time
from config import ENVIRONMENT_ID
from api import graphql_query


def get_all_services():
    """Get all service instances."""
    query = """
    query($id: String!) {
        environment(id: $id) {
            serviceInstances {
                edges {
                    node {
                        serviceId
                        serviceName
                        latestDeployment {
                            id
                            status
                        }
                    }
                }
            }
        }
    }
    """
    result = graphql_query(query, {"id": ENVIRONMENT_ID})

    data = (result or {}).get("data") or {}
    environment = data.get("environment")
    if environment:
        return environment["serviceInstances"]["edges"]
    return []


def redeploy_service(service_id: str) -> bool:
    """Trigger a redeploy for a service."""
    query = """
    mutation($serviceId: String!, $environmentId: String!) {
        serviceInstanceRedeploy(serviceId: $serviceId, environmentId: $environmentId)
    }
    """
    result = graphql_query(query, {
        "serviceId": service_id,
        "environmentId": ENVIRONMENT_ID
    })
    
    if result and result.get("data", {}).get("serviceInstanceRedeploy") is True:
        return True
    return False


def restart_deployment(deployment_id: str) -> bool:
    """Restart a specific deployment."""
    query = """
    mutation($id: String!) {
        deploymentRestart(id: $id)
    }
    """
    result = graphql_query(query, {"id": deployment_id})
    
    if result and result.get("data", {}).get("deploymentRestart") is True:
        return True
    return False


def main():
    print("=" * 60)
    print("🚀 STARTING ALL RAILWAY SERVICES")
    print("=" * 60)
    
    services = get_all_services()
    
    if not services:
        print("❌ Failed to get service list")
        return
    
    started = []
    failed = []
    
    # Prioritize databases, then app
    def sort_key(edge):
        name = edge["node"]["serviceName"].lower()
        if "postgres" in name or "postgis" in name:
            return 0
        if "redis" in name:
            return 1
        return 2
    
    sorted_services = sorted(services, key=sort_key)
    
    for edge in sorted_services:
        node = edge["node"]
        service_name = node["serviceName"]
        service_id = node["serviceId"]
        deployment = node.get("latestDeployment")
        
        print(f"\n📦 {service_name.upper()}")
        print("-" * 40)
        
        # Try serviceInstanceRedeploy first
        print("  Triggering redeploy...")
        if redeploy_service(service_id):
            print("  ✅ Redeploy triggered!")
            started.append(service_name)
        elif deployment:
            # Fallback to restart
            print("  ⚠️  Redeploy failed, trying restart...")
            if restart_deployment(deployment["id"]):
                print("  ✅ Restart triggered!")
                started.append(service_name)
            else:
                print("  ❌ Failed to start service")
                failed.append(service_name)
        else:
            print("  ❌ No deployment to restart")
            failed.append(service_name)
        
        # Wait between services for proper startup order
        if service_name.lower() not in ["toronto-parking", "app"]:
            print("  ⏳ Waiting 3s before next service...")
            time.sleep(3)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    print(f"  ✅ Started: {', '.join(started) if started else 'None'}")
    print(f"  ❌ Failed:  {', '.join(failed) if failed else 'None'}")
    
    if started:
        print("\n💡 Run 'python status.py' to check deployment status")
        print("💡 Deployments may take 1-5 minutes to complete")


if __name__ == "__main__":
    main()
