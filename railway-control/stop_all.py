#!/usr/bin/env python3
"""
Stop all Railway services in the project.

This will stop all active deployments to save costs.
Use start_all.py to restart services when needed.
"""

import time
from config import ENVIRONMENT_ID
from api import graphql_query


def get_all_active_deployments():
    """Get all service instances and their latest deployments."""
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
    
    if result and result.get("data", {}).get("environment"):
        return result["data"]["environment"]["serviceInstances"]["edges"]
    return []


def stop_deployment(deployment_id: str) -> bool:
    """Stop a deployment using deploymentStop mutation."""
    query = """
    mutation($id: String!) {
        deploymentStop(id: $id)
    }
    """
    result = graphql_query(query, {"id": deployment_id})
    
    if result and result.get("data", {}).get("deploymentStop") is True:
        return True
    return False


def remove_deployment(deployment_id: str) -> bool:
    """Remove a deployment (fallback if stop doesn't work)."""
    query = """
    mutation($id: String!) {
        deploymentRemove(id: $id)
    }
    """
    result = graphql_query(query, {"id": deployment_id})
    
    if result and result.get("data", {}).get("deploymentRemove") is True:
        return True
    return False


def main():
    print("=" * 60)
    print("🛑 STOPPING ALL RAILWAY SERVICES")
    print("=" * 60)
    
    services = get_all_active_deployments()
    
    if not services:
        print("❌ Failed to get service list")
        return
    
    stopped = []
    failed = []
    skipped = []
    
    for edge in services:
        node = edge["node"]
        service_name = node["serviceName"]
        deployment = node.get("latestDeployment")
        
        print(f"\n📦 {service_name.upper()}")
        print("-" * 40)
        
        if not deployment:
            print("  ⏭️  No deployment found")
            skipped.append(service_name)
            continue
        
        status = deployment["status"]
        deploy_id = deployment["id"]
        
        # Skip if already stopped/sleeping/crashed
        if status in {"SLEEPING", "REMOVED", "CRASHED", "FAILED"}:
            print(f"  ⏭️  Already {status}")
            skipped.append(service_name)
            continue
        
        print(f"  Found: {status} - {deploy_id[:30]}...")
        
        # Try deploymentStop
        print("  Attempting deploymentStop...")
        if stop_deployment(deploy_id):
            print("  ✅ Stopped successfully!")
            stopped.append(service_name)
        else:
            # Fallback to deploymentRemove
            print("  ⚠️  deploymentStop failed, trying deploymentRemove...")
            if remove_deployment(deploy_id):
                print("  ✅ Removed successfully!")
                stopped.append(service_name)
            else:
                print("  ❌ Failed to stop deployment")
                failed.append(service_name)
        
        # Small delay between operations
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    print(f"  ✅ Stopped: {', '.join(stopped) if stopped else 'None'}")
    print(f"  ⏭️  Skipped: {', '.join(skipped) if skipped else 'None'}")
    print(f"  ❌ Failed:  {', '.join(failed) if failed else 'None'}")
    
    if stopped:
        print("\n💡 To restart services, run: python start_all.py")


if __name__ == "__main__":
    main()
