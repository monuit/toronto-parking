# Railway Deployment Control

Scripts to manage Railway deployments - stop all services to save costs, restart when needed.

## Usage

### Stop all services

```bash
python stop_all.py
```

### Restart all services (redeploy latest)

```bash
python start_all.py
```

### Check current status

```bash
python status.py
```

## Configuration

The scripts use a **project-scoped token** that only has access to this specific Railway project.

**Important**: Project tokens use the `Project-Access-Token` header, not `Authorization: Bearer`.

- **Project ID**: `18bce3c7-a46a-473c-9f3c-00bc32e6322b`
- **Environment**: Production (`8326720e-6bf2-47f3-b336-d04be106d7ec`)

## Services Managed

| Service | Service ID |
|---------|------------|
| toronto-parking (app) | `15593dae-9d71-4b15-8e39-ecc1cbee11d3` |
| Postgres | `4ec76b6f-84e9-41d1-a487-abb89e517fa4` |
| PostGIS | `b0a63598-8b62-4ad1-9c0d-b3c182bcc731` |
| Redis | `fa9f55d1-30f2-45a8-9ce3-0b5081131376` |

## GraphQL API Reference

- **Endpoint**: `https://backboard.railway.com/graphql/v2`
- **Authentication**: `Project-Access-Token: <token>` header
- **Stop Mutation**: `deploymentStop(id: String!): Boolean!`
- **Redeploy Mutation**: `serviceInstanceRedeploy(serviceId: String!, environmentId: String!): Boolean!`

## Files

- [config.py](config.py) - Token and project configuration
- [api.py](api.py) - GraphQL API client
- [status.py](status.py) - Check current deployment status
- [stop_all.py](stop_all.py) - Stop all active deployments
- [start_all.py](start_all.py) - Restart all services
