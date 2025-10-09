# Toronto Parking ETL Runbook

## Overview
Operational checklist for rebuilding parking, red-light, and ASE datasets, refreshing Redis caches, and validating the deployment in production.

## Prerequisites
- `.env` populated with production credentials (`DATABASE_URL`, `POSTGIS_DATABASE_URL`, `REDIS_URL`, `MAPLIBRE_API_KEY`).
- Postgres instance sized for the full archive (recommend ≥15 GB free).
- Python dependencies installed (`pip install -r requirements.txt`).
- Redis reachable from the host running these commands.

## Environment Setup
```powershell
# Example for PowerShell; adjust as needed
$env:DATABASE_URL = "postgresql://<user>:<pass>@<host>:<port>/<db>"
$env:POSTGIS_DATABASE_URL = "postgres://<user>:<pass>@<postgis-host>:<port>/<db>"
$env:REDIS_URL = "redis://<user>:<pass>@<host>:<port>"
$env:MAPLIBRE_API_KEY = "<maptiler-key>"
```

## One-off Refresh Sequence
Run from the repository root unless noted otherwise.

1. **Load parking tickets (full history):**
   ```powershell
   python scripts/load_parking_tickets_local.py --years 2008-2024
   ```
   Use `--years 2015` to reprocess a single year if required.

2. **Rebuild yearly aggregates:**
   ```powershell
   python scripts/build_yearly_metrics.py
   ```

3. **Regenerate camera datasets (GeoJSON + summaries):**
   ```powershell
   python preprocessing/build_camera_datasets.py
   ```

4. **Push assets to Redis:**
   ```powershell
   python scripts/push_tickets_to_redis.py
   ```

5. **Restart map server / redeploy** so SSR payload and base style pick up the new data.

## Validation Queries & Checks

### Postgres Capacity
```sql
SELECT pg_size_pretty(pg_database_size(current_database()));
```

### Parking Ticket Coverage
```sql
SELECT EXTRACT(YEAR FROM date_of_infraction)::INT AS year,
       COUNT(*)
FROM parking_tickets
GROUP BY 1
ORDER BY 1;
```

### ASE Yearly Totals (sanity: 2,054,677 tickets / $102,733,850)
```sql
SELECT SUM(ticket_count)    AS total_tickets,
       SUM(total_revenue)   AS total_revenue
FROM ase_yearly_locations;
```

### Red-Light Yearly Totals (sanity: 1,094,217 tickets / $355,600,000)
```sql
SELECT SUM(ticket_count)  AS total_tickets,
       SUM(total_revenue) AS total_revenue
FROM red_light_yearly_locations;
```

### Parking Yearly Table Presence
```sql
SELECT year,
       SUM(ticket_count) AS tickets,
       SUM(total_revenue) AS revenue
FROM parking_ticket_yearly_locations
GROUP BY year
ORDER BY year;
```

### Redis Payload Spot-check (from Redis CLI)
```shell
redis-cli --raw GET toronto:map-data:tickets:aggregated:v1:summary | head
redis-cli MEMORY USAGE toronto:map-data:tickets:aggregated:v1
```

## Troubleshooting
- **Disk full during parking load:** Upgrade Postgres storage or purge unused tables; rerun the failed year only (`--years 2010`).
- **Map style still using placeholder key:** Confirm `MAPLIBRE_API_KEY` is set before restarting the server; the `/styles/basic-style.json` endpoint should show your key.
- **Year filter missing entries:** Ensure `build_yearly_metrics.py` completed successfully and the server was restarted after running the script.
- **Redis stale data:** Re-run `scripts/push_tickets_to_redis.py` and verify the keys’ timestamps.

## Automation Notes
- Deploy `scripts/railway_etl_runner.ts` as a Railway Function for scheduled refreshes; configure env vars there identically to `.env`.
- Monitor the `etl_run_log` table for run status and failure messages.
