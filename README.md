# Toronto Parking Insights

Interactive map, analytics, and data pipeline for Toronto parking enforcement tickets, red-light charges, and automated speed enforcement locations. The project combines a PostGIS-backed ETL pipeline with a React + MapLibre front-end, Redis-backed vector tiles, and Railway deployment tooling.

## Table of Contents

1. [Overview & Recent Work](#overview--recent-work)
2. [System Architecture](#system-architecture)
3. [Local Getting Started](#local-getting-started)
4. [Railway Deployment](#railway-deployment)
5. [Project Layout](#project-layout)
6. [How the Pieces Fit Together](#how-the-pieces-fit-together)
7. [Development Guidelines](#development-guidelines)
8. [Testing & Quality Gates](#testing--quality-gates)
9. [Troubleshooting](#troubleshooting)
10. [Contributing](#contributing)
11. [License & Data Attribution](#license--data-attribution)

---

## Overview & Recent Work

- **Datasets**: 26M+ parking tickets (2008–present), red-light camera charges, ASE camera rotations, and Toronto ward boundaries.
- **What ships**: SSR React map, viewport summaries, ward/street leaderboards, Ko-fi support CTA, and data exports served from Redis-backed endpoints.
- **Recent optimizations**
  - Web Mercator tile schema with partitioned tile tables and quadkey pruning (no more per-request ST_Transform).
  - Redis tile cache with Brotli compression and tiered TTLs (24h/2h/10m by zoom) plus GTA prewarm job.
  - Client viewport summary throttling, in-flight dedupe, and 10-minute TTL eviction for glow datasets.
  - Hardened SSR error responses and resolved React render loops.
  - Mobile drawer/header + legend support button, updated copy, and legacy totals toggle (parking & RLC only).

---

## System Architecture

| Layer | Description |
| --- | --- |
| **PostgreSQL + PostGIS** | Primary warehouse. Holds raw tickets, camera tables, and pre-computed tile partitions (*_tiles) with geom_3857 columns and quadkey prefixes. |
| **Python services** | src/tiles/service.py handles tile queries and schema bootstrapping (TileSchemaManager.ensure). ETL modules under src/etl orchestrate downloads, cleaning, and yearly snapshots. |
| **Redis** | Cache for compressed vector tiles and warm map data used by SSR and API routes. |
| **Node/Express SSR** | map-app/server/index.js renders HTML, proxies tile requests, exposes /api/* endpoints, and initializes Redis cache warmers. |
| **React + MapLibre** | map-app/src contains the interactive client (desktop + mobile layouts, viewport analytics). |
| **CI / Ops** | Git-based workflows, Railway deployment, and ad-hoc scripts in map-app/scripts + scripts/.

---

## Local Getting Started

### Prerequisites

- Python 3.11+
- Node.js 20+
- PostgreSQL 15+ with PostGIS
- Redis 6+
- Optional: MapTiler API key for custom basemap styles (otherwise default style loads).

### 1. Clone & Bootstrapping

`ash
git clone https://github.com/monuit/toronto-parking.git
cd toronto-parking
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
cd map-app && npm install && cd ..
`

### 2. Environment Variables

Copy .env.example if available (or create .env at repo root):

`dotenv
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/toronto_parking
REDIS_URL=redis://localhost:6379/0
MAPTILER_API_KEY=your_maptiler_key          # optional but recommended
OPENAI_API_KEY=placeholder                   # leave blank if unused
TILE_PREWARM_ENABLED=true
TILE_PREWARM_MIN_ZOOM=8
TILE_PREWARM_MAX_ZOOM=14
`

> The Node server reads from the root .env. Vite also exposes variables prefixed with VITE_.

### 3. Database Prep

1. Create database and enable PostGIS.
   `sql
   CREATE DATABASE toronto_parking;
   \c toronto_parking
   CREATE EXTENSION postgis;
   `
2. Apply tile schema guarantees (idempotent):
   `ash
   python - <<'PY'
from src.etl.config import ETLConfig
from src.etl.postgres import PostgresClient
from src.tiles.schema import TileSchemaManager
cfg = ETLConfig.default()
pg = PostgresClient(cfg.database.dsn)
TileSchemaManager(pg).ensure()
print( Tile schema ready ✔)
PY
   `
   > This adds geom_3857 columns, helper SQL functions, tile tables, and list partitions if they do not already exist.
3. Load data. For a full refresh run the ETL (time-consuming) or import a dump.
   `ash
   python -m src.etl.runner              # full ETL (downloads from Toronto Open Data)
   # or load your own CSV extracts with scripts/load_parking_tickets_local.py
   `

### 4. Redis Cache (optional but recommended)

`ash
node map-app/scripts/backgroundAppDataRefresh.js
`

### 5. Run the App

- **Client/Vite dev server:**
  `ash
  cd map-app
  npm run dev
  `
  Visit http://localhost:5173 (API proxied to SSR server when running).

- **SSR dev server (Node + Express):**
  `ash
  cd map-app
  npm run dev:ssr
  `
  This hosts http://localhost:5173 with server rendering, Redis integration, and tile prewarm cron.

---

## Railway Deployment

1. Install the Railway CLI: 
pm i -g @railway/cli and run ailway login.
2. Provision services:
   - PostgreSQL (enable PostGIS manually after provision).
   - Redis.
   - Web service for the SSR app.
3. Sync environment variables (ailway variables set ...) mirroring your local .env (DB connection strings, Redis URL, MapTiler key, OpenAI key if needed).
4. Deploy using the provided ailway.json (Nixpacks) from repo root:
   `ash
   railway up --service web
   `
5. Ensure the PostgreSQL instance has the tile schema:
   `ash
   railway run -- python - <<'PY'
from src.etl.config import ETLConfig
from src.etl.postgres import PostgresClient
from src.tiles.schema import TileSchemaManager
cfg = ETLConfig.default()
pg = PostgresClient(cfg.database.dsn)
TileSchemaManager(pg).ensure()
print(Tile schema ready ✔)
PY
   `
   > The Python tile service also calls TileSchemaManager.ensure() on boot, but running once up-front gives visibility into any permissions issues.
6. After deploy, warm caches if desired:
   `ash
   railway run --service web -- npm run prewarm:tiles
   `

---

## Project Layout

`
toronto-parking/
├─ map-app/                    # React + SSR server
│  ├─ server/                  # Express server, Redis tile cache, warmers
│  └─ src/                     # Components, hooks, contexts, styles
├─ src/
│  ├─ etl/                     # CKAN ETL modules, state tracking, Postgres helpers
│  ├─ tiles/                   # Tile schema + service (Web Mercator queries)
│  └─ utils/                   # Shared helpers
├─ scripts/                    # Operational scripts (metrics, cache fills)
├─ analysis/                   # Snapshots & investigative notebooks
├─ docker-compose.local.yml    # Optional local Postgres/Redis stack
├─ requirements.txt            # Python dependencies
├─ map-app/package.json        # Node dependencies
└─ railway.json                # Railway deployment spec
`

---

## How the Pieces Fit Together

1. **ETL** downloads ticket + camera datasets, normalizes addresses, and loads PostGIS tables (*_tickets, *_charges).
2. **Tile schema manager** materializes Mercator geometries plus quadkey-based tile partitions (no runtime transform/simplify).
3. **Python tile service** queries the partitioned tables and populates Redis with Brotli-compressed MVT payloads using zoom-based TTLs.
4. **Node/Express SSR** renders HTML, injects initial dataset payloads, exposes JSON APIs, and maintains a tile prewarm loop (zoom 8–14 across GTA bounds).
5. **React client** consumes SSR payloads, requests tiles, throttles viewport summary calls, renders stats/leaderboards, and manages legacy totals toggles (parking + red-light only).
6. **Monitoring** logs tile warmup durations and warns on cache misses. Lint + tests run locally (pytest currently requires unique_queries.json fixture – see troubleshooting).

---

## Development Guidelines

- Follow existing patterns (Python type hints + docstrings; React functional components with hooks and PropTypes).
- Keep SSR-friendly guards (	ypeof window !== 'undefined') around browser APIs.
- Respect caching helpers (esolveTileTtl, GLOW_CACHE_TTL_MS) and avoid silently bypassing Redis.
- Use TileSchemaManager (or SQL migrations) for schema changes; avoid ad-hoc DDL in production.
- SSR error responses must remain generic (Internal Server Error), with detailed logging server-side only.

---

## Testing & Quality Gates

| Command | Purpose |
| --- | --- |
| 
pm run lint (inside map-app/) | ESLint + React lint rules. Required before PR. |
| python -m pytest | Python tests. Note: geocoding/test_robust_geocoder.py expects unique_queries.json; without it, pytest fails during collection. |
| 
pm run dev:ssr | Manual E2E smoke (SSR, Redis cache, tile warmup). |
| 
ode map-app/scripts/backgroundAppDataRefresh.js | Optional smoke for dataset refresh job. |

Document any known test failures (e.g., missing fixtures) in your PR notes.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
| --- | --- | --- |
| Failed to resolve import ../lib/envFlags | Dev server caching stale imports. | Restart 
pm run dev:ssr; ensure file isn\'t referenced. |
| Cannot access 'handleLegacyTotalsToggle' before initialization | Handler declared after usage. | Keep handler definitions above their first reference (already fixed). |
| SSR log Redis: disabled | REDIS_URL missing or Redis offline. | Configure REDIS_URL and confirm service availability. |
| Blank map / tile 404s | Tile schema or prewarm not executed. | Run schema ensure script and 
pm run prewarm:tiles. |
| Pytest fixture error | unique_queries.json absent. | Restore fixture or skip geocoding test module locally. |

---

## Contributing

1. Fork and branch: eature/<summary> or ix/<summary>.
2. Align with coding conventions and keep components focused.
3. Include tests or manual verification steps.
4. Run 
pm run lint and python -m pytest (noting fixture gaps).
5. Submit a PR outlining changes, risks, rollout plan, and any schema requirements.

Please do not commit credentials. Use Railway secrets or local .env entries.

---

## License & Data Attribution

- MIT License (see LICENSE).
- Data © City of Toronto, licensed under the [Open Government Licence – Toronto](https://open.toronto.ca/open-data-license/).
- Map tiles © OpenStreetMap contributors / MapLibre.

Maintained by Moe (@monuit) and contributors. For questions, open an issue or reach out via the contact links in the in-app How It Works modal.
