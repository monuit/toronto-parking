# Toronto Parking Insights

An end-to-end platform for analysing Toronto’s parking enforcement (26M+ tickets), red-light camera offences, and automated speed enforcement (ASE) activity. The stack pairs a high-performance PostGIS + Redis data backend with a MapLibre-powered client, PMTiles distribution, and fully automated Railway deployment.

---

## Table of Contents

1. [Feature Highlights](#feature-highlights)
2. [System Architecture](#system-architecture)
3. [Local Development](#local-development)
4. [Environment Variables](#environment-variables)
5. [Data & PMTiles Pipeline](#data--pmtiles-pipeline)
6. [Operations & Monitoring](#operations--monitoring)
7. [Testing & Quality](#testing--quality)
8. [Deployment (Railway)](#deployment-railway)
9. [Troubleshooting](#troubleshooting)
10. [Security & Secret Handling](#security--secret-handling)
11. [Project Structure](#project-structure)
12. [License & Attribution](#license--attribution)

---

## Feature Highlights

- **Server-side rendered map** with MapLibre GL, responsive mobile layout, leaderboards, stats panels, legacy totals toggle (parking + red-light) and Ko-fi support CTA.
- **High-availability tile delivery** using:
  - Partitioned PostGIS quadkey tables (Web Mercator geom_3857 + prefix indexes).
  - Redis-backed tile caches with Brotli compression, tiered TTLs (24h/2h/10m by zoom) and pre-warm routines.
  - PMTiles shards hosted on MinIO with CDN/origin fallbacks and automatic warmup.
- **MapTiler proxy hardening** (server-side key injection, adaptive fallback, style sanitisation, cache invalidation, runtime metrics).
- **Ward + Street analytics** cached in memory/Redis with background refresh scheduling to avoid Postgres overload.
- **ETL framework** (Python) for downloading, normalising, and loading city datasets with repeatable schema management.
- **Operational scripts** covering tile schema resets, PMTiles builds/uploads, cache warmers, metric collection, table usage audits, and schema rollback.

---

## System Architecture

```
           +---------------------------+
           |  CKAN / Open Data APIs    |
           +-------------+-------------+
                         |
               (python src/etl)
                         v
 +------------------------------+      +-----------------------+
 | PostgreSQL 17 + PostGIS      |      | Redis (tiles + data)  |
 | • Raw datasets               |      | • Tile caches         |
 | • *_tiles partitions         |<---->| • Ward summaries      |
 | • TileSchemaManager helpers  |      | • Glow datasets       |
 +--------------+---------------+      +-----------+-----------+
                |                                 |
                | Postgres/Redis clients          |
                v                                 v
        +--------------------+      +-------------------------------+
        | Node/Express SSR   | ---> | MapLibre client (React/Vite)  |
        | • /api, /data      |      | • Points + choropleth layers  |
        | • MapTiler proxy   |      | • PMTiles protocol handler    |
        | • PMTiles manifest |      | • Viewport summaries          |
        +---------+----------+      +-------------------------------+
                  |
                  v
        +--------------------+
        | MinIO (PMTiles)    |
        | • Sharded point    |
        |   & ward datasets  |
        | • CDN/origin URLs  |
        +--------------------+
```

Key modules:

| Component | Responsibility |
| --- | --- |
| `src/etl` | Dataset download/transformation, ETL state tracking, schema bootstrap. |
| `src/tiles` | Quadkey schema manager and SQL tile function compilation. |
| `map-app/server` | SSR app, API routes, warmers, proxy, PMTiles manifest, Redis/Postgres clients. |
| `map-app/src` | React map UI, layers, contexts, viewport throttling, metrics overlays. |
| `scripts/pmtiles` | Build, convert, upload, warm PMTiles assets. |
| `map-app/scripts` & `scripts/` | Operational tooling (cache refresh, metrics, ETL runners, schema utilities). |

---

## Local Development

### Requirements

- Python **3.11+** (with `pip`)
- Node.js **22.12.0+** (matches production engines)
- PostgreSQL **15+** with PostGIS extension
- Redis **6+**
- Optional: MinIO/S3 credentials for PMTiles pipeline, MapTiler API key

### Setup

```bash
git clone https://github.com/monuit/toronto-parking.git
cd toronto-parking

# Python environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

# Node dependencies
cd map-app
npm install
cd ..
```

### Database initialisation

```sql
CREATE DATABASE toronto_parking;
\c toronto_parking
CREATE EXTENSION postgis;
```

Then bake the tile schema (idempotent):

```bash
python - <<'PY'
from src.etl.config import ETLConfig
from src.etl.postgres import PostgresClient
from src.tiles.schema import TileSchemaManager

cfg = ETLConfig.default()
pg = PostgresClient(cfg.database.dsn)
TileSchemaManager(pg).ensure()
print("Tile schema ready")
PY
```

Load data via the ETL runner (full refresh) or custom loaders:

```bash
python -m src.etl.runner              # downloads CKAN datasets, transforms, loads Postgres
# or targeted loaders, e.g.
python scripts/load_parking_tickets_local.py
python scripts/load_ase_charges.py
```

### Running the app

In separate terminals:

```bash
# Express SSR + API
cd map-app
npm run dev:ssr

# Vite client (optional for HMR)
npm run dev
```

Visit <http://localhost:5173>. The SSR process proxies API requests, manages Redis/PMTiles warmup, and surfaces detailed logging (caches, proxy mode transitions, PMTiles metrics).

### Optional local services

- **Redis cache priming:** `node map-app/scripts/backgroundAppDataRefresh.js`
- **PMTiles warmup test:** `node map-app/server/pmtilesWarmup.js`
- **Ward cache refresh:** `node map-app/scripts/backgroundAppDataRefresh.js --wards`

---

## Environment Variables

Create `.env` (local) or `.env.production` (deployment). Key variables:

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` / `CORE_DB_URL` / `TILES_DB_URL` | Postgres connection strings (tile + analytics queries). |
| `REDIS_URL` | Redis endpoint for cache + warmup jobs. |
| `MAPLIBRE_API_KEY` | MapTiler key injected *server-side* into proxied style/tiles. |
| `MAPTILER_PROXY_MODE` | `proxy` (default) or `direct`; proxy keeps key off the client. |
| `MAPTILER_PROXY_PATH` | API path prefix (defaults to `/api/maptiler`). |
| `MAP_TILE_REDIS_TTL`, `MAP_TILE_PREWARM`, `MAP_TILE_REDIS_MAX_BYTES` | Tile cache tuning knobs. |
| `PMTILES_ENABLED` | Toggle PMTiles pipeline usage (fallbacks to legacy XYZ paths when `false`). |
| `MINIO_*` + `PMTILES_BUCKET`, `PMTILES_PREFIX` | MinIO/S3 credentials and layout for PMTiles artefacts. |
| `PMTILES_PUBLIC_BASE_URL` / `PMTILES_CDN_BASE_URL` | Public URL(s) surfaced in the manifest. |
| `MAP_DATA_REDIS_NAMESPACE` | Namespace prefix for tile + summary caches (defaults `toronto:map-data`). |
| `SQL_STATEMENT_TIMEOUT_MS`, `CACHE_TTL_S` | Safety limits for backend query/caching. |

See `.env.production` for a production-ready template aligned with Railway’s service variables (Postgres/Redis/MinIO resource bindings).

---

## Data & PMTiles Pipeline

The `scripts/pmtiles` toolchain converts database state into web-ready PMTiles shards hosted on MinIO.

1. **Export ward GeoJSON:** `python scripts/pmtiles/export_ward_geojson.py`
2. **Build MBTiles:** `python scripts/pmtiles/build_ward_mbtiles.py`
3. **Convert to PMTiles:** `python scripts/pmtiles/convert_mbtiles_to_pmtiles.py`
4. **Point datasets:** use `scripts/pmtiles/build_pmtiles.py` (multi-threaded batching, ProcessPool compression, optional `--refresh-schema`).
5. **Upload:** `node scripts/pmtiles/upload_to_minio.mjs --env-file .env.production`
6. **Verify headers + access:** `python scripts/pmtiles/check_upload_headers.py`

The Node server reads `shared/pmtiles/shards.json` and uses `pmtilesManifest.js` to emit dataset + ward entries with both origin and CDN URLs. `pmtilesWarmup.js` prefetches tiles (3×3 grid around the GTA) across zooms 8–14 and records metrics (`/healthz`).

### Glow data pipeline

- `map-app/server/glowTileService.js` serves vector glow lines from Redis/Postgres with request dedupe and gzip/Brotli support.
- `scripts/pmtiles/build_glow_mbtiles.py` & `convert_mbtiles_to_pmtiles.py` handle PMTiles artefacts for glow datasets.
- Postgres fallback functions (`public.get_glow_tile`) ensure availability when caches miss.

---

## Operations & Monitoring

- **Warmup jobs:**
  - `pmtilesWarmup.js` (hourly interval) → logs success/failure counts + latency.
  - Ward summary refresh uses staggered timers (memory cache retains last good snapshot while background updates run).
- **Metrics endpoint:** `GET /healthz` returns MapTiler proxy mode, PMTiles warmup stats, cache sizes, and recent errors.
- **Startup hardening:**
  - Clears `toronto:map-data:*` Redis keys on boot (namespace reset).
  - Schedules `VACUUM (ANALYZE)` on tile tables after 60s delay (if tables exist).
  - MapTiler proxy auto-detects direct/proxy viability and invalidates cached styles on mode flips.
- **Logging:** Structured console logs include request IDs, cache hits, gzip decisions, fallback triggers, and PMTiles manifest state.

---

## Testing & Quality

| Command | Description |
| --- | --- |
| `npm run lint` (inside `map-app`) | ESlint across client + server (required before PR/commit). |
| `npm run build` / `npm run build:ssr` | Production build verification (SSR bundle + client assets). |
| `python -m pytest` | Python test suite (note: `tests/geocoding/test_robust_geocoder.py` expects `unique_queries.json`; provide fixture or skip module). |
| `npm run test` | Placeholder (add Jest/Vitest coverage as needed). |
| `python scripts/list_table_usage.py` | Operational smoke to inspect Postgres table bloat. |

Always run lint + builds prior to pushing. Document skipped tests (e.g., missing fixtures) in PR descriptions.

---

## Deployment (Railway)

1. **Ensure latest commits merged to `main`.**
2. **Railway service settings** should rely on repo `railway.json`, which:
   - Uses Nixpacks builder with `nodejs_22`.
   - Runs `cd map-app && npm ci && npm run build:ssr && cd ../tools/pmtiles && npm ci` during build.
   - Starts via `node scripts/start-app-with-worker.mjs` (spawns SSR server + warmup worker).
3. **Environment variables**: mirror `.env.production` using Railway Secrets (no plain-text DB credentials).
4. **Provisioned services**: Postgres (enable PostGIS), Redis, MinIO bucket (or S3 compatible), web service.
5. **Deploy:** `railway up --service web` or push to tracked branch.
6. **Post-deploy checks:**
   - Confirm `/api/pmtiles-manifest` returns `enabled: true` with correct `publicBaseUrl` + `objectPrefix`.
   - Tail logs for MapTiler proxy mode, warmup metrics, Redis reset notice.
   - Visit site, verify tiles load (no MapTiler 499/408), Ko-fi button position, ward toggles, glow layers.

Rotate any credentials that were ever committed or leaked (e.g., older Postgres DSNs) before production swap.

---

## Troubleshooting

| Symptom | Likely Cause | Resolution |
| --- | --- | --- |
| `MODULE_NOT_FOUND vite/bin/vite.js` during Railway build | Old build command using `npm ci --prefix` on Node 20. | Ensure `railway.json` from `main` is deployed; clear any custom build overrides. |
| Map tiles fail with `Decoding failed` | PMTiles manifest disabled or URLs wrong. | Check `/api/pmtiles-manifest`; verify `PMTILES_ENABLED=true`, MinIO credentials, and object prefix. |
| Repeated `499/408` on `/api/wards/summary` | Redis TTL shorter than refresh cadence. | Keep default `CACHE_TTL_S=86400` (or ≥ refresh interval); inspect warmup logs. |
| Glow GeoJSON 404 | Redis namespace mismatch or data missing. | Ensure `GLOW_TILE_CACHE_VERSION` matches populated keys or rebuild via glow scripts. |
| MapTiler proxy leaking key | Proxy mode forced to `direct`. | Set `MAPTILER_PROXY_MODE=proxy`, restart service (style cache wipes key placeholder). |
| Pytest fails on missing fixture | `tests/geocoding/...` expects `unique_queries.json`. | Provide fixture under `tests/data/` or skip module (`pytest -k "not geocoding"`). |

---

## Security & Secret Handling

- **No hardcoded credentials.** Scripts now require environment variables (e.g., `scripts/debug/update_tile_functions.py`).
- **Use Railway Secrets / `.env` files** to manage Postgres, Redis, MinIO, MapTiler, and any API tokens.
- **Rotate leaked secrets immediately** (the legacy Postgres DSN from commit `bf7258a` has been purged—ensure Railway credentials are refreshed).
- `.gitignore` covers dumps/artefacts; ensure PMTiles or dumps containing PII stay out of Git.
- Consider enabling pre-commit secret scanning (GitGuardian, gitleaks) for local assurance.

---

## Project Structure

```
toronto-parking/
├─ map-app/
│  ├─ server/               # Express SSR, proxies, warmers, runtime config
│  ├─ src/                  # React components, contexts, layers, hooks
│  ├─ scripts/              # Client-facing operational scripts (warmers, tiles)
│  └─ public/               # Static assets, styles, service worker
├─ scripts/                 # Python & Node automation (ETL, tiles, metrics)
│  ├─ pmtiles/              # Build/convert/upload PMTiles toolchain
│  └─ debug/                # Ad-hoc analytics utilities
├─ src/                     # Python ETL + tile services shared with worker/CLI
├─ shared/pmtiles/          # Shard manifests consumed by server + client
├─ analysis/                # Exploratory notebooks, snapshots, SQL
├─ tools/pmtiles/           # Worker pool (Node) for PMTiles uploads
├─ requirements.txt         # Python dependencies
├─ map-app/package.json     # Node dependencies (engines >= 22.12)
├─ railway.json             # Railway build + start specification
└─ .env.example / .env.production
```

---

## License & Attribution

- MIT License (see `LICENSE`).
- Data © City of Toronto, licensed under the [Open Government Licence – Toronto](https://open.toronto.ca/open-data-license/).
- Map tiles © OpenStreetMap contributors / MapLibre.
- Maintained by Moe (@monuit) and contributors—reach out via GitHub issues or the in-app “How It Works” modal.

---

**Have an improvement in mind?** Open an issue or start a feature branch (`feature/<summary>`). Please run lint/builds before submitting PRs and include any operational notes (schema changes, cache flush needs, deployment steps).
