# PMTiles tooling

This package contains the Redis stream bootstrapper and background worker used to
produce PMTiles artifacts from live PostGIS tables. It lives outside the web app
so it can run with a lean dependency set and independent start commands.

## Scripts

- `npm run start:enqueue` — queue up build jobs in Redis based on the shard
  configuration under `shared/pmtiles/shards.json`.
- `npm run start:worker` — consume queued jobs, write staging tiles, and trigger
  shard rebuilds (with optional MinIO uploads).

Both scripts honour the existing environment variables (database, Redis, and
PMTiles build settings). Run `npm install` in this directory before starting
either command.
