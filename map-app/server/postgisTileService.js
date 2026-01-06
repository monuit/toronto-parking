import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import process from 'node:process';
import { performance } from 'node:perf_hooks';
import { Pool } from 'pg';
import vtpbf from 'vt-pbf';
import { getTileDbConfig, getTileCacheConfig } from './runtimeConfig.js';
import { TICKET_TILE_MIN_ZOOM } from '../shared/mapConstants.js';

// Memory optimization: Trigger GC if available (requires --expose-gc)
function tryGC() {
  if (typeof globalThis.gc === 'function') {
    try {
      globalThis.gc();
    } catch {
      // Ignore GC errors
    }
  }
}

// Memory optimization: Reduced from 128 to 32 to lower memory footprint
const DEFAULT_MEMORY_CACHE_LIMIT = Number.parseInt(
  process.env.POSTGIS_TILE_MEMORY_CACHE_LIMIT || '',
  10,
) || 32;

const tileCacheConfig = getTileCacheConfig();

const DEFAULT_MEMORY_CACHE_SECONDS = Number.parseInt(
  process.env.POSTGIS_TILE_MEMORY_CACHE_SECONDS || '',
  10,
) || Math.max(300, tileCacheConfig.baseTtlSeconds);

const DEFAULT_CACHE_RULES = [
  { maxZoom: 10, ttl: tileCacheConfig.baseTtlSeconds },
  { maxZoom: 13, ttl: Math.max(600, Math.floor(tileCacheConfig.baseTtlSeconds / 6)) },
  { maxZoom: Number.POSITIVE_INFINITY, ttl: Math.max(300, Math.floor(tileCacheConfig.baseTtlSeconds / 24)) },
];

const PREFETCH_ENABLED = process.env.POSTGIS_TILE_PREFETCH !== '0';
const PREFETCH_MAX_QUEUE = Number.parseInt(process.env.POSTGIS_TILE_PREFETCH_MAX || '', 10) || 16;
const PREFETCH_BATCH_SIZE = Number.parseInt(process.env.POSTGIS_TILE_PREFETCH_BATCH || '', 10) || 6;
const PREFETCH_MAX_ZOOM = Number.parseInt(process.env.POSTGIS_TILE_PREFETCH_MAX_ZOOM || '', 10) || 13;
const PREFETCH_DELAY_MS = Number.parseInt(process.env.POSTGIS_TILE_PREFETCH_DELAY_MS || '', 10) || 35;
const PREFETCH_CHILD_ZOOM_MAX = Number.parseInt(process.env.POSTGIS_TILE_PREFETCH_CHILD_ZOOM_MAX || '', 10) || 12;
const PREFETCH_RADIUS = Number.parseInt(process.env.POSTGIS_TILE_PREFETCH_RADIUS || '', 10) || 1;

const EMPTY_LAYER_TILE_CACHE = new Map();

function getEmptyTileBuffer(layerName) {
  if (!EMPTY_LAYER_TILE_CACHE.has(layerName)) {
    const layer = { features: [], extent: 4096 }; // minimal layer definition
    const payload = { [layerName]: layer };
    const buffer = vtpbf.fromGeojsonVt(payload, { extent: 4096, version: 2 });
    EMPTY_LAYER_TILE_CACHE.set(layerName, buffer);
  }
  return EMPTY_LAYER_TILE_CACHE.get(layerName);
}

const DATASET_CONFIG = {
  parking_tickets: {
    functionName: 'public.get_parking_tiles',
    minZoom: TICKET_TILE_MIN_ZOOM,
    ttlRules: DEFAULT_CACHE_RULES,
    layer: 'parking_tickets',
    prefetchMaxZoom: PREFETCH_MAX_ZOOM,
  },
  red_light_locations: {
    functionName: 'public.get_red_light_tiles',
    minZoom: 8,
    ttlRules: DEFAULT_CACHE_RULES,
    layer: 'red_light_locations',
    prefetchMaxZoom: 11,
  },
  ase_locations: {
    functionName: 'public.get_ase_tiles',
    minZoom: 8,
    ttlRules: DEFAULT_CACHE_RULES,
    layer: 'ase_locations',
    prefetchMaxZoom: 11,
  },
};

const PARKING_TILE_MODE_POLICY = [
  { maxZoom: 8, mode: 'cluster', gridMeters: 1000 },
  { maxZoom: 10, mode: 'cluster', gridMeters: 500 },
  { maxZoom: 12, mode: 'sample', gridMeters: 250 },
  { maxZoom: Number.POSITIVE_INFINITY, mode: 'points', gridMeters: null },
];

const PARKING_SAMPLE_LIMIT_DEFAULT = Number.parseInt(
  process.env.PARKING_TILE_SAMPLE_LIMIT || '',
  10,
) || 8000;

const PARKING_POINT_LIMIT_DEFAULT = Number.parseInt(
  process.env.PARKING_TILE_POINT_LIMIT || '',
  10,
) || 50000;

const PARKING_CLUSTER_SQL = `
WITH bounds AS (
  SELECT ST_TileEnvelope($1, $2, $3)::geometry(Polygon, 3857) AS geom
),
pref AS (
  SELECT
    b.geom,
    mercator_quadkey_prefix(b.geom, 16, 16) AS prefix_full,
    mercator_quadkey_prefix(b.geom, 16, $1) AS prefix_zoom,
    SUBSTRING(mercator_quadkey_prefix(b.geom, 16, 16) FROM 1 FOR 1) AS grp
  FROM bounds b
),
candidates AS (
  SELECT t.geom, t.ticket_count, t.total_fine_amount
  FROM parking_ticket_tiles t
  JOIN pref p ON true
  WHERE t.dataset = 'parking_tickets'
    AND t.min_zoom <= $1
    AND t.max_zoom >= $1
    AND t.tile_qk_group = p.grp
    AND t.tile_qk_prefix LIKE p.prefix_zoom || '%%'
    AND t.geom && p.geom
),
aggregated AS (
  SELECT
    floor(ST_X(t.geom) / $4)::bigint AS gx,
    floor(ST_Y(t.geom) / $4)::bigint AS gy,
    SUM(COALESCE(t.ticket_count, 0))::bigint AS total_count,
    SUM(COALESCE(t.total_fine_amount, 0))::numeric AS total_fine
  FROM candidates t
  GROUP BY gx, gy
),
mvt_rows AS (
  SELECT
    ST_AsMVTGeom(
      ST_SetSRID(ST_MakePoint((agg.gx + 0.5) * $4, (agg.gy + 0.5) * $4), 3857),
      p.geom,
      4096,
      64,
      true
    ) AS geom,
    agg.total_count::bigint AS count,
    agg.total_count::bigint AS ticket_count,
    agg.total_fine::numeric AS total_fine_amount,
    CASE
      WHEN agg.total_count >= 1000000 THEN to_char((agg.total_count::numeric / 1000000.0), 'FM999990.0') || 'M'
      WHEN agg.total_count >= 1000 THEN to_char((agg.total_count::numeric / 1000.0), 'FM999990.0') || 'K'
      ELSE agg.total_count::text
    END AS point_count_abbreviated,
    agg.total_count::bigint AS point_count,
    'cluster'::text AS kind,
    $4::integer AS grid_meters
  FROM aggregated agg
  JOIN pref p ON true
),
summary AS (
  SELECT
    COALESCE((SELECT ST_AsMVT(mvt_rows, 'parking_tickets', 4096, 'geom') FROM mvt_rows), '\\x'::bytea) AS mvt,
    (SELECT COUNT(*) FROM aggregated) AS feature_count,
    FALSE AS partial
)
SELECT * FROM summary;
`;

const PARKING_SAMPLE_SQL = `
WITH bounds AS (
  SELECT ST_TileEnvelope($1, $2, $3)::geometry(Polygon, 3857) AS geom
),
pref AS (
  SELECT
    b.geom,
    mercator_quadkey_prefix(b.geom, 16, 16) AS prefix_full,
    mercator_quadkey_prefix(b.geom, 16, $1) AS prefix_zoom,
    SUBSTRING(mercator_quadkey_prefix(b.geom, 16, 16) FROM 1 FOR 1) AS grp
  FROM bounds b
),
candidates AS (
  SELECT
    t.geom,
    t.ticket_count,
    t.total_fine_amount,
    t.street_normalized,
    t.centreline_id,
    t.location_name,
    t.location,
    t.ward,
    t.feature_id,
    abs(('x' || substr(md5(COALESCE(t.centreline_id::text, t.feature_id::text)), 1, 8))::bit(32)::bigint) AS hash
  FROM parking_ticket_tiles t
  JOIN pref p ON true
  WHERE t.dataset = 'parking_tickets'
    AND t.min_zoom <= $1
    AND t.max_zoom >= $1
    AND t.tile_qk_group = p.grp
    AND t.tile_qk_prefix LIKE p.prefix_zoom || '%%'
    AND t.geom && p.geom
),
ordered AS (
  SELECT *
  FROM candidates
  ORDER BY hash
  LIMIT $4
),
counts AS (
  SELECT COUNT(*) AS total_available FROM candidates
),
mvt_rows AS (
  SELECT
    ST_AsMVTGeom(o.geom, p.geom, 4096, 64, true) AS geom,
    o.ticket_count::bigint AS ticket_count,
    o.total_fine_amount::numeric AS total_fine_amount,
    o.street_normalized,
    o.centreline_id,
    o.location_name,
    o.location,
    o.ward,
    'sample'::text AS kind,
    $4::integer AS sample_limit
  FROM ordered o
  JOIN pref p ON true
),
summary AS (
  SELECT
    COALESCE((SELECT ST_AsMVT(mvt_rows, 'parking_tickets', 4096, 'geom') FROM mvt_rows), '\\x'::bytea) AS mvt,
    (SELECT COUNT(*) FROM ordered) AS feature_count,
    CASE WHEN (SELECT total_available FROM counts) > $4 THEN TRUE ELSE FALSE END AS partial
)
SELECT * FROM summary;
`;

const PARKING_POINTS_SQL = `
WITH bounds AS (
  SELECT ST_TileEnvelope($1, $2, $3)::geometry(Polygon, 3857) AS geom
),
pref AS (
  SELECT
    b.geom,
    mercator_quadkey_prefix(b.geom, 16, 16) AS prefix_full,
    mercator_quadkey_prefix(b.geom, 16, $1) AS prefix_zoom,
    SUBSTRING(mercator_quadkey_prefix(b.geom, 16, 16) FROM 1 FOR 1) AS grp
  FROM bounds b
),
ranked AS (
  SELECT
    t.geom,
    t.ticket_count,
    t.total_fine_amount,
    t.street_normalized,
    t.centreline_id,
    t.location_name,
    t.location,
    t.ward,
    t.feature_id,
    ROW_NUMBER() OVER (
      ORDER BY t.ticket_count DESC,
               t.total_fine_amount DESC,
               COALESCE(t.feature_id::text, '') DESC
    ) AS rn,
    COUNT(*) OVER () AS total_available
  FROM parking_ticket_tiles t
  JOIN pref p ON true
  WHERE t.dataset = 'parking_tickets'
    AND t.min_zoom <= $1
    AND t.max_zoom >= $1
    AND t.tile_qk_group = p.grp
    AND t.tile_qk_prefix LIKE p.prefix_zoom || '%%'
    AND t.geom && p.geom
),
selected AS (
  SELECT *
  FROM ranked
  WHERE rn <= $4
),
mvt_rows AS (
  SELECT
    ST_AsMVTGeom(s.geom, p.geom, 4096, 64, true) AS geom,
    s.ticket_count::bigint AS ticket_count,
    s.total_fine_amount::numeric AS total_fine_amount,
    s.street_normalized,
    s.centreline_id,
    s.location_name,
    s.location,
    s.ward,
    'point'::text AS kind,
    $4::integer AS point_limit
  FROM selected s
  JOIN pref p ON true
),
summary AS (
  SELECT
    COALESCE((SELECT ST_AsMVT(mvt_rows, 'parking_tickets', 4096, 'geom') FROM mvt_rows), '\\x'::bytea) AS mvt,
    COALESCE((SELECT COUNT(*) FROM selected), 0) AS feature_count,
    CASE WHEN COALESCE((SELECT MAX(total_available) FROM ranked), 0) > $4 THEN TRUE ELSE FALSE END AS partial
)
SELECT * FROM summary;
`;

function resolveParkingTileMode(zoom) {
  const sampleLimit = PARKING_SAMPLE_LIMIT_DEFAULT;
  const pointLimit = PARKING_POINT_LIMIT_DEFAULT;
  const entry = PARKING_TILE_MODE_POLICY.find((policy) => (
    Number.isFinite(policy.maxZoom) ? zoom <= policy.maxZoom : false
  )) || PARKING_TILE_MODE_POLICY[PARKING_TILE_MODE_POLICY.length - 1];

  if (!entry) {
    return {
      mode: 'points',
      gridMeters: null,
      sampleLimit,
      pointLimit,
    };
  }

  const { mode, gridMeters } = entry;

  if (mode === 'cluster') {
    return {
      mode,
      gridMeters: Number.isFinite(gridMeters) ? gridMeters : 500,
      sampleLimit,
      pointLimit,
    };
  }

  if (mode === 'sample') {
    return {
      mode,
      gridMeters: Number.isFinite(gridMeters) ? gridMeters : 250,
      sampleLimit,
      pointLimit,
    };
  }

  return {
    mode: 'points',
    gridMeters: null,
    sampleLimit,
    pointLimit,
  };
}

function buildParkingFilterKey({ mode, gridMeters, sampleLimit, pointLimit }) {
  const parts = [
    `mode=${mode}`,
    gridMeters ? `grid=${gridMeters}` : 'grid=0',
    `sample=${sampleLimit || 0}`,
    `limit=${pointLimit || 0}`,
  ];
  return parts.join('|');
}

function resolveParkingCacheTtl(modeConfig) {
  if (!modeConfig) {
    return DEFAULT_MEMORY_CACHE_SECONDS;
  }
  if (modeConfig.mode === 'cluster') {
    return 86_400;
  }
  if (modeConfig.mode === 'sample') {
    return 43_200;
  }
  return 18_000;
}

function resolveCacheTtl(zoom, rules) {
  if (!Array.isArray(rules) || rules.length === 0) {
    return DEFAULT_MEMORY_CACHE_SECONDS;
  }
  for (const rule of rules) {
    if (Number.isFinite(rule.maxZoom) && zoom <= rule.maxZoom) {
      return Math.max(30, Number.parseInt(rule.ttl, 10) || DEFAULT_MEMORY_CACHE_SECONDS);
    }
  }
  return DEFAULT_MEMORY_CACHE_SECONDS;
}

function buildCacheKey(dataset, z, x, y, filters = null) {
  const base = `${dataset}:${z}:${x}:${y}`;
  if (!filters) {
    return base;
  }
  return `${base}?${filters}`;
}

function normaliseTileCoordinate(z, x, y) {
  const worldSize = 2 ** z;
  const wrappedX = ((x % worldSize) + worldSize) % worldSize;
  const clampedY = Math.min(Math.max(y, 0), worldSize - 1);
  return { z, x: wrappedX, y: clampedY };
}

function enumerateNeighbourTiles(z, x, y, radius = 1) {
  const coords = [];
  for (let dx = -radius; dx <= radius; dx += 1) {
    for (let dy = -radius; dy <= radius; dy += 1) {
      if (dx === 0 && dy === 0) {
        continue;
      }
      coords.push(normaliseTileCoordinate(z, x + dx, y + dy));
    }
  }
  return coords;
}

function enumerateChildTiles(z, x, y) {
  const nextZoom = z + 1;
  const baseX = x * 2;
  const baseY = y * 2;
  return [
    normaliseTileCoordinate(nextZoom, baseX, baseY),
    normaliseTileCoordinate(nextZoom, baseX + 1, baseY),
    normaliseTileCoordinate(nextZoom, baseX, baseY + 1),
    normaliseTileCoordinate(nextZoom, baseX + 1, baseY + 1),
  ];
}

function createAbortError(reason) {
  const error = reason instanceof Error ? reason : new Error(reason || 'Tile request aborted');
  error.name = 'AbortError';
  return error;
}

function throwIfAborted(signal) {
  if (signal?.aborted) {
    throw createAbortError(signal.reason);
  }
}

class MemoryTileCache {
  constructor(limit) {
    this.limit = Number.isFinite(limit) && limit > 0 ? limit : DEFAULT_MEMORY_CACHE_LIMIT;
    this.map = new Map();
    this.evictCount = 0;
  }

  get(key) {
    const entry = this.map.get(key);
    if (!entry) {
      return null;
    }
    if (entry.expiresAt !== null && entry.expiresAt <= Date.now()) {
      this.map.delete(key);
      return null;
    }
    entry.hits += 1;
    entry.lastAccess = Date.now();
    return {
      ...entry.payload,
      source: 'postgis-memory',
      cacheSeconds: entry.cacheSeconds,
    };
  }

  set(key, payload, ttlSeconds) {
    const expiresAt = Number.isFinite(ttlSeconds) && ttlSeconds > 0
      ? Date.now() + ttlSeconds * 1000
      : null;
    this.map.set(key, {
      payload,
      cacheSeconds: ttlSeconds,
      expiresAt,
      insertedAt: Date.now(),
      lastAccess: Date.now(),
      hits: 0,
    });
    this.evictIfNeeded();
  }

  evictIfNeeded() {
    if (this.map.size <= this.limit) {
      return;
    }
    const entries = Array.from(this.map.entries());
    entries.sort((a, b) => {
      const left = a[1];
      const right = b[1];
      if (left.hits !== right.hits) {
        return left.hits - right.hits;
      }
      return left.lastAccess - right.lastAccess;
    });
    const overflow = this.map.size - this.limit;
    for (let i = 0; i < overflow; i += 1) {
      const target = entries[i];
      if (target) {
        this.map.delete(target[0]);
      }
    }
    // Memory optimization: Trigger GC every 50 evictions
    this.evictCount += overflow;
    if (this.evictCount >= 50) {
      this.evictCount = 0;
      tryGC();
    }
  }

  // Memory optimization: Clear all entries and trigger GC
  clear() {
    this.map.clear();
    tryGC();
  }

  // Memory optimization: Get current memory pressure estimate
  get size() {
    return this.map.size;
  }
}

class PostgisTileService {
  constructor() {
    this.pool = null;
    this.poolSignature = null;
    this.memoryCache = new MemoryTileCache(DEFAULT_MEMORY_CACHE_LIMIT);
    this.inflight = new Map();
    this.enabled = false;
    this.lastConfigCheck = 0;
    this.statementTimeoutMs = 0;
    this.hardDisabled = false;
    this.prefetchQueue = new Map();
    this.prefetchTimer = null;
    this.prefetchProcessing = false;
  }

  isEnabled() {
    this.ensurePool();
    return this.enabled;
  }

  isDatasetEnabled(dataset) {
    const config = DATASET_CONFIG[dataset];
    if (!config) {
      return false;
    }
    return this.isEnabled();
  }

  ensurePool() {
    const now = Date.now();
    if (this.hardDisabled) {
      this.enabled = false;
      return null;
    }
    if (this.pool && this.poolSignature && now - this.lastConfigCheck < 30_000) {
      return this.pool;
    }
    this.lastConfigCheck = now;
    const pgConfig = getTileDbConfig();
    const connectionString = pgConfig.readOnlyConnectionString || pgConfig.connectionString;
    if (!pgConfig.enabled || !connectionString) {
      this.enabled = false;
      return null;
    }
    const signature = `${connectionString}|${pgConfig.ssl ? 'ssl' : 'plain'}`;
    if (!this.pool || this.poolSignature !== signature) {
      if (this.pool) {
        this.pool.end().catch(() => {
          /* ignore */
        });
      }
      this.pool = new Pool({
        connectionString,
        ssl: pgConfig.ssl,
        max: Number.parseInt(process.env.POSTGIS_TILE_POOL_SIZE || '', 10) || 6,
        idleTimeoutMillis: Number.parseInt(process.env.POSTGIS_TILE_POOL_IDLE_MS || '', 10) || 30_000,
        application_name: 'tile-service-postgis',
      });
      this.pool.on('error', (error) => {
        console.warn('[postgis-tiles] pool error:', error.message);
      });
      this.poolSignature = signature;
    }
    this.statementTimeoutMs = Number.isFinite(pgConfig.statementTimeoutMs) && pgConfig.statementTimeoutMs > 0
      ? pgConfig.statementTimeoutMs
      : 0;
    this.enabled = true;
    return this.pool;
  }

  disableService(reason) {
    if (!this.enabled) {
      return;
    }
    console.warn('[postgis-tiles] disabling service:', reason);
    this.enabled = false;
    this.hardDisabled = true;
    this.poolSignature = null;
    if (this.pool) {
      this.pool.end().catch(() => {
        /* ignore */
      });
      this.pool = null;
    }
    this.prefetchQueue.clear();
    if (this.prefetchTimer) {
      clearTimeout(this.prefetchTimer);
      this.prefetchTimer = null;
    }
    this.prefetchProcessing = false;
  }

  queuePrefetch(dataset, z, x, y) {
    if (!PREFETCH_ENABLED) {
      return;
    }
    const config = DATASET_CONFIG[dataset];
    if (!config) {
      return;
    }
    const maxZoom = Number.isFinite(config.prefetchMaxZoom)
      ? config.prefetchMaxZoom
      : PREFETCH_MAX_ZOOM;
    if (!Number.isFinite(maxZoom) || z > maxZoom) {
      return;
    }

    const neighbours = enumerateNeighbourTiles(z, x, y, PREFETCH_RADIUS);
    const shouldPrefetchChildren = z < Math.min(maxZoom, PREFETCH_CHILD_ZOOM_MAX);
    const childTiles = shouldPrefetchChildren ? enumerateChildTiles(z, x, y) : [];

    const candidates = [...neighbours, ...childTiles];
    for (const coord of candidates) {
      if (this.prefetchQueue.size >= PREFETCH_MAX_QUEUE) {
        break;
      }
      const modeConfig = dataset === 'parking_tickets' ? resolveParkingTileMode(coord.z) : null;
      const filterKey = dataset === 'parking_tickets' ? buildParkingFilterKey(modeConfig) : null;
      const cacheKey = buildCacheKey(dataset, coord.z, coord.x, coord.y, filterKey);
      if (this.memoryCache.get(cacheKey)) {
        continue;
      }
      const queueKey = `${dataset}:${coord.z}:${coord.x}:${coord.y}`;
      if (this.prefetchQueue.has(queueKey)) {
        continue;
      }
      this.prefetchQueue.set(queueKey, {
        dataset,
        z: coord.z,
        x: coord.x,
        y: coord.y,
        filterKey,
        mode: modeConfig,
      });
    }

    if (this.prefetchQueue.size === 0) {
      return;
    }

    if (!this.prefetchTimer && !this.prefetchProcessing) {
      this.prefetchTimer = setTimeout(() => this.processPrefetchQueue(), PREFETCH_DELAY_MS);
    }
  }

  async processPrefetchQueue() {
    if (!PREFETCH_ENABLED) {
      this.prefetchTimer = null;
      this.prefetchQueue.clear();
      return;
    }
    if (this.prefetchProcessing) {
      if (!this.prefetchTimer) {
        this.prefetchTimer = setTimeout(() => this.processPrefetchQueue(), PREFETCH_DELAY_MS);
      }
      return;
    }

    this.prefetchTimer = null;
    if (this.prefetchQueue.size === 0) {
      return;
    }

    const pool = this.ensurePool();
    if (!pool) {
      this.prefetchQueue.clear();
      return;
    }

    this.prefetchProcessing = true;
    try {
      const grouped = new Map();
      let processed = 0;
      for (const [queueKey, entry] of this.prefetchQueue) {
        if (processed >= PREFETCH_BATCH_SIZE) {
          break;
        }
        const config = DATASET_CONFIG[entry.dataset];
        if (!config) {
          this.prefetchQueue.delete(queueKey);
          continue;
        }
        const cacheKey = buildCacheKey(entry.dataset, entry.z, entry.x, entry.y, entry.filterKey || null);
        if (this.memoryCache.get(cacheKey)) {
          this.prefetchQueue.delete(queueKey);
          continue;
        }
        this.prefetchQueue.delete(queueKey);
        if (!grouped.has(entry.dataset)) {
          grouped.set(entry.dataset, []);
        }
        grouped.get(entry.dataset).push(entry);
        processed += 1;
      }

      for (const [dataset, coords] of grouped) {
        const config = DATASET_CONFIG[dataset];
        if (!config || coords.length === 0) {
          continue;
        }
        try {
          await this.fetchBatchTiles({
            pool,
            dataset,
            config,
            coords,
          });
        } catch (error) {
          if (error?.name !== 'AbortError') {
            console.warn('[postgis-tiles] prefetch error:', error?.message || error);
          }
        }
      }
    } finally {
      this.prefetchProcessing = false;
      if (this.prefetchQueue.size > 0) {
        this.prefetchTimer = setTimeout(() => this.processPrefetchQueue(), PREFETCH_DELAY_MS);
      }
    }
  }

  async fetchBatchTiles({ pool, dataset, config, coords }) {
    if (!Array.isArray(coords) || coords.length === 0) {
      return;
    }

    const unique = [];
    const seen = new Set();
    for (const coord of coords) {
      const modeConfig = dataset === 'parking_tickets'
        ? coord.mode || resolveParkingTileMode(coord.z)
        : null;
      const filterKey = dataset === 'parking_tickets'
        ? coord.filterKey || buildParkingFilterKey(modeConfig)
        : coord.filterKey || null;
      const cacheKey = buildCacheKey(dataset, coord.z, coord.x, coord.y, filterKey);
      if (this.memoryCache.get(cacheKey)) {
        continue;
      }
      const key = `${dataset}:${coord.z}:${coord.x}:${coord.y}?${filterKey || ''}`;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      unique.push({
        ...coord,
        filterKey,
        cacheKey,
        mode: modeConfig,
      });
    }

    if (unique.length === 0) {
      return;
    }

    if (dataset === 'parking_tickets') {
      const client = await pool.connect();
      try {
        for (const coord of unique) {
          const modeConfig = coord.mode || resolveParkingTileMode(coord.z);
          const filterKey = coord.filterKey || buildParkingFilterKey(modeConfig);
          const cacheKey = coord.cacheKey || buildCacheKey(dataset, coord.z, coord.x, coord.y, filterKey);
          await this.fetchAndCacheTile({
            pool,
            client,
            dataset,
            config,
            z: coord.z,
            x: coord.x,
            y: coord.y,
            cacheKey,
            signal: null,
            mode: modeConfig,
            skipPrefetchLogging: true,
          });
        }
      } finally {
        client.release();
      }
      return;
    }

    const zs = unique.map((coord) => coord.z);
    const xs = unique.map((coord) => coord.x);
    const ys = unique.map((coord) => coord.y);

    const client = await pool.connect();
    try {
      if (this.statementTimeoutMs > 0) {
        const timeout = Math.max(0, Math.floor(this.statementTimeoutMs));
        await client.query(`SET SESSION statement_timeout = ${timeout}`);
      }

      const query = {
        text: `SELECT z, x, y, mvt FROM ${config.functionName}($1::integer[], $2::integer[], $3::integer[])`,
        values: [zs, xs, ys],
      };

      let result;
      try {
        result = await client.query(query);
      } catch (error) {
        if (error?.code === '42P01' || error?.code === '42883') {
          this.disableService(error.message || error.code);
        }
        throw error;
      }

      if (!result?.rows?.length) {
        return;
      }

      for (const row of result.rows) {
        const buffer = row?.mvt;
        if (!buffer || !Buffer.isBuffer(buffer) || buffer.length === 0) {
          continue;
        }
        const ttlSeconds = resolveCacheTtl(row.z, config.ttlRules);
        const digest = createHash('sha1').update(buffer).digest('hex');
        const etag = `W/"pg-${dataset}-${row.z}-${row.x}-${row.y}-${digest}"`;
        const payload = {
          buffer,
          version: digest,
          etag,
          lastModified: null,
          source: 'postgis-db',
          cacheSeconds: ttlSeconds,
          queryDurationMs: null,
        };
        const cacheKey = buildCacheKey(dataset, row.z, row.x, row.y, null);
        this.memoryCache.set(cacheKey, payload, ttlSeconds);
      }
    } finally {
      client.release();
    }
  }

  async getTile(dataset, z, x, y, options = {}) {
    const config = DATASET_CONFIG[dataset];
    if (!config) {
      return null;
    }
    if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y)) {
      return null;
    }
    if (Number.isFinite(config.minZoom) && z < config.minZoom) {
      return null;
    }

    const pool = this.ensurePool();
    if (!pool) {
      return null;
    }

    const parkingMode = dataset === 'parking_tickets' ? resolveParkingTileMode(z) : null;
    const filterKey = dataset === 'parking_tickets' ? buildParkingFilterKey(parkingMode) : null;
    const cacheKey = buildCacheKey(dataset, z, x, y, filterKey);
    const cached = this.memoryCache.get(cacheKey);
    if (cached) {
      console.debug(
        '[postgis-tiles] cache-hit',
        JSON.stringify({ dataset, z, x, y, mode: cached.mode || parkingMode?.mode || 'unknown', source: cached.source }),
      );
      return {
        ...cached,
        source: 'memory',
      };
    }

    if (this.inflight.has(cacheKey)) {
      return this.inflight.get(cacheKey);
    }

    const isPrefetch = options.prefetch === true;

    const promise = this.fetchAndCacheTile({
      pool,
      dataset,
      config,
      z,
      x,
      y,
      cacheKey,
      signal: options.signal,
      prefetch: isPrefetch,
      mode: parkingMode,
    })
      .then((payload) => {
        if (payload && !isPrefetch) {
          this.queuePrefetch(dataset, z, x, y);
        }
        return payload;
      })
      .catch((error) => {
        if (error?.name !== 'AbortError') {
          console.warn('[postgis-tiles] fetch error:', error.message);
        }
        throw error;
      })
      .finally(() => {
        this.inflight.delete(cacheKey);
      });

    this.inflight.set(cacheKey, promise);
    return promise;
  }

  async fetchAndCacheTile({
    pool,
    dataset,
    config,
    z,
    x,
    y,
    cacheKey,
    signal,
    mode,
    client: providedClient = null,
    skipPrefetchLogging = false,
  }) {
    throwIfAborted(signal);

    const client = providedClient || await pool.connect();
    try {
      throwIfAborted(signal);
      const start = performance.now();
      if (this.statementTimeoutMs > 0) {
        const timeout = Math.max(0, Math.floor(this.statementTimeoutMs));
        await client.query(`SET SESSION statement_timeout = ${timeout}`);
      }
      let payload = null;

      if (dataset === 'parking_tickets') {
        const modeConfig = mode || resolveParkingTileMode(z);
        const query = {
          text: modeConfig.mode === 'cluster'
            ? PARKING_CLUSTER_SQL
            : modeConfig.mode === 'sample'
              ? PARKING_SAMPLE_SQL
              : PARKING_POINTS_SQL,
          values: modeConfig.mode === 'cluster'
            ? [z, x, y, modeConfig.gridMeters]
            : modeConfig.mode === 'sample'
              ? [z, x, y, modeConfig.sampleLimit]
              : [z, x, y, modeConfig.pointLimit],
        };
        if (signal) {
          query.signal = signal;
        }

        let result;
        try {
          result = await client.query(query);
        } catch (error) {
          if (error?.code === '42P01' || error?.code === '42883') {
            this.disableService(error.message || error.code);
          }
          throw error;
        }

        const duration = performance.now() - start;
        const row = result?.rows?.[0] || null;
        let buffer = row?.mvt;
        const featureCount = Number(row?.feature_count || 0);
        const partial = Boolean(row?.partial);
        if (!buffer || !Buffer.isBuffer(buffer) || buffer.length === 0) {
          const emptyBuffer = getEmptyTileBuffer(config.layer || dataset);
          buffer = Buffer.from(emptyBuffer);
        }
        const ttlSeconds = resolveParkingCacheTtl(modeConfig);
        const digest = createHash('sha1').update(buffer).digest('hex');
        const etag = `W/"pg-${dataset}-${modeConfig.mode}-${z}-${x}-${y}-${digest}"`;
        payload = {
          buffer,
          version: digest,
          etag,
          lastModified: null,
          source: 'postgis-db',
          cacheSeconds: ttlSeconds,
          queryDurationMs: Number(duration.toFixed(2)),
          featureCount,
          partial,
          mode: modeConfig.mode,
          gridMeters: modeConfig.gridMeters,
          sampleLimit: modeConfig.sampleLimit,
          pointLimit: modeConfig.pointLimit,
        };

        if (!skipPrefetchLogging) {
          console.info('[postgis-tiles] fetch', JSON.stringify({
            dataset,
            z,
            x,
            y,
            mode: modeConfig.mode,
            source: 'db',
            durationMs: payload.queryDurationMs,
            features: featureCount,
            partial,
          }));
        }

        this.memoryCache.set(cacheKey, payload, ttlSeconds);
        return payload;
      }

      const query = {
        text: `SELECT z, x, y, mvt FROM ${config.functionName}($1::integer[], $2::integer[], $3::integer[]) WHERE z = $4 AND x = $5 AND y = $6`,
        values: [[z], [x], [y], z, x, y],
      };
      if (signal) {
        query.signal = signal;
      }
      let result;
      try {
        result = await client.query(query);
      } catch (error) {
        if (error?.code === '42P01' || error?.code === '42883') {
          this.disableService(error.message || error.code);
        }
        throw error;
      }
      const duration = performance.now() - start;
      if (!result?.rows?.length) {
        return null;
      }

      const row = result.rows[0];
      const buffer = row?.mvt;
      if (!buffer || !Buffer.isBuffer(buffer) || buffer.length === 0) {
        return null;
      }

      const ttlSeconds = resolveCacheTtl(z, config.ttlRules);
      const digest = createHash('sha1').update(buffer).digest('hex');
      const etag = `W/"pg-${dataset}-${z}-${x}-${y}-${digest}"`;
      payload = {
        buffer,
        version: digest,
        etag,
        lastModified: null,
        source: 'postgis-db',
        cacheSeconds: ttlSeconds,
        queryDurationMs: Number(duration.toFixed(2)),
      };

      this.memoryCache.set(cacheKey, payload, ttlSeconds);
      return payload;
    } finally {
      if (!providedClient) {
        client.release();
      }
    }
  }
}

export function createPostgisTileService() {
  const service = new PostgisTileService();
  service.ensurePool();
  return service;
}

export { DATASET_CONFIG };
