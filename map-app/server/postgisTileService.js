import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import process from 'node:process';
import { performance } from 'node:perf_hooks';
import { Pool } from 'pg';
import { getPostgresConfig } from './runtimeConfig.js';
import { TICKET_TILE_MIN_ZOOM } from '../shared/mapConstants.js';

const DEFAULT_MEMORY_CACHE_LIMIT = Number.parseInt(
  process.env.POSTGIS_TILE_MEMORY_CACHE_LIMIT || '',
  10,
) || 512;

const DEFAULT_MEMORY_CACHE_SECONDS = Number.parseInt(
  process.env.POSTGIS_TILE_MEMORY_CACHE_SECONDS || '',
  10,
) || 300;

const DEFAULT_CACHE_RULES = [
  { maxZoom: 10, ttl: 60 * 60 },
  { maxZoom: 13, ttl: 15 * 60 },
  { maxZoom: Number.POSITIVE_INFINITY, ttl: 5 * 60 },
];

const DATASET_CONFIG = {
  parking_tickets: {
    functionName: 'public.get_parking_tiles',
    minZoom: TICKET_TILE_MIN_ZOOM,
    ttlRules: DEFAULT_CACHE_RULES,
    layer: 'parking_tickets',
  },
  red_light_locations: {
    functionName: 'public.get_red_light_tiles',
    minZoom: 8,
    ttlRules: DEFAULT_CACHE_RULES,
    layer: 'red_light_locations',
  },
  ase_locations: {
    functionName: 'public.get_ase_tiles',
    minZoom: 8,
    ttlRules: DEFAULT_CACHE_RULES,
    layer: 'ase_locations',
  },
};

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
    if (this.pool && this.poolSignature && now - this.lastConfigCheck < 30_000) {
      return this.pool;
    }
    this.lastConfigCheck = now;
    const pgConfig = getPostgresConfig();
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
    this.enabled = true;
    return this.pool;
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

    const filterKey = null;
    const cacheKey = buildCacheKey(dataset, z, x, y, filterKey);
    const cached = this.memoryCache.get(cacheKey);
    if (cached) {
      return {
        ...cached,
        source: 'memory',
      };
    }

    if (this.inflight.has(cacheKey)) {
      return this.inflight.get(cacheKey);
    }

    const promise = this.fetchAndCacheTile({
      pool,
      dataset,
      config,
      z,
      x,
      y,
      cacheKey,
      signal: options.signal,
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

  async fetchAndCacheTile({ pool, dataset, config, z, x, y, cacheKey, signal }) {
    throwIfAborted(signal);

    const client = await pool.connect();
    try {
      throwIfAborted(signal);
      const start = performance.now();
      const query = {
        text: `SELECT z, x, y, mvt FROM ${config.functionName}($1::integer[], $2::integer[], $3::integer[]) WHERE z = $4 AND x = $5 AND y = $6`,
        values: [[z], [x], [y], z, x, y],
      };
      if (signal) {
        query.signal = signal;
      }
      const result = await client.query(query);
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
      const payload = {
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
      client.release();
    }
  }
}

export function createPostgisTileService() {
  const service = new PostgisTileService();
  service.ensurePool();
  return service;
}

export { DATASET_CONFIG };
