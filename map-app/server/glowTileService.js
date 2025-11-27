import process from 'node:process';
import { Buffer } from 'node:buffer';
import { gzipSync } from 'node:zlib';
import { performance } from 'node:perf_hooks';
import path from 'node:path';
import fs from 'node:fs/promises';

import { Pool } from 'pg';
import { createClient } from 'redis';
import geojsonvt from 'geojson-vt';
import vtpbf from 'vt-pbf';

import { getTileDbConfig, getRedisConfig } from './runtimeConfig.js';

const ALLOWED_DATASETS = new Set(['parking_tickets', 'red_light_locations', 'ase_locations']);
const CACHE_NAMESPACE = process.env.MAP_DATA_REDIS_NAMESPACE || 'toronto:map-data';
const CACHE_VERSION = process.env.GLOW_TILE_CACHE_VERSION || 'v1';
const CACHE_PREFIX = `${CACHE_NAMESPACE}:glow:tiles:${CACHE_VERSION}`;
const CACHE_TTL_SECONDS = Number.parseInt(process.env.GLOW_TILE_CACHE_TTL || '', 10) || 24 * 60 * 60;
const EMPTY_CACHE_TTL_SECONDS = Number.parseInt(process.env.GLOW_TILE_CACHE_EMPTY_TTL || '', 10) || 60 * 60;
const MAX_CACHE_BYTES = Number.parseInt(process.env.GLOW_TILE_CACHE_MAX_BYTES || '', 10) || 1_000_000;

const EMPTY_TILE_RAW = Buffer.alloc(0);
const EMPTY_TILE_GZIP = gzipSync(EMPTY_TILE_RAW);
const VECTOR_LAYER_NAME = 'glow_lines';

const DATASET_FILE_MAP = {
  parking_tickets: path.resolve(process.cwd(), 'public/data/tickets_glow_lines.geojson'),
  red_light_locations: path.resolve(process.cwd(), 'public/data/red_light_glow_lines.geojson'),
  ase_locations: path.resolve(process.cwd(), 'public/data/ase_glow_lines.geojson'),
};

const geojsonIndexCache = new Map();
const geojsonIndexPromises = new Map();

function buildCacheKey(dataset, z, x, y) {
  return `${CACHE_PREFIX}:${dataset}:${z}:${x}:${y}`;
}

function isValidTileCoordinate(value) {
  return Number.isInteger(value) && value >= 0 && value < 2 ** 25;
}

class GlowTileService {
  constructor() {
    this.pool = null;
    this.poolSignature = null;
    this.redisClientPromise = null;
    this.redisSupported = false;
    this.dbAvailable = false;
    this.fallbackWarned = false;
    this.setup();
  }

  setup() {
    const dbConfig = getTileDbConfig();
    if (!dbConfig?.enabled || !dbConfig.connectionString) {
      this.pool = null;
      this.poolSignature = null;
    } else if (dbConfig.connectionString !== this.poolSignature) {
      if (this.pool) {
        this.pool.end().catch(() => {
          /* ignore */
        });
      }
      this.pool = new Pool({ connectionString: dbConfig.connectionString, ssl: dbConfig.ssl, max: 4 });
      this.poolSignature = dbConfig.connectionString;
    }
    this.dbAvailable = Boolean(this.pool);

    const redisConfig = getRedisConfig();
    this.redisSupported = Boolean(redisConfig.enabled && redisConfig.url);
    if (!this.redisSupported) {
      this.redisClientPromise = null;
    }
  }

  async getRedisClient() {
    if (!this.redisSupported) {
      return null;
    }
    if (!this.redisClientPromise) {
      const redisConfig = getRedisConfig();
      if (!redisConfig.enabled || !redisConfig.url) {
        this.redisSupported = false;
        return null;
      }
      this.redisClientPromise = createClient({ url: redisConfig.url })
        .on('error', (error) => {
          console.warn('[glow-tiles] Redis error:', error?.message || error);
        })
        .connect()
        .catch((error) => {
          console.warn('[glow-tiles] Failed to connect to Redis:', error?.message || error);
          this.redisClientPromise = null;
          this.redisSupported = false;
          return null;
        });
    }
    return this.redisClientPromise;
  }

  async getFromCache(key) {
    const client = await this.getRedisClient();
    if (!client) {
      return null;
    }
    try {
      const response = await client.sendCommand(['GET', key], { returnBuffers: true });
      return response instanceof Buffer ? response : null;
    } catch (error) {
      console.warn('[glow-tiles] Redis GET failed:', error?.message || error);
      return null;
    }
  }

  async storeInCache(key, value, ttlSeconds) {
    const client = await this.getRedisClient();
    if (!client) {
      return;
    }
    if (!(value instanceof Buffer) || value.length === 0) {
      return;
    }
    if (value.length > MAX_CACHE_BYTES) {
      return;
    }
    const ttl = Number.isFinite(ttlSeconds) && ttlSeconds > 0 ? Math.floor(ttlSeconds) : CACHE_TTL_SECONDS;
    try {
      await client.sendCommand(['SET', key, value, 'EX', ttl.toString()]);
    } catch (error) {
      console.warn('[glow-tiles] Redis SET failed:', error?.message || error);
    }
  }

  async fetchFromDatabase(dataset, z, x, y, signal) {
    if (!this.pool) {
      throw new Error('Tile database pool is not configured');
    }
    const client = await this.pool.connect();
    try {
      if (signal?.aborted) {
        throw new Error('Aborted');
      }
      const result = await client.query('SELECT public.get_glow_tile($1, $2, $3, $4) AS tile', [
        dataset,
        z,
        x,
        y,
      ]);
      const tile = result?.rows?.[0]?.tile;
      if (!tile || tile.length === 0) {
        return { raw: EMPTY_TILE_RAW, compressed: EMPTY_TILE_GZIP, empty: true };
      }
      const rawBuffer = Buffer.isBuffer(tile) ? tile : Buffer.from(tile);
      const compressed = gzipSync(rawBuffer, { level: 6 });
      return { raw: rawBuffer, compressed, empty: rawBuffer.length === 0 };
    } finally {
      client.release();
    }
  }

  async loadTileIndex(dataset) {
    if (!DATASET_FILE_MAP[dataset]) {
      return null;
    }
    if (geojsonIndexCache.has(dataset)) {
      return geojsonIndexCache.get(dataset);
    }
    if (geojsonIndexPromises.has(dataset)) {
      return geojsonIndexPromises.get(dataset);
    }

    const loadPromise = fs.readFile(DATASET_FILE_MAP[dataset], 'utf8')
      .then((raw) => JSON.parse(raw))
      .then((geojson) => geojsonvt(geojson, {
        maxZoom: 16,
        indexMaxZoom: 14,
        tolerance: 3,
        extent: 4096,
        buffer: 64,
        lineMetrics: true,
      }))
      .then((index) => {
        geojsonIndexCache.set(dataset, index);
        return index;
      })
      .catch((error) => {
        console.warn(`[glow-tiles] Failed to build geojson index for ${dataset}:`, error?.message || error);
        return null;
      })
      .finally(() => {
        geojsonIndexPromises.delete(dataset);
      });

    geojsonIndexPromises.set(dataset, loadPromise);
    return loadPromise;
  }

  async fetchFromGeoJson(dataset, z, x, y) {
    const index = await this.loadTileIndex(dataset);
    if (!index) {
      return { raw: EMPTY_TILE_RAW, compressed: EMPTY_TILE_GZIP, empty: true };
    }
    const tile = index.getTile(z, x, y);
    if (!tile || !tile.features || tile.features.length === 0) {
      return { raw: EMPTY_TILE_RAW, compressed: EMPTY_TILE_GZIP, empty: true };
    }
    const layer = { [VECTOR_LAYER_NAME]: tile };
    const rawBuffer = vtpbf.fromGeojsonVt(layer, { version: 2, extent: 4096 });
    const buffer = Buffer.isBuffer(rawBuffer) ? rawBuffer : Buffer.from(rawBuffer);
    const compressed = gzipSync(buffer, { level: 6 });
    return { raw: buffer, compressed, empty: false };
  }

  async getTile({ dataset, z, x, y, signal }) {
    if (!ALLOWED_DATASETS.has(dataset)) {
      const allowed = Array.from(ALLOWED_DATASETS).join(', ');
      throw new Error(`Unsupported glow dataset "${dataset}" (allowed: ${allowed})`);
    }
    if (![z, x, y].every(isValidTileCoordinate)) {
      throw new Error('Invalid tile coordinates');
    }

    const cacheKey = buildCacheKey(dataset, z, x, y);
    const startedAt = performance.now();
    let cacheStatus = 'miss';

    const cached = await this.getFromCache(cacheKey);
    if (cached) {
      cacheStatus = 'hit';
      return {
        buffer: cached,
        empty: cached.equals(EMPTY_TILE_GZIP),
        cacheStatus,
        durationMs: performance.now() - startedAt,
        size: cached.length,
      };
    }

    let tile;
    if (this.dbAvailable && this.pool) {
      try {
        tile = await this.fetchFromDatabase(dataset, z, x, y, signal);
      } catch (error) {
        this.dbAvailable = false;
        if (!this.fallbackWarned) {
          console.warn('[glow-tiles] Database glow tiles unavailable, falling back to GeoJSON tiling:', error?.message || error);
          this.fallbackWarned = true;
        }
        tile = await this.fetchFromGeoJson(dataset, z, x, y);
      }
    } else {
      tile = await this.fetchFromGeoJson(dataset, z, x, y);
    }
    if (!tile) {
      tile = { raw: EMPTY_TILE_RAW, compressed: EMPTY_TILE_GZIP, empty: true };
    }
    const ttl = tile.empty ? EMPTY_CACHE_TTL_SECONDS : CACHE_TTL_SECONDS;
    await this.storeInCache(cacheKey, tile.compressed, ttl);

    return {
      buffer: tile.compressed,
      empty: tile.empty,
      cacheStatus,
      durationMs: performance.now() - startedAt,
      size: tile.compressed.length,
    };
  }
}

const glowTileService = new GlowTileService();

export default glowTileService;
