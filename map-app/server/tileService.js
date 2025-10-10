import process from 'node:process';
import { Buffer } from 'node:buffer';
import fs from 'node:fs/promises';
import path from 'node:path';
import { brotliCompressSync, brotliDecompressSync } from 'node:zlib';
import { Pool } from 'pg';
import Supercluster from 'supercluster';
import geojsonvt from 'geojson-vt';
import vtpbf from 'vt-pbf';
import Flatbush from 'flatbush';
import { createClient } from 'redis';
import { normalizeStreetName } from '../shared/streetUtils.js';
import {
  RAW_POINT_ZOOM_THRESHOLD,
  SUMMARY_ZOOM_THRESHOLD,
  TICKET_TILE_MIN_ZOOM,
  TILE_LAYER_NAME,
  WARD_TILE_SOURCE_LAYER,
} from '../shared/mapConstants.js';
import {
  getTicketChunks,
  loadTicketChunk,
  getTicketsRaw,
  loadCameraWardGeojson,
} from './ticketsDataStore.js';
import { getParkingLocationYearMap } from './yearlyMetricsService.js';
import { getRedisConfig, getPostgresConfig } from './runtimeConfig.js';

const TILE_CACHE_LIMIT = 512;
const SUMMARY_LIMIT = 5;
const YEARS_LIMIT = 32;
const MONTHS_LIMIT = 24;

const redisSettings = getRedisConfig();
const TILE_REDIS_ENABLED = Boolean(redisSettings.enabled && redisSettings.url);
const TILE_REDIS_NAMESPACE = process.env.MAP_DATA_REDIS_NAMESPACE || 'toronto:map-data';
const TILE_REDIS_PREFIX = `${TILE_REDIS_NAMESPACE}:tiles:parking:v3`;
const TILE_REDIS_TTL_OVERRIDE = Number.parseInt(process.env.MAP_TILE_REDIS_TTL || '', 10);
const TILE_TTL_RULES = [
  { maxZoom: 11, ttl: 60 * 60 * 24 },
  { maxZoom: 14, ttl: 60 * 60 * 2 },
  { maxZoom: Number.POSITIVE_INFINITY, ttl: 60 * 10 },
];
const TILE_PAYLOAD_FLAG_RAW = 0;
const TILE_PAYLOAD_FLAG_BROTLI = 1;
const TILE_PREWARM_ENABLED = process.env.MAP_TILE_PREWARM === '0' ? false : true;
const TILE_PREWARM_INTERVAL_MS = Number.parseInt(
  process.env.MAP_TILE_PREWARM_INTERVAL_MS || '',
  10,
) || 60 * 60 * 1000;
const TILE_PREWARM_INITIAL_DELAY_MS = Number.parseInt(
  process.env.MAP_TILE_PREWARM_INITIAL_DELAY_MS || '',
  10,
) || 30_000;
const TILE_PREWARM_JITTER_MS = Number.parseInt(
  process.env.MAP_TILE_PREWARM_JITTER_MS || '',
  10,
) || 5_000;
const GTA_BOUNDS = {
  west: Number.parseFloat(process.env.TILE_PREWARM_WEST ?? '-79.6393'),
  south: Number.parseFloat(process.env.TILE_PREWARM_SOUTH ?? '43.4032'),
  east: Number.parseFloat(process.env.TILE_PREWARM_EAST ?? '-79.1169'),
  north: Number.parseFloat(process.env.TILE_PREWARM_NORTH ?? '43.8554'),
};
const TILE_PREWARM_ZOOMS = [8, 9, 10, 11, 12, 13, 14];
const PARKING_TILE_SNAPSHOT_DIR = process.env.PARKING_TILE_SNAPSHOT_DIR
  || path.resolve(process.cwd(), 'map-app/.cache/parking-tile-snapshots');
const PARKING_TILE_SNAPSHOT_LIMIT = Number.parseInt(
  process.env.PARKING_TILE_SNAPSHOT_LIMIT || '',
  10,
) || 256;

const BROTLI_LEGACY_REWRITE_KEYS = new Set();

const TILE_HARD_TIMEOUT_MS = Number.parseInt(process.env.TILE_HARD_MS || '', 10)
  || 450;
const MAX_ACTIVE_RENDERS = (() => {
  const parsed = Number.parseInt(process.env.MAX_ACTIVE_RENDERS || '', 10);
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return 6;
})();
const TILE_REVALIDATE_DELAY_MS = Number.parseInt(process.env.TILE_REVALIDATE_DELAY_MS || '', 10)
  || 25;

const EMPTY_TILE_BUFFER = vtpbf.fromGeojsonVt({}, { extent: 4096, version: 2 });

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

class AsyncSemaphore {
  constructor(limit) {
    this.limit = Number.isFinite(limit) && limit > 0 ? limit : 0;
    this.active = 0;
    this.queue = [];
  }

  async acquire(signal) {
    if (this.limit <= 0) {
      return () => {};
    }
    throwIfAborted(signal);
    if (this.active < this.limit) {
      this.active += 1;
      return () => this._release();
    }
    return await new Promise((resolve, reject) => {
      const entry = {
        resolve,
        reject,
        signal,
      };
      const abortHandler = () => {
        this.queue = this.queue.filter((item) => item !== entry);
        reject(createAbortError(signal.reason));
      };
      if (signal) {
        signal.addEventListener('abort', abortHandler, { once: true });
        entry.abortHandler = abortHandler;
      }
      this.queue.push(entry);
    });
  }

  _release() {
    if (this.limit <= 0) {
      return;
    }
    this.active = Math.max(0, this.active - 1);
    if (this.queue.length === 0) {
      return;
    }
    const next = this.queue.shift();
    if (!next) {
      return;
    }
    if (next.abortHandler && next.signal?.aborted) {
      return;
    }
    if (next.abortHandler && next.signal) {
      next.signal.removeEventListener('abort', next.abortHandler);
    }
    this.active += 1;
    next.resolve(() => this._release());
  }
}

const renderSemaphore = MAX_ACTIVE_RENDERS > 0
  ? new AsyncSemaphore(MAX_ACTIVE_RENDERS)
  : null;
let tileRedisPromise = null;
let tilePostgresPool = null;
let tilePostgresSignature = null;
const parkingTileSnapshotCache = new Map();

async function getTileRedisClient() {
  if (!TILE_REDIS_ENABLED) {
    return null;
  }
  if (tileRedisPromise) {
    try {
      const existing = await tileRedisPromise;
      if (existing && existing.isOpen) {
        return existing;
      }
    } catch (error) {
      console.warn('Vector tile Redis client error:', error.message);
    }
    tileRedisPromise = null;
  }

  tileRedisPromise = (async () => {
    const client = createClient({ url: redisSettings.url });
    const reset = () => {
      if (tileRedisPromise) {
        tileRedisPromise = null;
      }
    };
    client.on('error', (error) => {
      console.warn('Vector tile Redis connection error:', error.message);
    });
    client.on('end', reset);
    client.on('close', reset);
    try {
      await client.connect();
      return client;
    } catch (error) {
      reset();
      console.warn('Failed to connect to Redis for tiles:', error.message);
      try {
        await client.disconnect();
      } catch (disconnectError) {
        console.warn('Error closing tile Redis client after failure:', disconnectError.message);
      }
      return null;
    }
  })();

  const client = await tileRedisPromise;
  return client && client.isOpen ? client : null;
}

function ensureTilePostgresPool() {
  const config = getPostgresConfig();
  const connectionString = config.readOnlyConnectionString || config.connectionString;
  if (!config.enabled || !connectionString) {
    return null;
  }
  const signature = `${connectionString}|${config.ssl ? 'ssl' : 'plain'}`;
  if (!tilePostgresPool || tilePostgresSignature !== signature) {
    if (tilePostgresPool) {
      tilePostgresPool.end().catch(() => {
        /* ignore shutdown errors */
      });
    }
    tilePostgresPool = new Pool({
      connectionString,
      ssl: config.ssl,
      application_name: 'tile-service-fallback',
    });
    tilePostgresPool.on('error', (error) => {
      console.warn('[tiles] Postgres pool error:', error.message);
    });
    tilePostgresSignature = signature;
  }
  return tilePostgresPool;
}

function getParkingSnapshotKey(z, x, y) {
  return `${z}/${x}/${y}`;
}

async function readParkingTileSnapshot(z, x, y) {
  const key = getParkingSnapshotKey(z, x, y);
  if (parkingTileSnapshotCache.has(key)) {
    return parkingTileSnapshotCache.get(key);
  }
  try {
    const filePath = path.join(PARKING_TILE_SNAPSHOT_DIR, `${z}-${x}-${y}.json`);
    const raw = await fs.readFile(filePath, 'utf-8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed.base64 !== 'string') {
      return null;
    }
    const payload = {
      buffer: Buffer.from(parsed.base64, 'base64'),
      version: parsed.version ?? null,
    };
    parkingTileSnapshotCache.set(key, payload);
    return payload;
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.warn('Failed to read parking tile snapshot:', error.message);
    }
    return null;
  }
}

async function writeParkingTileSnapshot(z, x, y, buffer, version) {
  if (!buffer || !Buffer.isBuffer(buffer) || buffer.length === 0) {
    return;
  }
  if (parkingTileSnapshotCache.size >= PARKING_TILE_SNAPSHOT_LIMIT
    && !parkingTileSnapshotCache.has(getParkingSnapshotKey(z, x, y))) {
    const oldestKey = parkingTileSnapshotCache.keys().next().value;
    if (oldestKey) {
      parkingTileSnapshotCache.delete(oldestKey);
    }
  }
  try {
    await fs.mkdir(PARKING_TILE_SNAPSHOT_DIR, { recursive: true });
    const payload = {
      version: version ?? null,
      base64: buffer.toString('base64'),
      storedAt: Date.now(),
    };
    const filePath = path.join(PARKING_TILE_SNAPSHOT_DIR, `${z}-${x}-${y}.json`);
    await fs.writeFile(filePath, JSON.stringify(payload));
    parkingTileSnapshotCache.set(getParkingSnapshotKey(z, x, y), {
      buffer: Buffer.from(payload.base64, 'base64'),
      version: payload.version,
    });
  } catch (error) {
    console.warn('Failed to persist parking tile snapshot:', error.message);
  }
}

function jitterDelay(baseMs) {
  if (!Number.isFinite(baseMs) || baseMs <= 0) {
    return 0;
  }
  return Math.floor(Math.random() * baseMs);
}

async function fetchParkingFeaturesFromPostgres() {
  const pool = ensureTilePostgresPool();
  if (!pool) {
    return [];
  }
  const client = await pool.connect();
  try {
    const result = await client.query(
      `
        SELECT
          location,
          SUM(ticket_count)::BIGINT AS total_count,
          SUM(total_revenue)::NUMERIC AS total_revenue,
          AVG(latitude)::DOUBLE PRECISION AS latitude,
          AVG(longitude)::DOUBLE PRECISION AS longitude,
          MAX(top_infraction) FILTER (WHERE top_infraction IS NOT NULL AND top_infraction <> '') AS top_infraction,
          ARRAY_AGG(DISTINCT year) AS years
        FROM parking_ticket_yearly_locations
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        GROUP BY location
        HAVING SUM(ticket_count) > 0
      `,
    );
    const rows = result?.rows || [];
    return rows
      .filter((row) => Number.isFinite(row.longitude) && Number.isFinite(row.latitude))
      .map((row) => ({
        type: 'Feature',
        geometry: {
          type: 'Point',
          coordinates: [Number(row.longitude), Number(row.latitude)],
        },
        properties: {
          location: row.location,
          count: Number(row.total_count) || 0,
          total_revenue: Number(row.total_revenue) || 0,
          top_infraction: row.top_infraction || null,
          years: Array.isArray(row.years) ? row.years : [],
          months: [],
        },
      }));
  } catch (error) {
    console.warn('Failed to fetch parking features from Postgres:', error.message);
    return [];
  } finally {
    client.release();
  }
}

async function readTileFromRedis(version, z, x, y) {
  if (!TILE_REDIS_ENABLED) {
    return null;
  }
  const client = await getTileRedisClient();
  if (!client) {
    return null;
  }
  const key = `${TILE_REDIS_PREFIX}:${version ?? 'noversion'}:${z}:${x}:${y}`;
  try {
    const raw = await client.sendCommand(['GET', key], { returnBuffers: true });
    if (!raw) {
      return null;
    }
    const envelope = Buffer.from(raw);
    if (envelope.length === 0) {
      return null;
    }
    const flag = envelope[0];
    const hasEnvelopeHeader = flag === TILE_PAYLOAD_FLAG_RAW || flag === TILE_PAYLOAD_FLAG_BROTLI;
    const payload = hasEnvelopeHeader ? envelope.subarray(1) : envelope;
    if (hasEnvelopeHeader && flag === TILE_PAYLOAD_FLAG_BROTLI) {
      try {
        return brotliDecompressSync(payload);
      } catch (error) {
        if (!BROTLI_LEGACY_REWRITE_KEYS.has(key)) {
          console.warn('Failed to decompress cached tile payload, rewriting legacy entry:', error.message);
          BROTLI_LEGACY_REWRITE_KEYS.add(key);
        }
        const rawBuffer = Buffer.from(payload);
        try {
          await client.del(key);
        } catch (delError) {
          console.warn('Failed to purge corrupt tile cache entry:', delError.message);
        }
        await writeTileToRedis(version, z, x, y, rawBuffer, { forceRaw: true });
        return rawBuffer;
      }
    }
    return Buffer.from(payload);
  } catch (error) {
    console.warn('Failed to read tile from Redis:', error.message);
    return null;
  }
}

async function writeTileToRedis(version, z, x, y, buffer, options = {}) {
  if (!TILE_REDIS_ENABLED || !buffer) {
    return;
  }
  const client = await getTileRedisClient();
  if (!client) {
    return;
  }
  const key = `${TILE_REDIS_PREFIX}:${version ?? 'noversion'}:${z}:${x}:${y}`;
  const ttlSeconds = resolveTileTtl(z);
  let envelope;
  if (options.forceRaw) {
    envelope = Buffer.concat([Buffer.from([TILE_PAYLOAD_FLAG_RAW]), Buffer.from(buffer)]);
  } else {
    try {
      const compressed = brotliCompressSync(buffer);
      envelope = Buffer.concat([Buffer.from([TILE_PAYLOAD_FLAG_BROTLI]), compressed]);
    } catch (error) {
      console.warn('Failed to brotli-compress tile payload, storing raw copy:', error.message);
      envelope = Buffer.concat([Buffer.from([TILE_PAYLOAD_FLAG_RAW]), buffer]);
    }
  }
  try {
    await client.set(key, envelope, { EX: ttlSeconds });
  } catch (error) {
    console.warn('Failed to cache tile in Redis:', error.message);
  }
}

function resolveTileTtl(z) {
  if (Number.isFinite(TILE_REDIS_TTL_OVERRIDE) && TILE_REDIS_TTL_OVERRIDE > 0) {
    return TILE_REDIS_TTL_OVERRIDE;
  }
  for (const rule of TILE_TTL_RULES) {
    if (z <= rule.maxZoom) {
      return rule.ttl;
    }
  }
  return 60 * 30;
}

function lngLatToTile(lng, lat, zoom) {
  const clampedLat = Math.min(Math.max(lat, -85.05112878), 85.05112878);
  const clampedLng = Math.min(Math.max(lng, -180), 180);
  const latRad = (clampedLat * Math.PI) / 180;
  const n = 2 ** zoom;
  const x = Math.floor(((clampedLng + 180) / 360) * n);
  const y = Math.floor(
    ((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2) * n,
  );
  return { x, y };
}

function computePrewarmSeeds() {
  const seeds = [];
  for (const zoom of TILE_PREWARM_ZOOMS) {
    if (!Number.isInteger(zoom) || zoom < TICKET_TILE_MIN_ZOOM) {
      continue;
    }
    const topLeft = lngLatToTile(GTA_BOUNDS.west, GTA_BOUNDS.north, zoom);
    const bottomRight = lngLatToTile(GTA_BOUNDS.east, GTA_BOUNDS.south, zoom);
    const minX = Math.min(topLeft.x, bottomRight.x);
    const maxX = Math.max(topLeft.x, bottomRight.x);
    const minY = Math.min(topLeft.y, bottomRight.y);
    const maxY = Math.max(topLeft.y, bottomRight.y);
    const step = zoom >= 13 ? 2 : 1;
    for (let x = minX; x <= maxX; x += step) {
      for (let y = minY; y <= maxY; y += step) {
        seeds.push({ z: zoom, x, y });
      }
    }
  }
  return seeds;
}

function scheduleTilePrewarm(tileService) {
  if (!TILE_REDIS_ENABLED || !TILE_PREWARM_ENABLED) {
    return;
  }
  let running = false;
  const run = async () => {
    if (running) {
      return;
    }
    running = true;
    try {
      const client = await getTileRedisClient();
      if (!client) {
        return;
      }
      await tileService.ensureLoaded();
      const seeds = computePrewarmSeeds();
      if (!seeds.length) {
        return;
      }
      let warmed = 0;
      for (const seed of seeds) {
        try {
          const tile = await tileService.getTile(seed.z, seed.x, seed.y);
          if (tile && tile.buffer && tile.buffer.length > 0) {
            warmed += 1;
          }
        } catch (error) {
          console.warn('Tile prewarm fetch failed:', error.message);
        }
      }
      if (warmed > 0) {
        console.log(`Prewarmed ${warmed} parking tiles across ${seeds.length} targets.`);
      }
    } finally {
      running = false;
    }
  };

  const scheduleRun = (delayMs) => {
    const timer = setTimeout(() => {
      run()
        .catch((error) => {
          console.warn('Tile prewarm execution failed:', error.message);
        })
        .finally(() => {
          const nextDelay = TILE_PREWARM_INTERVAL_MS + jitterDelay(TILE_PREWARM_JITTER_MS);
          scheduleRun(nextDelay);
        });
    }, Math.max(0, delayMs));
    if (typeof timer.unref === 'function') {
      timer.unref();
    }
  };

  const initialDelay = Math.max(0, TILE_PREWARM_INITIAL_DELAY_MS) + jitterDelay(TILE_PREWARM_JITTER_MS);
  scheduleRun(initialDelay);
}

const CLUSTER_VISIBILITY_RULES = [
  {
    minCount: 1000,
    opacityLow: 0.7,
    opacityMid: 0.7,
    opacityHigh: 0.7,
    labelLow: 1,
    labelMid: 1,
    labelHigh: 1,
  },
  {
    minCount: 300,
    opacityLow: 0,
    opacityMid: 0.7,
    opacityHigh: 0.7,
    labelLow: 0,
    labelMid: 1,
    labelHigh: 1,
  },
  {
    minCount: 75,
    opacityLow: 0,
    opacityMid: 0.55,
    opacityHigh: 0.65,
    labelLow: 0,
    labelMid: 0.75,
    labelHigh: 0.9,
  },
];

function decorateClusterFeature(tags) {
  const count = Number(tags.point_count || tags.count || tags.ticketCount || 0);
  if (!Number.isFinite(count) || count <= 0) {
    tags.cluster_opacity_low = 0;
    tags.cluster_opacity_mid = 0;
    tags.cluster_opacity_high = 0;
    tags.cluster_label_low = 0;
    tags.cluster_label_mid = 0;
    tags.cluster_label_high = 0;
    return;
  }

  const rule = CLUSTER_VISIBILITY_RULES.find((entry) => count >= entry.minCount);
  const fallbackOpacity = count >= 10 ? 0.4 : 0;
  const fallbackLabel = count >= 10 ? 0.6 : 0;

  const selected = rule || {
    opacityLow: 0,
    opacityMid: fallbackOpacity,
    opacityHigh: Math.min(0.5, fallbackOpacity),
    labelLow: 0,
    labelMid: fallbackLabel,
    labelHigh: Math.min(0.7, fallbackLabel),
  };

  tags.cluster_opacity_low = selected.opacityLow;
  tags.cluster_opacity_mid = selected.opacityMid;
  tags.cluster_opacity_high = selected.opacityHigh;
  tags.cluster_label_low = selected.labelLow;
  tags.cluster_label_mid = selected.labelMid;
  tags.cluster_label_high = selected.labelHigh;
}

const YEAR_OFFSET = 2000;
const YEAR_MASK_WIDTH = 32;
const MONTH_MASK_WIDTH = 12;

const WARD_TILE_OPTIONS = {
  maxZoom: 12,
  tolerance: 4,
  extent: 4096,
  buffer: 2,
};
const WARD_TILE_CACHE_LIMIT = 256;
const wardTileIndexes = new Map();
const wardTileVersions = new Map();
const wardTileCache = new Map();
const wardTilePromises = new Map();

const WARD_SNAPSHOT_DIR = process.env.WARD_TILE_SNAPSHOT_DIR
  || path.resolve(process.cwd(), 'map-app/.cache/ward-tile-snapshots');

async function readWardTileSnapshot(dataset) {
  try {
    const filePath = path.join(WARD_SNAPSHOT_DIR, `${dataset}.json`);
    const raw = await fs.readFile(filePath, 'utf-8');
    return JSON.parse(raw);
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.warn(`Failed to read ward tile snapshot for ${dataset}:`, error.message);
    }
    return null;
  }
}

async function writeWardTileSnapshot(dataset, version, tiles) {
  try {
    await fs.mkdir(WARD_SNAPSHOT_DIR, { recursive: true });
    const filePath = path.join(WARD_SNAPSHOT_DIR, `${dataset}.json`);
    const payload = {
      dataset,
      version,
      generatedAt: new Date().toISOString(),
      tileCount: Object.keys(tiles).length,
      tiles,
    };
    await fs.writeFile(filePath, JSON.stringify(payload));
  } catch (error) {
    console.warn(`Failed to persist ward tile snapshot for ${dataset}:`, error.message);
  }
}

async function hydrateWardCacheFromSnapshot(dataset, version) {
  const snapshot = await readWardTileSnapshot(dataset);
  if (!snapshot || snapshot.version !== version || !snapshot.tiles) {
    return false;
  }
  let hydrated = 0;
  for (const [key, base64] of Object.entries(snapshot.tiles)) {
    const [zStr, xStr, yStr] = key.split('/');
    const z = Number.parseInt(zStr, 10);
    const x = Number.parseInt(xStr, 10);
    const y = Number.parseInt(yStr, 10);
    if (![z, x, y].every(Number.isFinite)) {
      continue;
    }
    const buffer = Buffer.from(base64, 'base64');
    const cacheKey = getWardTileCacheKey(dataset, z, x, y);
    if (!wardTileCache.has(cacheKey) && wardTileCache.size < WARD_TILE_CACHE_LIMIT) {
      wardTileCache.set(cacheKey, { buffer, version });
      hydrated += 1;
    }
  }
  if (hydrated > 0) {
    console.log(`Hydrated ${hydrated} ward tiles for ${dataset} from snapshot.`);
    return true;
  }
  return false;
}

async function ensureWardSnapshot(dataset, index, version) {
  const hydrated = await hydrateWardCacheFromSnapshot(dataset, version);
  if (hydrated) {
    return;
  }

  const coords = Array.isArray(index.tileCoords) ? index.tileCoords : [];
  if (!coords.length) {
    return;
  }
  const tiles = {};
  const layerName = WARD_TILE_SOURCE_LAYER || 'ward_polygons';
  const limit = Math.min(coords.length, WARD_TILE_CACHE_LIMIT * 4);
  for (let i = 0; i < limit; i += 1) {
    const coord = coords[i];
    if (!coord) {
      continue;
    }
    const { z, x, y } = coord;
    if (![z, x, y].every((value) => Number.isInteger(value))) {
      continue;
    }
    const tile = index.getTile(z, x, y);
    if (!tile) {
      continue;
    }
    const normalizedTile = {
      ...tile,
      extent: typeof tile.extent === 'number' ? tile.extent : 4096,
      version: 2,
    };
    const buffer = vtpbf.fromGeojsonVt({ [layerName]: normalizedTile }, { extent: 4096, version: 2 });
    tiles[`${z}/${x}/${y}`] = buffer.toString('base64');
    if (!wardTileCache.has(getWardTileCacheKey(dataset, z, x, y)) && wardTileCache.size < WARD_TILE_CACHE_LIMIT) {
      wardTileCache.set(getWardTileCacheKey(dataset, z, x, y), { buffer, version });
    }
  }

  if (Object.keys(tiles).length > 0) {
    await writeWardTileSnapshot(dataset, version, tiles);
  }
}

function toNumberArray(value, limit) {
  if (!Array.isArray(value)) {
    return [];
  }
  const unique = new Set();
  for (const entry of value) {
    const num = Number(entry);
    if (!Number.isFinite(num)) {
      continue;
    }
    unique.add(num);
    if (unique.size >= limit) {
      break;
    }
  }
  return Array.from(unique).sort((a, b) => a - b).slice(0, limit);
}

function encodeYearMask(values) {
  if (!Array.isArray(values) || values.length === 0) {
    return 0;
  }
  let mask = 0;
  for (const value of values) {
    const offset = Math.trunc(value) - YEAR_OFFSET;
    if (offset < 0 || offset >= YEAR_MASK_WIDTH) {
      continue;
    }
    mask |= 1 << offset;
  }
  return mask >>> 0;
}

function decodeYearMask(mask) {
  if (!Number.isFinite(mask) || mask === 0) {
    return [];
  }
  const years = [];
  for (let offset = 0; offset < YEAR_MASK_WIDTH; offset += 1) {
    if ((mask & (1 << offset)) !== 0) {
      years.push(YEAR_OFFSET + offset);
    }
  }
  return years;
}

function yearMaskIncludes(mask, value) {
  if (!Number.isFinite(mask) || !Number.isFinite(value)) {
    return false;
  }
  const offset = Math.trunc(value) - YEAR_OFFSET;
  if (offset < 0 || offset >= YEAR_MASK_WIDTH) {
    return false;
  }
  return (mask & (1 << offset)) !== 0;
}

function encodeMonthMask(values) {
  if (!Array.isArray(values) || values.length === 0) {
    return 0;
  }
  let mask = 0;
  for (const value of values) {
    const offset = Math.trunc(value) - 1;
    if (offset < 0 || offset >= MONTH_MASK_WIDTH) {
      continue;
    }
    mask |= 1 << offset;
  }
  return mask >>> 0;
}

function decodeMonthMask(mask) {
  if (!Number.isFinite(mask) || mask === 0) {
    return [];
  }
  const months = [];
  for (let offset = 0; offset < MONTH_MASK_WIDTH; offset += 1) {
    if ((mask & (1 << offset)) !== 0) {
      months.push(offset + 1);
    }
  }
  return months;
}

function monthMaskIncludes(mask, value) {
  if (!Number.isFinite(mask) || !Number.isFinite(value)) {
    return false;
  }
  const offset = Math.trunc(value) - 1;
  if (offset < 0 || offset >= MONTH_MASK_WIDTH) {
    return false;
  }
  return (mask & (1 << offset)) !== 0;
}

function safeParseGeojson(raw) {
  if (!raw) {
    return null;
  }
  if (typeof raw === 'object') {
    return raw;
  }
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw);
    } catch (error) {
      console.warn('Failed to parse ward GeoJSON payload:', error.message);
      return null;
    }
  }
  return null;
}

function clearWardTileCache(dataset) {
  const prefix = `${dataset}:`;
  for (const key of wardTileCache.keys()) {
    if (key.startsWith(prefix)) {
      wardTileCache.delete(key);
    }
  }
}

function getWardTileCacheKey(dataset, z, x, y) {
  return `${dataset}:${z}/${x}/${y}`;
}

function buildTileKey(z, x, y) {
  return `${z}/${x}/${y}`;
}

function hydrateTemporalTags(tile) {
  if (!tile || !Array.isArray(tile.features)) {
    return;
  }
  for (const feature of tile.features) {
    const tags = feature?.tags;
    if (!tags) {
      continue;
    }
    if (typeof tags.yearMask === 'number') {
      tags.years = decodeYearMask(tags.yearMask);
      delete tags.yearMask;
    }
    if (typeof tags.monthMask === 'number') {
      tags.months = decodeMonthMask(tags.monthMask);
      delete tags.monthMask;
    }
  }
}

async function ensureWardTileIndex(dataset) {
  if (!dataset) {
    throw new Error('Dataset must be provided for ward tiles');
  }
  if (wardTileIndexes.has(dataset)) {
    return wardTileIndexes.get(dataset);
  }
  if (wardTilePromises.has(dataset)) {
    return wardTilePromises.get(dataset);
  }

  const promise = (async () => {
    const resource = await loadCameraWardGeojson(dataset);
    if (!resource) {
      throw new Error(`Ward dataset ${dataset} is unavailable`);
    }
    const payload = safeParseGeojson(resource.raw || resource.data);
    if (!payload || !Array.isArray(payload.features)) {
      throw new Error(`Invalid ward GeoJSON payload for ${dataset}`);
    }

    const index = geojsonvt(payload, WARD_TILE_OPTIONS);
    wardTileIndexes.set(dataset, index);
    const resolvedVersion = typeof resource.etag === 'string'
      ? resource.etag
      : (resource.version !== null && resource.version !== undefined
        ? `W/"${resource.version}"`
        : `W/"${Date.now()}"`);
    wardTileVersions.set(dataset, resolvedVersion);
    clearWardTileCache(dataset);
    try {
      await ensureWardSnapshot(dataset, index, resolvedVersion);
    } catch (error) {
      console.warn(`Failed to generate ward tile snapshot for ${dataset}:`, error.message);
    }
    return index;
  })()
    .catch((error) => {
      wardTileIndexes.delete(dataset);
      wardTileVersions.delete(dataset);
      clearWardTileCache(dataset);
      throw error;
    })
    .finally(() => {
      wardTilePromises.delete(dataset);
    });

  wardTilePromises.set(dataset, promise);
  return promise;
}

export async function prewarmWardTiles(dataset) {
  try {
    await ensureWardTileIndex(dataset);
  } catch (error) {
    console.warn('Failed to prewarm ward tiles:', error.message);
    throw error;
  }
}

export async function getWardTile(dataset, zValue, xValue, yValue) {
  const z = Number.parseInt(zValue, 10);
  const x = Number.parseInt(xValue, 10);
  const y = Number.parseInt(yValue, 10);
  if (![z, x, y].every(Number.isFinite)) {
    return null;
  }

  const cacheKey = getWardTileCacheKey(dataset, z, x, y);
  if (wardTileCache.has(cacheKey)) {
    return wardTileCache.get(cacheKey);
  }

  const index = await ensureWardTileIndex(dataset);
  if (!index) {
    return null;
  }
  const tile = index.getTile(z, x, y);
  const version = wardTileVersions.get(dataset) || null;
  if (!tile) {
    const payload = { buffer: null, version };
    wardTileCache.set(cacheKey, payload);
    if (wardTileCache.size > WARD_TILE_CACHE_LIMIT) {
      const oldestKey = wardTileCache.keys().next().value;
      if (oldestKey) {
        wardTileCache.delete(oldestKey);
      }
    }
    return payload;
  }

  const layerName = WARD_TILE_SOURCE_LAYER || 'ward_polygons';
  const normalizedTile = {
    ...tile,
    extent: typeof tile.extent === 'number' ? tile.extent : 4096,
    version: 2,
  };
  const buffer = vtpbf.fromGeojsonVt({ [layerName]: normalizedTile }, { extent: 4096, version: 2 });
  const payload = { buffer, version };
  wardTileCache.set(cacheKey, payload);
  if (wardTileCache.size > WARD_TILE_CACHE_LIMIT) {
    const oldestKey = wardTileCache.keys().next().value;
    if (oldestKey) {
      wardTileCache.delete(oldestKey);
    }
  }
  return payload;
}

// MARK: TileService
class TileService {
  constructor() {
    this.dataVersion = null;
    this.loadingPromise = null;
    this.clusterIndex = null;
    this.pointIndex = null;
    this.summaryPoints = [];
    this.locationTable = [];
    this.streetTable = [];
    this.summaryTree = null;
    this.tileCache = new Map();
    this.locationYearCounts = null;
    this.inflightTiles = new Map();
    this.revalidateQueue = new Set();
  }

  async ensureLoaded() {
    const { version, chunks } = await getTicketChunks();
    const resolvedVersion = version ?? null;
    if (
      resolvedVersion !== null &&
      this.dataVersion !== null &&
      this.dataVersion === resolvedVersion &&
      this.clusterIndex &&
      this.pointIndex
    ) {
      return;
    }
    if (this.loadingPromise) {
      await this.loadingPromise;
      if (
        resolvedVersion !== null &&
        this.dataVersion !== null &&
        this.dataVersion === resolvedVersion &&
        this.clusterIndex &&
        this.pointIndex
      ) {
        return;
      }
    }
    this.loadingPromise = this.loadFromChunks({ version: resolvedVersion, chunks });
    try {
      await this.loadingPromise;
    } finally {
      this.loadingPromise = null;
    }
  }

  async loadFromChunks({ version, chunks }) {
    let chunkDescriptors = Array.isArray(chunks) ? chunks : [];

    if (chunkDescriptors.length === 0) {
      try {
        const resource = await getTicketsRaw();
        const parsed = JSON.parse(resource.raw);
        chunkDescriptors = [
          {
            features: Array.isArray(parsed?.features) ? parsed.features : [],
            featureCount: Array.isArray(parsed?.features) ? parsed.features.length : 0,
            source: resource.source,
            version: resource.version,
          },
        ];
      } catch (error) {
        console.error('Failed to load tickets GeoJSON fallback:', error.message);
        throw error;
      }
    }
    if (chunkDescriptors.length === 0) {
      const postgresFeatures = await fetchParkingFeaturesFromPostgres();
      if (postgresFeatures.length > 0) {
        chunkDescriptors = [
          {
            features: postgresFeatures,
            featureCount: postgresFeatures.length,
            source: 'postgres',
            version: Date.now(),
          },
        ];
      }
    }

    const locationYearCounts = await getParkingLocationYearMap().catch(() => new Map());
    this.locationYearCounts = locationYearCounts;

    const sanitized = [];
    const summaryPoints = [];
    const locationTable = [];
    const streetTable = [];
    const locationLookup = new Map();
    const streetLookup = new Map();
    let featureId = 0;

    const internString = (value, lookup, table) => {
      if (!value) {
        return null;
      }
      const existing = lookup.get(value);
      if (existing !== undefined) {
        return existing;
      }
      const id = table.length;
      table.push(value);
      lookup.set(value, id);
      return id;
    };

    for (const descriptor of chunkDescriptors) {
      const chunk = descriptor.features ? descriptor : await loadTicketChunk(descriptor);
      const features = Array.isArray(chunk?.features) ? chunk.features : [];
      for (const feature of features) {
        const coords = feature?.geometry?.coordinates;
        if (!Array.isArray(coords) || coords.length < 2) {
          continue;
        }
        const [longitude, latitude] = coords;
        if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
          continue;
        }

        const props = feature?.properties || {};
        const count = Number(props.count) || 0;
        const totalRevenue = Number(props.total_revenue) || 0;
        if (count <= 0) {
          continue;
        }

        const years = toNumberArray(props.years, YEARS_LIMIT);
        const months = toNumberArray(props.months, MONTHS_LIMIT);
        const yearMask = encodeYearMask(years);
        const monthMask = encodeMonthMask(months);
        const location = props.location || null;
        const topInfraction = props.top_infraction || null;
        const streetName = normalizeStreetName(location || props.address || '');

        const cleanedProperties = {
          id: featureId,
          location,
          count,
          total_revenue: Number(totalRevenue.toFixed(2)),
          top_infraction: topInfraction,
          yearMask,
          monthMask,
        };

        if (location && locationYearCounts.has(location)) {
          const yearEntry = locationYearCounts.get(location);
          cleanedProperties.year_counts = JSON.stringify(yearEntry);
        }

        if (!cleanedProperties.top_infraction) {
          delete cleanedProperties.top_infraction;
        }

        sanitized.push({
          type: 'Feature',
          geometry: {
            type: 'Point',
            coordinates: [longitude, latitude],
          },
          properties: cleanedProperties,
        });

        const locationId = internString(location || streetName, locationLookup, locationTable);
        const streetId = internString(streetName || location, streetLookup, streetTable);

        summaryPoints.push({
          longitude,
          latitude,
          ticketCount: count,
          totalRevenue,
          locationId,
          streetId,
          yearMask,
          monthMask,
          location,
        });

        featureId += 1;
      }

      if (!descriptor.features && chunk) {
        chunk.features = [];
      }
    }

    const clusterIndex = new Supercluster({
      minZoom: 0,
      maxZoom: 16,
      radius: 110,
      extent: 4096,
      map: (properties) => ({
        ...properties,
        ticketCount: properties.count,
        yearMask: properties.yearMask ?? 0,
        monthMask: properties.monthMask ?? 0,
      }),
      reduce: (acc, props) => {
        acc.total_revenue = (acc.total_revenue || 0) + (props.total_revenue || 0);
        acc.count = (acc.count || 0) + (props.count || props.ticketCount || 0);
        acc.ticketCount = (acc.ticketCount || 0) + (props.ticketCount || props.count || 0);
        acc.yearMask = (acc.yearMask || 0) | (props.yearMask || 0);
        acc.monthMask = (acc.monthMask || 0) | (props.monthMask || 0);
      },
    });

    clusterIndex.load([...sanitized]);

    const vtCollection = { type: 'FeatureCollection', features: sanitized };
    const pointIndex = geojsonvt(vtCollection, {
      maxZoom: 16,
      extent: 4096,
      buffer: 64,
      indexMaxZoom: 16,
      indexMaxPoints: 0,
    });

    vtCollection.features = [];
    const featureCount = summaryPoints.length || 1;
    sanitized.length = 0;

    const spatialIndex = new Flatbush(featureCount);
    for (const point of summaryPoints) {
      const { longitude, latitude } = point;
      if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
        continue;
      }
      spatialIndex.add(longitude, latitude, longitude, latitude);
    }
    spatialIndex.finish();

    this.clusterIndex = clusterIndex;
    this.pointIndex = pointIndex;
    this.summaryPoints = summaryPoints;
    this.locationTable = locationTable;
    this.streetTable = streetTable;
    this.summaryTree = spatialIndex;
    this.dataVersion = version ?? Date.now();
    this.tileCache.clear();
  }

  async getTile(z, x, y, options = {}) {
    const {
      signal = null,
      allowStale = true,
      revalidate = true,
    } = options;

    await this.ensureLoaded();
    throwIfAborted(signal);

    if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y)) {
      return null;
    }
    if (z < TICKET_TILE_MIN_ZOOM) {
      return null;
    }

    const key = buildTileKey(z, x, y);
    const cacheVersion = this.dataVersion ?? null;

    const cached = this.tileCache.get(key);
    if (cached) {
      return cached;
    }

    const inflight = this.inflightTiles.get(key);
    if (inflight) {
      return inflight;
    }

    const redisBuffer = await readTileFromRedis(cacheVersion, z, x, y);
    throwIfAborted(signal);
    if (redisBuffer) {
      const payload = { buffer: redisBuffer, version: cacheVersion };
      this.cacheTile(key, payload);
      return payload;
    }

    if (allowStale) {
      const snapshot = await readParkingTileSnapshot(z, x, y);
      throwIfAborted(signal);
      if (snapshot && snapshot.buffer) {
        const payload = {
          buffer: snapshot.buffer,
          version: snapshot.version ?? cacheVersion,
        };
        this.cacheTile(key, payload);
        if (revalidate) {
          this.queueTileRevalidate(z, x, y);
        }
        return payload;
      }
    }

    return this.generateFreshTile({
      key,
      z,
      x,
      y,
      signal,
    });
  }

  cacheTile(key, payload) {
    if (!payload) {
      return;
    }
    this.tileCache.set(key, payload);
    if (this.tileCache.size > TILE_CACHE_LIMIT) {
      const oldestKey = this.tileCache.keys().next().value;
      if (oldestKey) {
        this.tileCache.delete(oldestKey);
      }
    }
  }

  queueTileRevalidate(z, x, y) {
    const key = buildTileKey(z, x, y);
    if (this.revalidateQueue.has(key) || this.inflightTiles.has(key)) {
      return;
    }
    this.revalidateQueue.add(key);
    const run = () => {
      this.generateFreshTile({
        key,
        z,
        x,
        y,
        signal: null,
        background: true,
      })
        .catch((error) => {
          if (error?.name !== 'AbortError') {
            console.warn('Tile revalidation failed:', error.message);
          }
        })
        .finally(() => {
          this.revalidateQueue.delete(key);
        });
    };
    if (TILE_REVALIDATE_DELAY_MS > 0) {
      const timer = setTimeout(run, TILE_REVALIDATE_DELAY_MS);
      if (typeof timer?.unref === 'function') {
        timer.unref();
      }
    } else {
      const timer = setTimeout(run, 0);
      if (typeof timer?.unref === 'function') {
        timer.unref();
      }
    }
  }

  async generateFreshTile({ key, z, x, y, signal, background = false }) {
    if (this.inflightTiles.has(key)) {
      return this.inflightTiles.get(key);
    }
    const promise = (async () => {
      let releaseFn = null;
      try {
        throwIfAborted(signal);
        if (renderSemaphore) {
          releaseFn = await renderSemaphore.acquire(signal);
        }
        throwIfAborted(signal);
        const layers = this.buildTileLayers(z, x, y, signal);
        if (!layers) {
          return null;
        }
        const buffer = vtpbf.fromGeojsonVt(layers, { extent: 4096, version: 2 });
        const cacheVersion = this.dataVersion ?? null;
        const payload = { buffer, version: cacheVersion };
        this.cacheTile(key, payload);
        writeTileToRedis(cacheVersion, z, x, y, buffer);
        writeParkingTileSnapshot(z, x, y, buffer, cacheVersion);
        return payload;
      } catch (error) {
        if (background) {
          if (error?.name !== 'AbortError') {
            console.warn('Background tile generation failed:', error.message);
          }
          return null;
        }
        throw error;
      } finally {
        if (typeof releaseFn === 'function') {
          releaseFn();
        }
        this.inflightTiles.delete(key);
      }
    })();

    this.inflightTiles.set(key, promise);
    return promise;
  }

  buildTileLayers(z, x, y, signal) {
    throwIfAborted(signal);
    const layers = {};

    if (z < RAW_POINT_ZOOM_THRESHOLD) {
      const clusterTile = this.clusterIndex.getTile(z, x, y);
      if (clusterTile) {
        hydrateTemporalTags(clusterTile);
        for (const feature of clusterTile.features || []) {
          if (feature && feature.tags) {
            decorateClusterFeature(feature.tags);
          }
        }
        layers[TILE_LAYER_NAME] = {
          ...clusterTile,
          extent: typeof clusterTile.extent === 'number' ? clusterTile.extent : 4096,
          version: 2,
        };
      }
    } else {
      const pointTile = this.pointIndex.getTile(z, x, y);
      if (pointTile) {
        hydrateTemporalTags(pointTile);
        layers[TILE_LAYER_NAME] = {
          ...pointTile,
          extent: typeof pointTile.extent === 'number' ? pointTile.extent : 4096,
          version: 2,
        };
      }
    }

    if (Object.keys(layers).length === 0) {
      return null;
    }
    throwIfAborted(signal);
    return layers;
  }

  async summarizeViewport({ west, south, east, north, zoom, filters }) {
    await this.ensureLoaded();
    const numericZoom = Number(zoom);
    if (!Number.isFinite(numericZoom) || numericZoom < SUMMARY_ZOOM_THRESHOLD) {
      return {
        zoomRestricted: true,
        topStreets: [],
      };
    }

    if (!this.summaryTree || !this.summaryPoints.length) {
      return {
        zoomRestricted: false,
        visibleCount: 0,
        visibleRevenue: 0,
        topStreets: [],
      };
    }

    const indices = this.summaryTree.search(west, south, east, north);
    let visibleCount = 0;
    let visibleRevenue = 0;
    const streetMap = new Map();
    const yearCountsMap = this.locationYearCounts || null;

    for (const index of indices) {
      const point = this.summaryPoints[index];
      if (!point) {
        continue;
      }
      if (filters?.year && !yearMaskIncludes(point.yearMask, filters.year)) {
        continue;
      }
      if (filters?.month && !monthMaskIncludes(point.monthMask, filters.month)) {
        continue;
      }
      let ticketCount = point.ticketCount;
      let totalRevenue = point.totalRevenue;
      if (filters?.year && yearCountsMap && point.location) {
        const locationEntry = yearCountsMap.get(point.location);
        if (locationEntry && locationEntry[filters.year]) {
          ticketCount = locationEntry[filters.year].ticketCount;
          totalRevenue = locationEntry[filters.year].totalRevenue;
        } else {
          ticketCount = 0;
          totalRevenue = 0;
        }
      }
      if (ticketCount <= 0) {
        continue;
      }

      visibleCount += ticketCount;
      visibleRevenue += totalRevenue;

      const streetName =
        Number.isInteger(point.streetId) ? this.streetTable[point.streetId] : null;
      const locationName =
        Number.isInteger(point.locationId) ? this.locationTable[point.locationId] : null;
      const streetKey = streetName || locationName || 'Unknown';
      if (!streetMap.has(streetKey)) {
        streetMap.set(streetKey, {
          name: streetKey,
          ticketCount: 0,
          totalRevenue: 0,
          sampleLocation: locationName || streetKey,
        });
      }
      const streetEntry = streetMap.get(streetKey);
      streetEntry.ticketCount += ticketCount;
      streetEntry.totalRevenue += totalRevenue;
    }

    const topStreets = Array.from(streetMap.values())
      .sort((a, b) => (b.totalRevenue || 0) - (a.totalRevenue || 0))
      .slice(0, SUMMARY_LIMIT)
      .map((entry) => ({
        ...entry,
        totalRevenue: Number(entry.totalRevenue.toFixed(2)),
      }));

    return {
      zoomRestricted: false,
      visibleCount,
      visibleRevenue: Number(visibleRevenue.toFixed(2)),
      topStreets,
    };
  }

  async getViewportPoints({ west, south, east, north, limit = 5000, filters }) {
    await this.ensureLoaded();
    if (!this.summaryTree || !this.summaryPoints.length) {
      return [];
    }

    const indices = this.summaryTree.search(west, south, east, north);
    if (!indices || indices.length === 0) {
      return [];
    }

    const maxPoints = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 20000) : 5000;
    const result = [];

    const yearCountsMap = this.locationYearCounts || null;

    for (let i = 0; i < indices.length; i += 1) {
      const point = this.summaryPoints[indices[i]];
      if (!point) {
        continue;
      }

      if (filters?.year && !yearMaskIncludes(point.yearMask, filters.year)) {
        continue;
      }
      if (filters?.month && !monthMaskIncludes(point.monthMask, filters.month)) {
        continue;
      }

      let ticketCount = point.ticketCount;
      if (filters?.year && yearCountsMap && point.location) {
        const locationEntry = yearCountsMap.get(point.location);
        if (locationEntry && locationEntry[filters.year]) {
          ticketCount = locationEntry[filters.year].ticketCount;
        } else {
          ticketCount = 0;
        }
      }
      if (ticketCount <= 0) {
        continue;
      }

      result.push({
        longitude: point.longitude,
        latitude: point.latitude,
        count: ticketCount,
      });

      if (result.length >= maxPoints) {
        break;
      }
    }

    return result;
  }

  async getClusterExpansionZoom(clusterId) {
    await this.ensureLoaded();
    if (!this.clusterIndex || !Number.isFinite(clusterId)) {
      return null;
    }
    try {
      return this.clusterIndex.getClusterExpansionZoom(clusterId);
    } catch (error) {
      console.warn('Failed to resolve cluster expansion zoom', error.message);
      return null;
    }
  }
}

export function createTileService() {
  // Use DATA_DIR from environment (set by index.js)
  const dataDir = process.env.DATA_DIR;
  if (!dataDir) {
    throw new Error('DATA_DIR environment variable must be set');
  }
  const service = new TileService();
  scheduleTilePrewarm(service);
  return service;
}


export { TILE_HARD_TIMEOUT_MS, EMPTY_TILE_BUFFER };

