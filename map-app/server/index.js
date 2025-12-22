/* eslint-env node */
import fs from 'fs/promises';
import { existsSync } from 'fs';
import express from 'express';
import path from 'path';
import process from 'node:process';
import { brotliCompressSync, gzipSync } from 'node:zlib';
import { performance } from 'node:perf_hooks';
import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'url';
import { createServer as createViteServer } from 'vite';
import { createClient } from 'redis';
import { Pool } from 'pg';
import { createAppData, getLatestAppDataMeta } from './createAppData.js';
import {
  createTileService,
  getWardTile,
  prewarmWardTiles,
  EMPTY_TILE_BUFFER,
  TILE_HARD_TIMEOUT_MS,
  getTileMetrics,
} from './tileService.js';
import { createPostgisTileService, DATASET_CONFIG as POSTGIS_DATASET_CONFIG } from './postgisTileService.js';
import { mergeGeoJSONChunks } from './mergeGeoJSONChunks.js';
import { getDatasetTotals } from './datasetTotalsService.js';
import { wakeRemoteServices } from './wakeRemoteServices.js';
import { startBackgroundAppDataRefresh } from '../scripts/backgroundAppDataRefresh.js';
import { TICKET_TILE_MIN_ZOOM } from '../shared/mapConstants.js';
import {
  loadStreetStats,
  loadTicketsSummary,
  loadNeighbourhoodStats,
  loadDatasetSummary,
  loadCameraGlow,
  loadCameraLocations,
  loadCameraWardGeojson,
  loadCameraWardSummary,
} from './ticketsDataStore.js';
import {
  getDatasetYears,
  getParkingTotals,
  getParkingTopStreets,
  getParkingTopNeighbourhoods,
  getParkingLocationDetail,
  getCameraTotals,
  getCameraTopLocations,
  getCameraLocationDetail,
  getCameraTopGroups,
} from './yearlyMetricsService.js';
import {
  getRedisConfig,
  getPmtilesRuntimeConfig,
  getCoreDbConfig,
  getTileDbConfig,
  getTileCacheConfig,
} from './runtimeConfig.js';
import { buildPmtilesManifest } from './pmtilesManifest.js';
import { schedulePmtilesWarmup } from './pmtilesWarmup.js';
import glowTileService from './glowTileService.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const resolve = (p) => path.resolve(__dirname, '..', p);

if (typeof globalThis.fetch !== 'function') {
  console.warn('[init] global fetch API is unavailable; MapTiler proxy requires Node.js 18+ with native fetch support.');
}

function normaliseBaseUrl(baseUrl) {
  if (typeof baseUrl !== 'string' || !baseUrl.trim()) {
    return '';
  }
  return baseUrl.replace(/\/+$/u, '');
}
const rawMaptilerProxyPath = process.env.MAPTILER_PROXY_PATH || '/api/maptiler';
const MAPTILER_PROXY_BASE = `/${rawMaptilerProxyPath.replace(/^\/+|\/+$/gu, '')}`;
const MAPTILER_PROXY_LOCAL_PREFIX = MAPTILER_PROXY_BASE.endsWith('/')
  ? MAPTILER_PROXY_BASE
  : `${MAPTILER_PROXY_BASE}/`;

function getMaptilerProxyPrefix(baseUrl = '') {
  const origin = normaliseBaseUrl(baseUrl);
  return origin ? `${origin}${MAPTILER_PROXY_LOCAL_PREFIX}` : MAPTILER_PROXY_LOCAL_PREFIX;
}
const MAPTILER_PROXY_ROUTE = `${MAPTILER_PROXY_BASE}/:path(*)`;

// Memory optimization: Warmup disabled by default to prevent OOM on startup
// Enable with TILE_WARMUP_ENABLED=1 if needed
const TILE_WARMUP_ENABLED = process.env.TILE_WARMUP_ENABLED === '1';
// Memory optimization: Reduced from 8,9,10,11 to just 8 (much fewer tiles)
const TILE_WARMUP_ZOOMS = (process.env.TILE_WARMUP_ZOOMS || '8')
  .split(',')
  .map((entry) => Number.parseInt(entry, 10))
  .filter((value) => Number.isInteger(value) && value >= 0 && value <= 16);
// Memory optimization: Only warm parking_tickets by default (largest dataset)
const TILE_WARMUP_DATASETS = (process.env.TILE_WARMUP_DATASETS || 'parking_tickets')
  .split(',')
  .map((entry) => entry.trim())
  .filter((entry) => entry.length > 0);
const TILE_WARMUP_BOUNDS = {
  west: Number.parseFloat(process.env.TILE_WARMUP_WEST ?? '-79.6393'),
  south: Number.parseFloat(process.env.TILE_WARMUP_SOUTH ?? '43.4032'),
  east: Number.parseFloat(process.env.TILE_WARMUP_EAST ?? '-79.1169'),
  north: Number.parseFloat(process.env.TILE_WARMUP_NORTH ?? '43.8554'),
};

function shouldUseMaptilerProxy() {
  if (maptilerProxyState.mode === 'proxy') {
    return true;
  }
  if (maptilerProxyState.mode === 'direct') {
    return false;
  }
  return Boolean(maptilerProxyState.proxyEnabled);
}

function sanitizeMaptilerUrl(raw, baseUrl = '') {
  if (!shouldUseMaptilerProxy()) {
    return raw;
  }
  if (typeof raw !== 'string') {
    return raw;
  }
  const origin = normaliseBaseUrl(baseUrl);
  const prefix = getMaptilerProxyPrefix(origin);
  if (raw.startsWith(prefix) || raw.startsWith(MAPTILER_PROXY_LOCAL_PREFIX)) {
    return raw;
  }
  if (!raw.includes('api.maptiler.com')) {
    return raw.replace(/([?&])key=[^&"'\s]+/gi, (match, pfx) => (pfx === '?' ? '?' : ''))
      .replace(/\?&/g, '?')
      .replace(/\?($|["'])/g, '$1');
  }
  try {
    const candidate = raw.startsWith('http') ? raw : `https://${raw.replace(/^\/\//, '')}`;
    const url = new URL(candidate);
    if (!url.hostname || !url.hostname.includes('maptiler.com')) {
      return raw;
    }
    url.searchParams.delete('key');
    const pathname = url.pathname.replace(/^\/+/u, '');
    const decodedPath = decodeURIComponent(pathname);
    const remaining = url.searchParams.toString();
    return `${prefix}${decodedPath}${remaining ? `?${remaining}` : ''}`;
  } catch {
    return raw.replace(/https:\/\/api\.maptiler\.com\//gi, prefix)
      .replace(/([?&])key=[^&"'\s]+/gi, (match, pfx) => (pfx === '?' ? '?' : ''))
      .replace(/\?&/g, '?')
      .replace(/\?($|["'])/g, '$1');
  }
}

function escapeForJsonPath(value) {
  return value.replace(/\//g, '\\/');
}

function sanitizeMaptilerText(text, baseUrl = '') {
  if (typeof text !== 'string' || !text) {
    return text;
  }
  if (!shouldUseMaptilerProxy()) {
    return text;
  }
  const origin = normaliseBaseUrl(baseUrl);
  let sanitized = text.replace(/https?:\/\/api\.maptiler\.com\/[^\s"')]+/gi, (match) => sanitizeMaptilerUrl(match, origin));
  sanitized = sanitized.replace(/https:\\\/\\\/api\.maptiler\.com\\\/[^\s"')]+/gi, (match) => {
    const normalized = match.replace(/\\\//g, '/');
    const transformed = sanitizeMaptilerUrl(normalized, origin);
    if (typeof transformed !== 'string') {
      return match;
    }
    return transformed.startsWith(MAPTILER_PROXY_LOCAL_PREFIX)
      ? escapeForJsonPath(transformed)
      : transformed.replace(/\//g, '\\/');
  });
  sanitized = sanitized.replace(/([?&])key=[^&"'\s]+/gi, (match, prefix) => (prefix === '?' ? '?' : ''))
    .replace(/\?&/g, '?')
    .replace(/\?($|["'])/g, '$1');
  return sanitized;
}

function resolveDataDirectory(isProduction) {
  if (process.env.DATA_DIR) {
    return process.env.DATA_DIR;
  }

  const candidates = [];
  if (isProduction) {
    candidates.push(resolve('dist/client/data'));
  }
  candidates.push(resolve('public/data'));

  for (const dir of candidates) {
    if (existsSync(dir)) {
      return dir;
    }
  }

  return candidates[candidates.length - 1];
}

function resolveStylePath(isProduction) {
  if (process.env.MAP_STYLE_PATH) {
    return process.env.MAP_STYLE_PATH;
  }

  const candidates = [];
  if (isProduction) {
    candidates.push(resolve('dist/client/styles/basic-style.json'));
  }
  candidates.push(resolve('public/styles/basic-style.json'));

  for (const filePath of candidates) {
    if (existsSync(filePath)) {
      return filePath;
    }
  }

  return candidates[candidates.length - 1];
}

// Set data directory for bundled server modules
const isProd = process.env.NODE_ENV === 'production';
const dataDir = resolveDataDirectory(isProd);
const stylePath = resolveStylePath(isProd);
if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = dataDir;
}
if (isProd && !dataDir.includes('dist/client/data')) {
  console.warn(`Using fallback data directory: ${dataDir}`);
}

const styleCache = {
  proxyTemplate: null,
  proxyMtime: null,
};

function invalidateStyleCache() {
  styleCache.proxyTemplate = null;
  styleCache.proxyMtime = null;
}

let loggedMissingMapKey = false;
const WARD_DATASETS = new Set(['red_light_locations', 'ase_locations', 'cameras_combined']);
const REDIS_NAMESPACE = process.env.MAP_DATA_REDIS_NAMESPACE || 'toronto:map-data';
const ENABLE_LEGACY_TILE_RENDERER = process.env.ENABLE_LEGACY_TILE_RENDERER === '1';
const RESET_CACHE_ON_BOOT = process.env.RESET_CACHE_ON_BOOT !== '0';
const VACUUM_ON_BOOT = process.env.VACUUM_ON_BOOT !== '0';
const VACUUM_START_DELAY_MS = Number.parseInt(process.env.VACUUM_START_DELAY_MS || '60000', 10);
const DEFAULT_VACUUM_TABLES = ['parking_ticket_tiles', 'red_light_camera_tiles', 'ase_camera_tiles'];
const VACUUM_TABLE_LIST = (process.env.STARTUP_VACUUM_TABLES || DEFAULT_VACUUM_TABLES.join(','))
  .split(',')
  .map((entry) => entry.trim())
  .filter((entry) => entry.length > 0);

const redisSettings = getRedisConfig();
const MAPTILER_REDIS_ENABLED = Boolean(redisSettings.enabled && redisSettings.url);
const MAPTILER_PROXY_MODE = (process.env.MAPTILER_PROXY_MODE || 'proxy').trim().toLowerCase();
const MAPTILER_PROXY_RECHECK_MS = Number.parseInt(process.env.MAPTILER_PROXY_RECHECK_MS || '', 10) || 5 * 60 * 1000;
const ALLOWED_PROXY_MODES = new Set(['auto', 'proxy', 'direct']);
const resolvedProxyMode = ALLOWED_PROXY_MODES.has(MAPTILER_PROXY_MODE) ? MAPTILER_PROXY_MODE : 'proxy';
const maptilerProxyState = {
  mode: resolvedProxyMode,
  proxyEnabled: resolvedProxyMode === 'proxy',
  lastProbeAt: 0,
  probing: null,
};
const MAPTILER_REDIS_NAMESPACE = REDIS_NAMESPACE;
const MAPTILER_CACHE_PREFIX = `${MAPTILER_REDIS_NAMESPACE}:maptiler:v1`;
const MAPTILER_PROXY_TIMEOUT_MS = Number.parseInt(process.env.MAPTILER_PROXY_TIMEOUT_MS || '', 10) || 12_000;
const MAPTILER_PROXY_MAX_RETRIES = Number.parseInt(process.env.MAPTILER_PROXY_MAX_RETRIES || '', 10) || 2;
const MAPTILER_PROXY_BACKOFF_MS = Number.parseInt(process.env.MAPTILER_PROXY_BACKOFF_MS || '', 10) || 500;
const MAPTILER_TILE_CACHE_CONTROL = 'public, max-age=86400, stale-while-revalidate=3600';
const MAPTILER_FONT_CACHE_CONTROL = 'public, max-age=86400, stale-while-revalidate=3600';
const MAPTILER_DEFAULT_CACHE_CONTROL = 'public, max-age=3600, stale-while-revalidate=600';
const MAPTILER_FALLBACK_TIMEOUT_MS = Number.parseInt(process.env.MAPTILER_PROXY_FALLBACK_TIMEOUT_MS || '', 10) || 20_000;

const TILE_SLOW_LOG_THRESHOLD_MS = Number.parseInt(process.env.TILE_SLOW_LOG_MS || '', 10) || 800;
const GLOW_TILE_CACHE_CONTROL = process.env.GLOW_TILE_CACHE_CONTROL || 'public, max-age=86400, stale-while-revalidate=3600';
const GLOW_TILE_TIMEOUT_MS = Number.parseInt(process.env.GLOW_TILE_TIMEOUT_MS || '', 10) || 2500;
const GLOW_TILE_SLOW_THRESHOLD_MS = Number.parseInt(process.env.GLOW_TILE_SLOW_MS || '', 10) || 600;
const GLOW_ALLOWED_DATASETS = new Set(['parking_tickets', 'red_light_locations', 'ase_locations']);
const TILE_JSON_CACHE_CONTROL = process.env.TILE_JSON_CACHE_CONTROL || 'public, max-age=60, stale-while-revalidate=300';

const metrics = {
  maptiler: {
    requests: 0,
    errors: 0,
    totalDurationMs: 0,
    maxDurationMs: 0,
    slowCount: 0,
    modes: { proxy: 0, direct: 0 },
    fallbacks: 0,
    lastStatus: null,
    lastDurationMs: 0,
    lastErrorMessage: null,
  },
  pmtiles: {
    warmup: {
      tilesFetched: 0,
      tilesFailed: 0,
      originTiles: 0,
      cdnTiles: 0,
      lastRunStartedAt: null,
      lastRunDurationMs: 0,
      lastErrorMessage: null,
      lastRunTilesFetched: 0,
      lastRunTilesFailed: 0,
      lastRunOriginTiles: 0,
      lastRunCdnTiles: 0,
    },
  },
  glowTiles: {
    requests: 0,
    hits: 0,
    misses: 0,
    errors: 0,
    totalDurationMs: 0,
    maxDurationMs: 0,
    lastErrorMessage: null,
  },
  client: {
    lastSubmission: null,
    payload: null,
  },
  ssr: {
    requests: 0,
    cacheHits: 0,
    cacheMisses: 0,
    totalDurationMs: 0,
    maxDurationMs: 0,
    lastDurationMs: 0,
    appData: {
      runs: 0,
      totalDurationMs: 0,
      maxDurationMs: 0,
      lastDurationMs: 0,
    },
  },
};

function recordMaptilerMetric({ durationMs, success, statusCode, mode, usedFallback = false, errorMessage = null }) {
  if (!Number.isFinite(durationMs) || durationMs < 0) {
    return;
  }
  metrics.maptiler.requests += 1;
  metrics.maptiler.totalDurationMs += durationMs;
  metrics.maptiler.maxDurationMs = Math.max(metrics.maptiler.maxDurationMs, durationMs);
  metrics.maptiler.lastDurationMs = durationMs;
  metrics.maptiler.lastStatus = statusCode;
  if (durationMs >= TILE_SLOW_LOG_THRESHOLD_MS) {
    metrics.maptiler.slowCount += 1;
  }
  if (!success) {
    metrics.maptiler.errors += 1;
    if (errorMessage) {
      metrics.maptiler.lastErrorMessage = errorMessage;
    }
  } else if (!errorMessage) {
    metrics.maptiler.lastErrorMessage = null;
  }
  if (mode && metrics.maptiler.modes[mode] !== undefined) {
    metrics.maptiler.modes[mode] += 1;
  }
  if (usedFallback) {
    metrics.maptiler.fallbacks += 1;
  }
}

function recordSsrMetric({ durationMs, fromCache, appDataDurationMs }) {
  if (Number.isFinite(durationMs)) {
    metrics.ssr.requests += 1;
    metrics.ssr.totalDurationMs += durationMs;
    metrics.ssr.maxDurationMs = Math.max(metrics.ssr.maxDurationMs, durationMs);
    metrics.ssr.lastDurationMs = durationMs;
  }
  if (fromCache === true) {
    metrics.ssr.cacheHits += 1;
  } else if (fromCache === false) {
    metrics.ssr.cacheMisses += 1;
  }
  if (Number.isFinite(appDataDurationMs)) {
    metrics.ssr.appData.runs += 1;
    metrics.ssr.appData.totalDurationMs += appDataDurationMs;
    metrics.ssr.appData.maxDurationMs = Math.max(metrics.ssr.appData.maxDurationMs, appDataDurationMs);
    metrics.ssr.appData.lastDurationMs = appDataDurationMs;
  }
}

function recordPmtilesTileMetric({ success, source }) {
  metrics.pmtiles.warmup.tilesFetched += 1;
  if (!success) {
    metrics.pmtiles.warmup.tilesFailed += 1;
  }
  if (source === 'cdn') {
    metrics.pmtiles.warmup.cdnTiles += 1;
  } else {
    metrics.pmtiles.warmup.originTiles += 1;
  }
}

function recordPmtilesRunMetric({
  startedAt,
  durationMs,
  tileCount,
  failureCount,
  originTiles,
  cdnTiles,
  errorMessage,
}) {
  metrics.pmtiles.warmup.lastRunStartedAt = startedAt;
  metrics.pmtiles.warmup.lastRunDurationMs = durationMs;
  metrics.pmtiles.warmup.lastRunTilesFetched = tileCount;
  metrics.pmtiles.warmup.lastRunTilesFailed = failureCount;
  metrics.pmtiles.warmup.lastRunOriginTiles = originTiles;
  metrics.pmtiles.warmup.lastRunCdnTiles = cdnTiles;
  if (failureCount > 0) {
    metrics.pmtiles.warmup.lastErrorMessage = errorMessage || metrics.pmtiles.warmup.lastErrorMessage;
  } else if (errorMessage) {
    metrics.pmtiles.warmup.lastErrorMessage = errorMessage;
  } else {
    metrics.pmtiles.warmup.lastErrorMessage = null;
  }
}

const pmtilesRuntimeConfig = getPmtilesRuntimeConfig();
console.log('[pmtiles] runtime config', {
  enabled: pmtilesRuntimeConfig.enabled,
  publicBaseUrl: pmtilesRuntimeConfig.publicBaseUrl,
  privateBaseUrl: pmtilesRuntimeConfig.privateBaseUrl,
  cdnBaseUrl: pmtilesRuntimeConfig.cdnBaseUrl,
  bucket: pmtilesRuntimeConfig.bucket,
  region: pmtilesRuntimeConfig.region,
  objectPrefix: pmtilesRuntimeConfig.objectPrefix,
});
const pmtilesManifest = buildPmtilesManifest(pmtilesRuntimeConfig);
console.log('[pmtiles] manifest status', {
  enabled: pmtilesManifest.enabled,
  baseUrl: pmtilesManifest.baseUrl,
  originBaseUrl: pmtilesManifest.originBaseUrl,
  cdnBaseUrl: pmtilesManifest.cdnBaseUrl,
  objectPrefix: pmtilesManifest.objectPrefix,
  datasetCount: Object.keys(pmtilesManifest.datasets || {}).length,
  wardDatasetCount: Object.keys(pmtilesManifest.wardDatasets || {}).length,
});
console.log('[maptiler] basemap mode', {
  mode: maptilerProxyState.mode,
  proxyEnabled: shouldUseMaptilerProxy(),
});
if (pmtilesManifest.enabled) {
  schedulePmtilesWarmup(pmtilesManifest, pmtilesRuntimeConfig, {
    onTileFetched: recordPmtilesTileMetric,
    onRunComplete: recordPmtilesRunMetric,
  });
}

scheduleMaptilerProxyProbe();
validateMaptilerConfiguration().catch((error) => {
  console.warn('[maptiler] configuration validation failed:', error?.message || error);
});

function clampLatitude(lat) {
  if (!Number.isFinite(lat)) {
    return 0;
  }
  const max = 85.05112878;
  return Math.min(Math.max(lat, -max), max);
}

function wrapTileX(x, zoom) {
  const scale = 2 ** zoom;
  if (!Number.isFinite(scale) || scale <= 0) {
    return 0;
  }
  const wrapped = ((x % scale) + scale) % scale;
  return Number.isFinite(wrapped) ? wrapped : 0;
}

function clampTileY(y, zoom) {
  const scale = 2 ** zoom;
  if (!Number.isFinite(scale) || scale <= 0) {
    return 0;
  }
  if (!Number.isFinite(y)) {
    return 0;
  }
  return Math.min(Math.max(y, 0), scale - 1);
}

function lonLatToTileIndices(longitude, latitude, zoom) {
  const tileZoom = Math.max(0, Math.floor(Number.isFinite(zoom) ? zoom : 0));
  const boundedLat = clampLatitude(latitude);
  const scale = 2 ** tileZoom;
  const x = Math.floor(((longitude + 180) / 360) * scale);
  const latRad = boundedLat * (Math.PI / 180);
  const y = Math.floor(((1 - Math.log(Math.tan(latRad) + (1 / Math.cos(latRad))) / Math.PI) / 2) * scale);
  return {
    z: tileZoom,
    x: wrapTileX(x, tileZoom),
    y: clampTileY(y, tileZoom),
  };
}

function enumerateTilesForBounds(bounds, zoom) {
  if (!bounds || !Number.isFinite(bounds.west) || !Number.isFinite(bounds.south)
    || !Number.isFinite(bounds.east) || !Number.isFinite(bounds.north)) {
    return [];
  }
  const west = bounds.west;
  const south = bounds.south;
  const east = bounds.east;
  const north = bounds.north;
  const zoomLevel = Math.max(0, Math.floor(Number.isFinite(zoom) ? zoom : 0));
  const nw = lonLatToTileIndices(west, north, zoomLevel);
  const se = lonLatToTileIndices(east, south, zoomLevel);
  const scale = 2 ** zoomLevel;
  const minX = Math.min(nw.x, se.x);
  const maxX = Math.max(nw.x, se.x);
  const minY = Math.min(nw.y, se.y);
  const maxY = Math.max(nw.y, se.y);
  const tiles = [];
  for (let x = minX; x <= maxX; x += 1) {
    for (let y = minY; y <= maxY; y += 1) {
      tiles.push({
        z: zoomLevel,
        x: wrapTileX(x, zoomLevel),
        y: clampTileY(y, zoomLevel),
      });
    }
  }
  if (tiles.length === 0 && scale > 0) {
    tiles.push({
      z: zoomLevel,
      x: wrapTileX(nw.x, zoomLevel),
      y: clampTileY(nw.y, zoomLevel),
    });
  }
  return tiles;
}

async function warmInitialTiles(reason = 'startup') {
  if (!TILE_WARMUP_ENABLED) {
    return;
  }
  if (!postgisTileService.isEnabled()) {
    console.warn('[warmup] postgis tile service disabled; skipping tile warmup');
    return;
  }
  if (!Array.isArray(TILE_WARMUP_ZOOMS) || TILE_WARMUP_ZOOMS.length === 0) {
    return;
  }
  const targetDatasets = TILE_WARMUP_DATASETS
    .filter((dataset) => postgisTileService.isDatasetEnabled(dataset));
  if (targetDatasets.length === 0) {
    console.warn('[warmup] no datasets enabled for tile warmup');
    return;
  }

  const bounds = TILE_WARMUP_BOUNDS;
  if (![bounds.west, bounds.south, bounds.east, bounds.north].every(Number.isFinite)) {
    console.warn('[warmup] invalid warmup bounds; skipping tile warmup');
    return;
  }

  const startedAt = Date.now();
  let warmed = 0;
  // Memory optimization: Add delay between tiles to allow GC and reduce memory spikes
  const WARMUP_DELAY_MS = Number.parseInt(process.env.TILE_WARMUP_DELAY_MS || '', 10) || 100;
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  for (const dataset of targetDatasets) {
    for (const zoom of TILE_WARMUP_ZOOMS) {
      const coords = enumerateTilesForBounds(bounds, zoom);
      for (const coord of coords) {
        try {
          const tile = await postgisTileService.getTile(dataset, coord.z, coord.x, coord.y, { prefetch: true });
          if (tile?.buffer?.length) {
            warmed += 1;
          }
          // Memory optimization: Pause between requests to reduce memory pressure
          if (WARMUP_DELAY_MS > 0) {
            await sleep(WARMUP_DELAY_MS);
          }
        } catch (error) {
          console.warn(
            `[warmup] failed to prefetch ${dataset} tile ${coord.z}/${coord.x}/${coord.y}:`,
            error?.message || error,
          );
        }
      }
    }
  }

  const durationMs = Date.now() - startedAt;
  console.log('[warmup] postgis tiles prepared', {
    datasets: targetDatasets.length,
    tiles: warmed,
    zooms: TILE_WARMUP_ZOOMS,
    durationMs,
    reason,
  });
}

let maptilerRedisPromise = null;
const maptilerInflight = new Map();
const TEXT_CONTENT_TYPE_REGEX = /json|text|javascript|xml/i;

let healthPgPool = null;

async function checkRedisHealth() {
  const config = getRedisConfig();
  if (!config.enabled || !config.url) {
    return { status: 'disabled' };
  }
  const client = createClient({ url: config.url });
  try {
    await client.connect();
    await client.ping();
    return { status: 'ok' };
  } catch (error) {
    return { status: 'error', error: error.message };
  } finally {
    try {
      if (client.isOpen) {
        await client.quit();
      }
    } catch (quitError) {
      console.warn('Failed to close Redis client during health check:', quitError.message);
    }
  }
}

function getHealthPgPool() {
  if (healthPgPool) {
    return healthPgPool;
  }
  const pgConfig = getCoreDbConfig();
  const connectionString = pgConfig.readOnlyConnectionString || pgConfig.connectionString;
  if (!pgConfig.enabled || !connectionString) {
    return null;
  }
  healthPgPool = new Pool({
    connectionString,
    ssl: pgConfig.ssl,
    application_name: 'healthz',
  });
  healthPgPool.on('error', (error) => {
    console.warn('[healthz] Postgres pool error:', error.message);
  });
  return healthPgPool;
}

async function checkPostgresHealth() {
  const pool = getHealthPgPool();
  if (!pool) {
    return { status: 'disabled' };
  }
  try {
    await pool.query('SELECT 1');
    return { status: 'ok' };
  } catch (error) {
    return { status: 'error', error: error.message };
  }
}

class MaptilerHttpError extends Error {
  constructor(status, body = '', headers = {}) {
    super(`MapTiler responded with ${status}`);
    this.name = 'MaptilerHttpError';
    this.status = status;
    this.body = body;
    this.headers = headers;
  }
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function randomJitter(ms) {
  if (!Number.isFinite(ms) || ms <= 0) {
    return 0;
  }
  return Math.floor(Math.random() * ms);
}

async function resetRedisNamespaceOnBoot() {
  if (!RESET_CACHE_ON_BOOT || !redisSettings.enabled || !redisSettings.url) {
    return;
  }
  const client = createClient({ url: redisSettings.url });
  const matchPattern = `${REDIS_NAMESPACE}:*`;
  let deleted = 0;
  let batch = [];
  try {
    await client.connect();
    for await (const key of client.scanIterator({ MATCH: matchPattern, COUNT: 1000 })) {
      batch.push(key);
      if (batch.length >= 512) {
        // redis v5+ DEL requires spread arguments, not array
        if (batch.length > 0) {
          const removed = await client.del(...batch);
          deleted += Number(removed) || 0;
        }
        batch = [];
      }
    }
    // Flush remaining keys - redis v5+ DEL requires spread arguments
    if (batch.length > 0) {
      const removed = await client.del(...batch);
      deleted += Number(removed) || 0;
    }
    if (deleted > 0) {
      console.log(`Cleared ${deleted} Redis keys for namespace '${REDIS_NAMESPACE}'`);
    } else {
      console.log(`Redis namespace '${REDIS_NAMESPACE}' already empty.`);
    }
  } catch (error) {
    console.warn('Failed to reset Redis namespace on boot:', error.message);
  } finally {
    batch = [];
    try {
      await client.quit();
    } catch (quitError) {
      console.warn('Failed to close Redis client after namespace reset:', quitError.message);
    }
  }
}

async function vacuumPostgresOnBoot() {
  if (!VACUUM_ON_BOOT || VACUUM_TABLE_LIST.length === 0) {
    return;
  }
  const pgConfig = getTileDbConfig();
  const connectionString = pgConfig.readOnlyConnectionString || pgConfig.connectionString;
  if (!pgConfig.enabled || !connectionString) {
    return;
  }
  const pool = new Pool({
    connectionString,
    ssl: pgConfig.ssl,
    application_name: 'startup-vacuum',
  });
  try {
    for (const table of VACUUM_TABLE_LIST) {
      try {
        const definition = await resolveTableDefinition(pool, table);
        if (!definition) {
          console.warn(`VACUUM skipped for ${table}: table not found`);
          continue;
        }
        const qualified = formatQualifiedName(definition.schema_name, definition.table_name);
        await pool.query(`VACUUM (ANALYZE) ${qualified};`);
        console.log(`VACUUM ANALYZE completed for ${qualified}`);
      } catch (tableError) {
        console.warn(`VACUUM failed for ${table}:`, tableError.message);
      }
    }
  } catch (error) {
    console.warn('Startup Postgres vacuum encountered an error:', error.message);
  } finally {
    await pool.end().catch((error) => {
      console.warn('Failed to close Postgres pool after vacuum:', error.message);
    });
  }
}

function schedulePostgresVacuum() {
  if (!VACUUM_ON_BOOT || VACUUM_TABLE_LIST.length === 0) {
    return;
  }
  const delay = Number.isFinite(VACUUM_START_DELAY_MS) && VACUUM_START_DELAY_MS >= 0
    ? VACUUM_START_DELAY_MS
    : 60_000;
  setTimeout(() => {
    vacuumPostgresOnBoot().catch((error) => {
      console.warn('Startup Postgres vacuum failed:', error.message);
    });
  }, delay);
}

function quoteIdentifier(value) {
  return `"${String(value).replace(/"/g, '""')}"`;
}

function formatQualifiedName(schema, name) {
  return `${quoteIdentifier(schema)}.${quoteIdentifier(name)}`;
}

async function resolveTableDefinition(pool, rawName) {
  const hasSchema = rawName.includes('.');
  const [schemaCandidate, tableName] = hasSchema ? rawName.split('.', 2) : [null, rawName];
  const query = `
    SELECT n.nspname AS schema_name, c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND c.relname = $1
      ${schemaCandidate ? 'AND n.nspname = $2' : ''}
    ORDER BY (n.nspname = 'public') DESC
    LIMIT 1;
  `;
  const params = schemaCandidate ? [tableName, schemaCandidate] : [tableName];
  let result = await pool.query(query, params);
  if (result.rowCount === 0 && !schemaCandidate) {
    result = await pool.query(
      `
        SELECT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p')
          AND n.nspname = 'public'
          AND c.relname = $1
        LIMIT 1;
      `,
      [tableName],
    );
  }
  return result.rows[0] || null;
}

function isTextLikeContentType(contentType) {
  if (!contentType) {
    return false;
  }
  return TEXT_CONTENT_TYPE_REGEX.test(contentType);
}

async function formatMaptilerResponse(response, originHint = '') {
  const headerEntries = {};
  response.headers.forEach((value, header) => {
    headerEntries[header.toLowerCase()] = value;
  });
  const contentType = response.headers.get('content-type') || '';
  let bodyBuffer;
  if (isTextLikeContentType(contentType)) {
    const text = await response.text();
    bodyBuffer = Buffer.from(sanitizeMaptilerText(text, originHint || ''));
  } else {
    const arrayBuffer = await response.arrayBuffer();
    bodyBuffer = Buffer.from(arrayBuffer);
  }
  return {
    status: response.status,
    headers: filterUpstreamHeaders(headerEntries),
    body: bodyBuffer,
  };
}

async function fetchMaptilerDirect(url, headers, originHint, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort();
  }, timeoutMs);
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers,
      redirect: 'follow',
      signal: controller.signal,
    });
    if (!response.ok) {
      const bodyText = await response.text().catch(() => '');
      throw new MaptilerHttpError(response.status, bodyText);
    }
    return formatMaptilerResponse(response, originHint);
  } finally {
    clearTimeout(timer);
  }
}

function isAbortError(error) {
  if (!error) {
    return false;
  }
  return error.name === 'AbortError' || (typeof error.message === 'string' && error.message.toLowerCase().includes('abort'));
}

async function getMaptilerRedisClient() {
  if (!MAPTILER_REDIS_ENABLED) {
    return null;
  }
  if (maptilerRedisPromise) {
    try {
      const existing = await maptilerRedisPromise;
      if (existing && existing.isOpen) {
        return existing;
      }
    } catch (error) {
      console.warn('MapTiler Redis client error:', error.message);
    }
    maptilerRedisPromise = null;
  }

  maptilerRedisPromise = (async () => {
    const client = createClient({ url: redisSettings.url });
    const reset = () => {
      if (maptilerRedisPromise) {
        maptilerRedisPromise = null;
      }
    };
    client.on('error', (error) => {
      console.warn('MapTiler Redis connection error:', error.message);
    });
    client.on('end', reset);
    client.on('close', reset);
    try {
      await client.connect();
      return client;
    } catch (error) {
      reset();
      console.warn('Failed to connect to Redis for MapTiler cache:', error.message);
      try {
        await client.disconnect();
      } catch (disconnectError) {
        console.warn('Error closing MapTiler Redis client after failure:', disconnectError.message);
      }
      return null;
    }
  })();

  const client = await maptilerRedisPromise;
  return client && client.isOpen ? client : null;
}

function hashForCache(value) {
  return createHash('sha1').update(value).digest('hex');
}

function buildCanonicalQuery(searchParams) {
  if (!searchParams) {
    return '';
  }
  const entries = [];
  for (const [key, value] of searchParams.entries()) {
    if (key === 'key') {
      continue;
    }
    if (Array.isArray(value)) {
      for (const entry of value) {
        entries.push([key, entry]);
      }
    } else {
      entries.push([key, value]);
    }
  }
  entries.sort((a, b) => (a[0] === b[0] ? String(a[1]).localeCompare(String(b[1])) : a[0].localeCompare(b[0])));
  if (!entries.length) {
    return '';
  }
  return entries.map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value ?? '')}`).join('&');
}

function resolveMaptilerCacheTtl(resourcePath) {
  if (resourcePath.startsWith('tiles/')) {
    return 60 * 60 * 6;
  }
  if (resourcePath.startsWith('fonts/') || resourcePath.endsWith('.pbf')) {
    return 60 * 60 * 24;
  }
  if (resourcePath.endsWith('.json') || resourcePath.includes('/styles/')) {
    return 60 * 60 * 6;
  }
  return 60 * 60;
}

function resolveDownstreamCacheControl(resourcePath) {
  if (resourcePath.startsWith('tiles/')) {
    return MAPTILER_TILE_CACHE_CONTROL;
  }
  if (resourcePath.startsWith('fonts/')) {
    return MAPTILER_FONT_CACHE_CONTROL;
  }
  return MAPTILER_DEFAULT_CACHE_CONTROL;
}

function normaliseTileResourcePath(resourcePath) {
  if (typeof resourcePath !== 'string' || !resourcePath.startsWith('tiles/')) {
    return resourcePath;
  }
  const match = resourcePath.match(/^(tiles\/[\w-]+\/)(\d+)\/(-?\d+)\/(-?\d+)(\.[a-z0-9]+)?$/i);
  if (!match) {
    return resourcePath;
  }
  const [, prefix, zStr, xStr, yStr, suffix = ''] = match;
  const z = Number.parseInt(zStr, 10);
  const x = Number.parseInt(xStr, 10);
  const y = Number.parseInt(yStr, 10);
  if (!Number.isFinite(z) || z < 0 || !Number.isFinite(x) || !Number.isFinite(y)) {
    return resourcePath;
  }
  const worldSize = 2 ** z;
  if (!Number.isFinite(worldSize) || worldSize <= 0) {
    return resourcePath;
  }
  const wrappedX = ((x % worldSize) + worldSize) % worldSize;
  const clampedY = Math.min(Math.max(y, 0), worldSize - 1);
  if (wrappedX === x && clampedY === y) {
    return resourcePath;
  }
  return `${prefix}${z}/${wrappedX}/${clampedY}${suffix}`;
}

async function probeMaptilerProxy() {
  if (maptilerProxyState.mode === 'direct') {
    maptilerProxyState.proxyEnabled = false;
    return false;
  }
  if (maptilerProxyState.probing) {
    return maptilerProxyState.probing;
  }
  const probe = (async () => {
    const key = process.env.MAPLIBRE_API_KEY || process.env.MAPTILER_API_KEY || '';
    if (!key) {
      if (maptilerProxyState.mode !== 'proxy' && maptilerProxyState.proxyEnabled) {
        console.warn('MapTiler proxy disabled: API key missing.');
        maptilerProxyState.proxyEnabled = false;
      }
      return false;
    }
    const resourcePath = 'tiles/v3/10/300/385.pbf';
    const upstreamUrl = new URL(`https://api.maptiler.com/${resourcePath}`);
    upstreamUrl.searchParams.set('key', key);
    const headers = {
      Accept: 'application/x-protobuf',
      'User-Agent': process.env.MAPTILER_USER_AGENT || 'toronto-parking-proxy/1.0',
    };
    try {
      const response = await fetchMaptilerWithRetry(upstreamUrl, headers);
      if (!response || !response.ok) {
        throw new Error(`status ${response ? response.status : 'unknown'}`);
      }
      const payload = await response.arrayBuffer();
      if (!payload || payload.byteLength === 0) {
        throw new Error('empty payload');
      }
      if (!maptilerProxyState.proxyEnabled) {
        console.log('[maptiler] proxy probe succeeded; enabling proxy-backed basemap.');
      }
      maptilerProxyState.proxyEnabled = true;
      maptilerProxyState.lastProbeAt = Date.now();
      invalidateStyleCache();
      return true;
    } catch (error) {
      maptilerProxyState.lastProbeAt = Date.now();
      if (maptilerProxyState.proxyEnabled) {
        console.warn(`[maptiler] proxy probe failed; forcing direct tiles: ${error.message}`);
      }
      maptilerProxyState.proxyEnabled = false;
      invalidateStyleCache();
      return false;
    }
  })();
  maptilerProxyState.probing = probe.finally(() => {
    maptilerProxyState.probing = null;
  });
  return maptilerProxyState.probing;
}

function scheduleMaptilerProxyProbe() {
  if (maptilerProxyState.mode === 'direct') {
    return;
  }
  const initialDelay = maptilerProxyState.mode === 'proxy' ? 0 : 1000;
  setTimeout(() => {
    probeMaptilerProxy().catch((error) => {
      console.warn('[maptiler] initial proxy probe failed:', error.message);
    });
  }, initialDelay);

  if (maptilerProxyState.mode === 'auto' && MAPTILER_PROXY_RECHECK_MS > 0) {
    setInterval(() => {
      if (maptilerProxyState.mode !== 'auto') {
        return;
      }
      if (maptilerProxyState.proxyEnabled) {
        return;
      }
      probeMaptilerProxy().catch((error) => {
        console.warn('[maptiler] scheduled proxy probe failed:', error.message);
      });
    }, MAPTILER_PROXY_RECHECK_MS);
  }
}

function buildMaptilerDescriptor(resourcePath, searchParams) {
  const normalizedPath = resourcePath.replace(/^\/+/, '');
  const canonicalQuery = buildCanonicalQuery(searchParams);
  const canonical = canonicalQuery ? `${normalizedPath}?${canonicalQuery}` : normalizedPath;
  const cacheKey = `${MAPTILER_CACHE_PREFIX}:${hashForCache(canonical)}`;
  return {
    cacheKey,
    cacheable: MAPTILER_REDIS_ENABLED,
    canonical,
    resourcePath: normalizedPath,
    ttlSeconds: resolveMaptilerCacheTtl(normalizedPath),
    downstreamCacheControl: resolveDownstreamCacheControl(normalizedPath),
  };
}

async function readMaptilerCache(cacheKey) {
  if (!MAPTILER_REDIS_ENABLED) {
    return null;
  }
  const client = await getMaptilerRedisClient();
  if (!client) {
    return null;
  }
  try {
    const stored = await client.get(cacheKey);
    if (!stored) {
      return null;
    }
    const parsed = JSON.parse(stored);
    if (!parsed || typeof parsed.base64 !== 'string') {
      return null;
    }
    return {
      status: parsed.status || 200,
      headers: parsed.headers || {},
      body: Buffer.from(parsed.base64, 'base64'),
      storedAt: parsed.storedAt || null,
    };
  } catch (error) {
    console.warn('Failed to read MapTiler cache from Redis:', error.message);
    return null;
  }
}

async function writeMaptilerCache(cacheKey, payload, ttlSeconds) {
  if (!MAPTILER_REDIS_ENABLED || !payload || !payload.body || payload.body.length === 0) {
    return;
  }
  const client = await getMaptilerRedisClient();
  if (!client) {
    return;
  }
  try {
    const envelope = {
      status: payload.status,
      headers: payload.headers,
      base64: payload.body.toString('base64'),
      storedAt: Date.now(),
    };
    const options = Number.isFinite(ttlSeconds) && ttlSeconds > 0 ? { EX: ttlSeconds } : {};
    await client.set(cacheKey, JSON.stringify(envelope), options);
  } catch (error) {
    console.warn('Failed to cache MapTiler payload in Redis:', error.message);
  }
}

function filterUpstreamHeaders(headers) {
  const allowed = ['content-type', 'content-encoding', 'last-modified', 'etag', 'x-tileset-version'];
  const sanitized = {};
  for (const [name, value] of Object.entries(headers)) {
    const lower = name.toLowerCase();
    if (!allowed.includes(lower)) {
      continue;
    }
    sanitized[lower] = value;
  }
  return sanitized;
}

async function fetchMaptilerWithRetry(url, initHeaders = {}) {
  let attempt = 0;
  let lastError = null;
  const maxAttempts = MAPTILER_PROXY_MAX_RETRIES + 1;
  while (attempt < maxAttempts) {
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
    }, MAPTILER_PROXY_TIMEOUT_MS + attempt * 500);
    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: initHeaders,
        signal: controller.signal,
        redirect: 'follow',
      });
      clearTimeout(timeout);
      if (response.status === 429 && attempt < maxAttempts - 1) {
        lastError = new Error('MapTiler responded with 429');
      } else if (response.status >= 500 && response.status < 600 && attempt < maxAttempts - 1) {
        lastError = new Error(`MapTiler responded with ${response.status}`);
      } else if (!response.ok) {
        const bodyText = await response.text().catch(() => '');
        throw new MaptilerHttpError(response.status, bodyText);
      } else {
        return response;
      }
    } catch (error) {
      clearTimeout(timeout);
      lastError = error;
    }
    attempt += 1;
    if (attempt >= maxAttempts) {
      break;
    }
    const backoff = MAPTILER_PROXY_BACKOFF_MS * 2 ** (attempt - 1);
    await sleep(backoff + randomJitter(200));
  }
  throw lastError || new Error('MapTiler request failed');
}

async function resolveMaptilerResource(descriptor, fetcher) {
  if (descriptor.cacheable) {
    const cached = await readMaptilerCache(descriptor.cacheKey);
    if (cached) {
      return {
        ...cached,
        fromCache: true,
        cacheControl: descriptor.downstreamCacheControl,
      };
    }
  }

  const inflightKey = descriptor.canonical;
  if (maptilerInflight.has(inflightKey)) {
    return maptilerInflight.get(inflightKey);
  }

  const promise = (async () => {
    const payload = await fetcher();
    if (descriptor.cacheable && payload && payload.body && payload.body.length > 0) {
      await writeMaptilerCache(descriptor.cacheKey, payload, descriptor.ttlSeconds);
    }
    return {
      ...payload,
      fromCache: false,
      cacheControl: descriptor.downstreamCacheControl,
    };
  })();

  maptilerInflight.set(inflightKey, promise);
  try {
    return await promise;
  } finally {
    maptilerInflight.delete(inflightKey);
  }
}

async function loadBaseStyle() {
  const key = process.env.MAPLIBRE_API_KEY
    || process.env.MAPTILER_API_KEY
    || process.env.VITE_MAPTILER_KEY
    || '';
  if (!key && !loggedMissingMapKey) {
    console.warn('MAPLIBRE_API_KEY not set; serving MapTiler style with placeholder key.');
    loggedMissingMapKey = true;
  }

  let stats;
  try {
    stats = await fs.stat(stylePath);
  } catch (error) {
    throw new Error(`Base style file not found at ${stylePath}: ${error.message}`);
  }

  if (shouldUseMaptilerProxy()) {
    if (!styleCache.proxyTemplate || styleCache.proxyMtime !== stats.mtimeMs) {
      const raw = await fs.readFile(stylePath, 'utf-8');
      const sanitized = sanitizeMaptilerText(raw, '');
      styleCache.proxyTemplate = typeof sanitized === 'string'
        ? sanitized.replace(/\{\{MAPLIBRE_API_KEY\}\}/g, '')
        : sanitized;
      styleCache.proxyMtime = stats.mtimeMs;
    }
    return styleCache.proxyTemplate;
  }

  const raw = await fs.readFile(stylePath, 'utf-8');
  if (!key) {
    return raw;
  }
  return raw
    .replace(/get_your_own_OpIi9ZULNHzrESv6T2vL/g, key)
    .replace(/\{\{MAPLIBRE_API_KEY\}\}/g, key);
}

async function validateMaptilerConfiguration() {
  const configuredKey = process.env.MAPLIBRE_API_KEY
    || process.env.MAPTILER_API_KEY
    || process.env.VITE_MAPTILER_KEY
    || '';
  if (!configuredKey) {
    console.warn('[maptiler] MAPLIBRE_API_KEY not configured; proxy will fall back to placeholder URLs.');
    return;
  }

  try {
    const style = await loadBaseStyle();
    if (typeof style === 'string') {
      if (/\{\{MAPLIBRE_API_KEY\}\}|get_your_own_/iu.test(style)) {
        console.warn('[maptiler] base style still contains placeholder key; verify MAPLIBRE_API_KEY is loaded in the server environment.');
      }
      if (shouldUseMaptilerProxy()) {
        const proxyPrefix = getMaptilerProxyPrefix();
        if (style.includes('api.maptiler.com') && !style.includes(proxyPrefix)) {
          console.warn('[maptiler] proxy mode enabled but style includes direct MapTiler URLs; ensure sanitizeMaptilerText is applied.');
        }
      } else if (style.includes(MAPTILER_PROXY_LOCAL_PREFIX)) {
        console.warn('[maptiler] direct basemap mode active but style references proxy prefix; consider restarting to refresh the style cache.');
      }
    }
  } catch (error) {
    console.warn('[maptiler] failed to validate base style:', error?.message || error);
  }
}

function encodeTileBuffer(buffer, acceptEncoding = '') {
  if (!buffer || buffer.length < 512) {
    return { buffer, encoding: null };
  }

  const normalized = Array.isArray(acceptEncoding)
    ? acceptEncoding.join(',')
    : String(acceptEncoding || '');

  try {
    if (normalized.includes('br')) {
      return { buffer: brotliCompressSync(buffer), encoding: 'br' };
    }
    if (normalized.includes('gzip')) {
      return { buffer: gzipSync(buffer), encoding: 'gzip' };
    }
  } catch (error) {
    console.warn('Tile compression failed:', error.message);
  }

  return { buffer, encoding: null };
}

function extractTimestamp(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (Number.isFinite(value)) {
    return Number(value);
  }
  if (typeof value === 'string') {
    const match = value.match(/(\d{10,})/);
    if (match) {
      const numeric = Number(match[1]);
      return Number.isFinite(numeric) ? numeric : null;
    }
  }
  return null;
}

const tileService = createTileService();
const postgisTileService = createPostgisTileService();

const startupState = {
  startedAt: Date.now(),
  listening: false,
  readyAt: null,
  dependencies: {
    redis: 'unknown',
    coreDb: 'unknown',
    tileDb: 'unknown',
  },
  merge: {
    status: 'pending',
    lastCompletedAt: null,
    lastError: null,
  },
  warmup: {
    appData: 'pending',
    appDataError: null,
    tileService: 'pending',
    tileServiceError: null,
    wardPrewarm: 'pending',
    wardPrewarmError: null,
  },
};

const tileCacheRuntime = getTileCacheConfig();
const TILE_HTTP_CACHE_CONTROL = `public, max-age=${tileCacheRuntime.baseTtlSeconds}, stale-while-revalidate=${tileCacheRuntime.staleWhileRevalidateSeconds}`;

function markDependencyStatus(name, status) {
  if (startupState.dependencies[name] !== status) {
    startupState.dependencies[name] = status;
  }
}

async function runGeojsonMergeOnce() {
  startupState.merge.status = 'running';
  startupState.merge.lastError = null;
  try {
    await mergeGeoJSONChunks();
    startupState.merge.status = 'complete';
    startupState.merge.lastCompletedAt = new Date().toISOString();
  } catch (error) {
    startupState.merge.status = 'failed';
    startupState.merge.lastError = error?.message || String(error);
    console.warn('GeoJSON merge failed:', error);
  }
}

function scheduleGeojsonMerge() {
  const timer = setTimeout(() => {
    runGeojsonMergeOnce().catch((error) => {
      startupState.merge.status = 'failed';
      startupState.merge.lastError = error?.message || String(error);
      console.warn('GeoJSON merge unhandled failure:', error);
    });
  }, 0);
  if (typeof timer?.unref === 'function') {
    timer.unref();
  }
}

function setWarmupStatus(key, status, error = null) {
  if (!Object.prototype.hasOwnProperty.call(startupState.warmup, key)) {
    return;
  }
  startupState.warmup[key] = status;
  const errorKey = `${key}Error`;
  if (Object.prototype.hasOwnProperty.call(startupState.warmup, errorKey)) {
    startupState.warmup[errorKey] = error;
  }
}

function scheduleProductionWarmup() {
  const timer = setTimeout(() => {
    (async () => {
      setWarmupStatus('appData', 'running');
      try {
        const label = 'app-data:warmup';
        console.time(label);
        try {
          await createAppData({ bypassCache: true });
        } finally {
          console.timeEnd(label);
        }
        setWarmupStatus('appData', 'complete', null);
      } catch (error) {
        console.warn('Unable to warm app data cache:', error.message);
        setWarmupStatus('appData', 'failed', error?.message || String(error));
      }

      setWarmupStatus('tileService', 'running');
      try {
        // Skip GeoJSON tile service when PostGIS handles parking_tickets tiles
        // Loading the 132MB GeoJSON causes OOM on memory-constrained Railway containers
        if (postgisTileService.isDatasetEnabled('parking_tickets')) {
          console.log('[warmup] skipping GeoJSON tile service - PostGIS enabled for parking_tickets');
          setWarmupStatus('tileService', 'skipped', null);
        } else {
          const label = 'tile-service:warmup';
          console.time(label);
          try {
            await tileService.ensureLoaded();
          } finally {
            console.timeEnd(label);
          }
          setWarmupStatus('tileService', 'complete', null);
        }
      } catch (error) {
        console.warn('Unable to warm tile cache:', error.message);
        setWarmupStatus('tileService', 'failed', error?.message || String(error));
      }

      setWarmupStatus('wardPrewarm', 'running');
      try {
        await Promise.all([...WARD_DATASETS].map((dataset) => prewarmWardTiles(dataset)));
        setWarmupStatus('wardPrewarm', 'complete', null);
      } catch (error) {
        console.warn('Ward tile prewarm failed:', error.message);
        setWarmupStatus('wardPrewarm', 'failed', error?.message || String(error));
      }

      try {
        await warmInitialTiles('production-warmup');
      } catch (error) {
        console.warn('[warmup] initial tile warmup failed:', error?.message || error);
      }
    })().catch((error) => {
      console.error('Production warmup encountered an unexpected error:', error);
    });
  }, 0);
  if (typeof timer?.unref === 'function') {
    timer.unref();
  }
}

async function bootstrapStartup() {
  try {
    console.log('🚀 Initializing server...');
    const wakeResults = await wakeRemoteServices();
    console.log(
      `   Redis: ${wakeResults.redis.enabled ? (wakeResults.redis.awake ? 'awake' : 'sleeping') : 'disabled'} | ` +
      `Core DB: ${wakeResults.coreDb.enabled ? (wakeResults.coreDb.awake ? 'awake' : 'sleeping') : 'disabled'} | ` +
      `Tile DB: ${wakeResults.tileDb.enabled ? (wakeResults.tileDb.awake ? 'awake' : 'sleeping') : 'disabled'}`,
    );
    markDependencyStatus(
      'redis',
      wakeResults.redis.enabled ? (wakeResults.redis.awake ? 'ok' : 'error') : 'disabled',
    );
    markDependencyStatus(
      'coreDb',
      wakeResults.coreDb.enabled ? (wakeResults.coreDb.awake ? 'ok' : 'error') : 'disabled',
    );
    markDependencyStatus(
      'tileDb',
      wakeResults.tileDb.enabled ? (wakeResults.tileDb.awake ? 'ok' : 'error') : 'disabled',
    );

    await resetRedisNamespaceOnBoot();
    scheduleGeojsonMerge();

    const refreshIntervalSeconds = Number.parseInt(
      process.env.APP_DATA_REFRESH_SECONDS || (isProd ? '900' : '0'),
      10,
    );

    const backgroundRefresh = startBackgroundAppDataRefresh({
      intervalSeconds: refreshIntervalSeconds,
      createSnapshot: () => createAppData({ bypassCache: true }),
      onAfterRefresh: async () => {
        try {
          // Skip GeoJSON tile service when PostGIS handles parking_tickets tiles
          if (!postgisTileService.isDatasetEnabled('parking_tickets')) {
            await tileService.ensureLoaded();
          }
          await Promise.all([...WARD_DATASETS].map((dataset) => prewarmWardTiles(dataset)));
          await warmInitialTiles('background-refresh');
        } catch (error) {
          console.warn('Background tile priming failed:', error.message);
        }
      },
    });
    // Don't trigger background refresh immediately - the main warmup handles initial data
    // This prevents duplicate parallel warmups that double memory usage at startup
    void backgroundRefresh;

    const preloadTimer = setTimeout(() => {
      Promise.all(
        ['red_light_locations', 'ase_locations'].map((cameraDataset) =>
          loadCameraLocations(cameraDataset).catch((error) => {
            console.warn(`Camera dataset preload failed for ${cameraDataset}:`, error.message);
            return null;
          }),
        ),
      ).catch((error) => {
        console.warn('Camera dataset preload failed:', error.message);
      });
    }, 0);
    if (typeof preloadTimer?.unref === 'function') {
      preloadTimer.unref();
    }
    schedulePostgresVacuum();
  } catch (error) {
    console.error('Startup initialization failed:', error);
  }
}

bootstrapStartup();

function registerTileRoutes(app) {
  const clientMetricsParser = express.json({ limit: '32kb' });

  app.get('/installHook.js.map', (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.status(404).json({ error: 'installHook source map unavailable' });
  });

  app.get('/react_devtools_backend_compact.js.map', (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.status(404).json({ error: 'React DevTools source map unavailable' });
  });

  app.get('/healthz', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    if (req.query.deep === '1') {
      const [redisStatus, postgresStatus] = await Promise.all([
        checkRedisHealth().catch((error) => ({ status: 'error', error: error.message })),
        checkPostgresHealth().catch((error) => ({ status: 'error', error: error.message })),
      ]);
      const checks = {
        redis: redisStatus,
        postgres: postgresStatus,
      };
      const healthy = Object.values(checks).every(
        (entry) => entry.status === 'ok' || entry.status === 'disabled',
      );
      if (!healthy) {
        console.warn('[healthz deep] failing readiness check:', checks);
      }
      const averageDurationMs = metrics.maptiler.requests > 0
        ? Math.round((metrics.maptiler.totalDurationMs / metrics.maptiler.requests) * 100) / 100
        : 0;
      const ssrAverageMs = metrics.ssr.requests > 0
        ? Math.round((metrics.ssr.totalDurationMs / metrics.ssr.requests) * 100) / 100
        : 0;
      const appDataAverageMs = metrics.ssr.appData.runs > 0
        ? Math.round((metrics.ssr.appData.totalDurationMs / metrics.ssr.appData.runs) * 100) / 100
        : 0;
      const glowAverageMs = metrics.glowTiles.requests > 0
        ? Math.round((metrics.glowTiles.totalDurationMs / metrics.glowTiles.requests) * 100) / 100
        : 0;
      const metricsSummary = {
        maptiler: {
          requests: metrics.maptiler.requests,
          errors: metrics.maptiler.errors,
          averageDurationMs,
          maxDurationMs: metrics.maptiler.maxDurationMs,
          slowCount: metrics.maptiler.slowCount,
          modes: metrics.maptiler.modes,
          fallbacks: metrics.maptiler.fallbacks,
          lastStatus: metrics.maptiler.lastStatus,
          lastDurationMs: metrics.maptiler.lastDurationMs,
          lastErrorMessage: metrics.maptiler.lastErrorMessage,
        },
        pmtilesWarmup: {
          tilesFetched: metrics.pmtiles.warmup.tilesFetched,
          tilesFailed: metrics.pmtiles.warmup.tilesFailed,
          originTiles: metrics.pmtiles.warmup.originTiles,
          cdnTiles: metrics.pmtiles.warmup.cdnTiles,
          lastRunStartedAt: metrics.pmtiles.warmup.lastRunStartedAt,
          lastRunDurationMs: metrics.pmtiles.warmup.lastRunDurationMs,
          lastRunTilesFetched: metrics.pmtiles.warmup.lastRunTilesFetched,
          lastRunTilesFailed: metrics.pmtiles.warmup.lastRunTilesFailed,
          lastRunOriginTiles: metrics.pmtiles.warmup.lastRunOriginTiles,
          lastRunCdnTiles: metrics.pmtiles.warmup.lastRunCdnTiles,
          lastErrorMessage: metrics.pmtiles.warmup.lastErrorMessage,
        },
        glowTiles: {
          requests: metrics.glowTiles.requests,
          hits: metrics.glowTiles.hits,
          misses: metrics.glowTiles.misses,
          errors: metrics.glowTiles.errors,
          averageDurationMs: glowAverageMs,
          maxDurationMs: metrics.glowTiles.maxDurationMs,
          lastErrorMessage: metrics.glowTiles.lastErrorMessage,
        },
        ssr: {
          requests: metrics.ssr.requests,
          cacheHits: metrics.ssr.cacheHits,
          cacheMisses: metrics.ssr.cacheMisses,
          averageDurationMs: ssrAverageMs,
          maxDurationMs: metrics.ssr.maxDurationMs,
          lastDurationMs: metrics.ssr.lastDurationMs,
          appData: {
            runs: metrics.ssr.appData.runs,
            averageDurationMs: appDataAverageMs,
            maxDurationMs: metrics.ssr.appData.maxDurationMs,
            lastDurationMs: metrics.ssr.appData.lastDurationMs,
          },
        },
      };
      res.status(healthy ? 200 : 503).json({
        status: healthy ? 'ok' : 'degraded',
        timestamp: new Date().toISOString(),
        checks,
        metrics: metricsSummary,
      });
      return;
    }

    const payload = {
      status: startupState.listening ? 'ok' : 'starting',
      timestamp: new Date().toISOString(),
      startedAt: new Date(startupState.startedAt).toISOString(),
      readyAt: startupState.readyAt ? new Date(startupState.readyAt).toISOString() : null,
      uptimeSeconds: Math.floor((Date.now() - startupState.startedAt) / 1000),
      dependencies: startupState.dependencies,
      merge: startupState.merge,
      warmup: startupState.warmup,
    };
    res.json(payload);
  });

  app.get(MAPTILER_PROXY_ROUTE, async (req, res) => {
    const requestStart = performance.now();
    const proxyModeForMetric = shouldUseMaptilerProxy() ? 'proxy' : 'direct';
    let fallbackAttempted = false;
    let recordedErrorMessage = null;

    res.once('finish', () => {
      recordMaptilerMetric({
        durationMs: performance.now() - requestStart,
        success: res.statusCode < 500,
        statusCode: res.statusCode,
        mode: proxyModeForMetric,
        usedFallback: fallbackAttempted,
        errorMessage: recordedErrorMessage,
      });
    });

    const key = process.env.MAPLIBRE_API_KEY || process.env.MAPTILER_API_KEY || '';
    if (!key) {
      res.status(503).json({ error: 'Map base unavailable' });
      return;
    }

    const rawResourcePath = req.params.path || '';
    const resourcePath = normaliseTileResourcePath(rawResourcePath);
    try {
      const upstreamUrl = new URL(`https://api.maptiler.com/${resourcePath}`);
      for (const [name, value] of Object.entries(req.query)) {
        if (Array.isArray(value)) {
          for (const entry of value) {
            upstreamUrl.searchParams.append(name, entry);
          }
        } else if (value !== undefined) {
          upstreamUrl.searchParams.append(name, value);
        }
      }
      upstreamUrl.searchParams.set('key', key);

      const descriptorSearch = new URLSearchParams(upstreamUrl.searchParams);
      descriptorSearch.delete('key');
      const descriptor = buildMaptilerDescriptor(resourcePath, descriptorSearch);

      const forwardedProto = req.get('x-forwarded-proto');
      const forwardedHost = req.get('x-forwarded-host');
      const hostHeader = forwardedHost || req.get('host');
      const originHint = hostHeader
        ? `${forwardedProto || req.protocol || 'https'}://${hostHeader}`
        : null;

      const proxyHeaders = {
        Accept: req.headers.accept || '*/*',
        'Accept-Encoding': req.headers['accept-encoding'] || 'gzip, br',
        'Accept-Language': req.headers['accept-language'] || 'en',
        'User-Agent': req.headers['user-agent']
          || process.env.MAPTILER_USER_AGENT
          || 'toronto-parking-proxy/1.0',
      };
      if (originHint) {
        proxyHeaders.Origin = originHint;
        proxyHeaders.Referer = originHint;
      }

      let payload;
      try {
        payload = await resolveMaptilerResource(descriptor, async () => {
          const response = await fetchMaptilerWithRetry(upstreamUrl, proxyHeaders);
          if (!response.ok) {
            const bodyText = await response.text().catch(() => '');
            throw new MaptilerHttpError(response.status, bodyText);
          }
          return formatMaptilerResponse(response, originHint);
        });
      } catch (error) {
        recordedErrorMessage = error?.message || String(error);
        if (error instanceof MaptilerHttpError && error.status >= 400 && error.status < 500 && error.status !== 429) {
          res.status(error.status).send(error.body || '');
          return;
        }

        console.warn(`MapTiler proxy primary fetch failed for ${resourcePath}:`, error.message);
        fallbackAttempted = true;
        let fallbackPayload = null;
        try {
          const fallbackResponse = await fetchMaptilerDirect(
            upstreamUrl,
            proxyHeaders,
            originHint,
            MAPTILER_FALLBACK_TIMEOUT_MS,
          );
          fallbackPayload = {
            ...fallbackResponse,
            cacheControl: descriptor.downstreamCacheControl,
            fromCache: false,
          };
          if (maptilerProxyState.proxyEnabled) {
            console.warn(`[maptiler] proxy fallback succeeded for ${resourcePath}; disabling proxy mode.`);
            maptilerProxyState.proxyEnabled = false;
            invalidateStyleCache();
          }
          recordedErrorMessage = null;
        } catch (fallbackError) {
          recordedErrorMessage = fallbackError?.message || fallbackError?.toString() || recordedErrorMessage;
          const status = fallbackError instanceof MaptilerHttpError && Number.isInteger(fallbackError.status)
            ? fallbackError.status
            : 502;
          const body = fallbackError instanceof MaptilerHttpError ? fallbackError.body : '';
          const reason = isAbortError(error) ? 'aborted' : fallbackError.message;
          console.error(`MapTiler fallback fetch failed for ${resourcePath}:`, reason);
          if (maptilerProxyState.proxyEnabled) {
            maptilerProxyState.proxyEnabled = false;
            invalidateStyleCache();
          }
          if (body) {
            res.status(status).send(body);
          } else {
            res.status(status).json({ error: 'Failed to proxy map resource' });
          }
          return;
        }

        payload = fallbackPayload;
      }

      res.status(payload.status);
      res.setHeader('Cache-Control', payload.cacheControl);
      res.setHeader('Vary', 'Accept-Encoding');
      res.setHeader('X-MapTiler-Cache', payload.fromCache ? 'HIT' : 'MISS');
      for (const [header, value] of Object.entries(payload.headers || {})) {
        if (!value) {
          continue;
        }
        if (header === 'location' || header === 'content-location') {
          const sanitizedLocation = sanitizeMaptilerUrl(value);
          res.setHeader(header, sanitizedLocation || value);
        } else {
          res.setHeader(header, value);
        }
      }
      if (!res.getHeader('Content-Type')) {
        res.setHeader('Content-Type', 'application/octet-stream');
      }
      if (payload.body && payload.body.length > 0) {
        res.setHeader('Content-Length', String(payload.body.length));
        res.end(payload.body);
      } else {
        res.end();
      }
    } catch (error) {
      console.error('MapTiler proxy failure:', error.message);
      res.status(502).json({ error: 'Failed to proxy map resource' });
    }
  });

  app.get('/styles/basic-style.json', async (req, res) => {
    try {
      const style = await loadBaseStyle();
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Cache-Control', 'no-store');
      res.send(style);
    } catch (error) {
      console.error('Failed to load base style', error);
      res.status(500).json({ error: 'Failed to load base map style' });
    }
  });

  app.get('/tiles/:dataset.json', (req, res) => {
    const dataset = (req.params.dataset || '').trim();
    const datasetConfig = POSTGIS_DATASET_CONFIG[dataset];
    if (!datasetConfig) {
      res.status(404).json({ error: 'Unknown dataset' });
      return;
    }

    const vectorLayerId = datasetConfig.layer || dataset;
    const minZoom = Number.isFinite(datasetConfig.minZoom) ? datasetConfig.minZoom : 0;
    const maxZoom = Number.isFinite(datasetConfig.maxZoom) ? datasetConfig.maxZoom : 16;
    const baseUrl = `${req.protocol}://${req.get('host')}`;
    const tilePath = `/tiles/{z}/{x}/{y}.pbf?dataset=${encodeURIComponent(dataset)}`;
    const tilesUrl = `${baseUrl}${tilePath}`;

    const payload = {
      tilejson: '2.2.0',
      name: dataset,
      version: '1.0.0',
      scheme: 'xyz',
      attribution: 'City of Toronto open data',
      minzoom: minZoom,
      maxzoom: maxZoom,
      tiles: [tilesUrl],
      vector_layers: [
        {
          id: vectorLayerId,
          description: dataset,
        },
      ],
    };

    res.setHeader('Cache-Control', TILE_JSON_CACHE_CONTROL);
    res.json(payload);
  });

  app.get('/tiles/glow/:dataset/:z/:x/:y.mvt', async (req, res) => {
    const datasetParam = typeof req.params.dataset === 'string' ? req.params.dataset.trim() : '';
    const dataset = datasetParam || 'parking_tickets';
    const z = Number.parseInt(req.params.z, 10);
    const x = Number.parseInt(req.params.x, 10);
    const y = Number.parseInt(req.params.y, 10);

    glowTileService.setup();

    const startedAt = performance.now();
    const abortController = new AbortController();
    const timeout = setTimeout(
      () => abortController.abort(new Error('Glow tile timeout exceeded')),
      GLOW_TILE_TIMEOUT_MS,
    );

    const handleClose = () => {
      abortController.abort(new Error('Request closed by client'));
    };
    req.on('close', handleClose);

    metrics.glowTiles.requests += 1;

    if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y)) {
      res.status(400).json({ error: 'Invalid tile coordinates' });
      metrics.glowTiles.errors += 1;
      metrics.glowTiles.lastErrorMessage = 'invalid-coordinates';
      clearTimeout(timeout);
      req.off('close', handleClose);
      return;
    }

    if (!GLOW_ALLOWED_DATASETS.has(dataset)) {
      res.status(404).json({ error: 'Unknown glow dataset' });
      metrics.glowTiles.errors += 1;
      metrics.glowTiles.lastErrorMessage = 'unknown-dataset';
      clearTimeout(timeout);
      req.off('close', handleClose);
      return;
    }

    try {
      const result = await glowTileService.getTile({
        dataset,
        z,
        x,
        y,
        signal: abortController.signal,
      });

      const durationMs = performance.now() - startedAt;
      metrics.glowTiles.totalDurationMs += durationMs;
      metrics.glowTiles.maxDurationMs = Math.max(metrics.glowTiles.maxDurationMs, durationMs);
      if (result.cacheStatus === 'hit') {
        metrics.glowTiles.hits += 1;
      } else {
        metrics.glowTiles.misses += 1;
      }

      if (durationMs > GLOW_TILE_SLOW_THRESHOLD_MS) {
        console.warn(
          `[glow-tiles] slow response dataset=${dataset} z=${z} x=${x} y=${y} cache=${result.cacheStatus} duration=${durationMs.toFixed(1)}ms size=${result.size}`,
        );
      }

      res.setHeader('Content-Type', 'application/x-protobuf');
      res.setHeader('Content-Encoding', 'gzip');
      res.setHeader('Cache-Control', GLOW_TILE_CACHE_CONTROL);
      res.setHeader('Vary', 'Accept-Encoding');
      res.setHeader('X-GlowTile-Cache', result.cacheStatus.toUpperCase());
      res.setHeader('X-GlowTile-Duration', durationMs.toFixed(1));
      res.setHeader('Content-Length', String(result.buffer.length));
      res.status(200).end(result.buffer);
    } catch (error) {
      const durationMs = performance.now() - startedAt;
      metrics.glowTiles.errors += 1;
      metrics.glowTiles.lastErrorMessage = error?.message || String(error);
      console.warn(
        `[glow-tiles] failed dataset=${dataset} z=${z} x=${x} y=${y}:`,
        error?.message || error,
      );
      if (error?.name === 'AbortError') {
        res.status(499).json({ error: 'Glow tile request aborted' });
      } else if (typeof error?.message === 'string' && error.message.includes('Unsupported glow dataset')) {
        res.status(404).json({ error: 'Unknown glow dataset' });
      } else {
        res.status(500).json({ error: 'Failed to render glow tile' });
      }
      metrics.glowTiles.totalDurationMs += durationMs;
      metrics.glowTiles.maxDurationMs = Math.max(metrics.glowTiles.maxDurationMs, durationMs);
    } finally {
      clearTimeout(timeout);
      req.off('close', handleClose);
    }
  });

  app.get('/tiles/:z/:x/:y.pbf', async (req, res) => {
    console.log('[tiles] request', req.url, req.headers.accept);
    const z = Number.parseInt(req.params.z, 10);
    const x = Number.parseInt(req.params.x, 10);
    const y = Number.parseInt(req.params.y, 10);
    const dataset = typeof req.query.dataset === 'string' && req.query.dataset.trim().length > 0
      ? req.query.dataset.trim()
      : 'parking_tickets';
    const datasetConfig = POSTGIS_DATASET_CONFIG[dataset] || null;

    const startTime = performance.now();
    let tileDurationMs = 0;
    let encodeDurationMs = 0;
    let tileSource = 'none';
    let headersApplied = false;
    let slowLogged = false;
    const preferPostgis = postgisTileService.isDatasetEnabled(dataset);
    const legacyRendererAllowed = ENABLE_LEGACY_TILE_RENDERER || !preferPostgis;

    const applyHeadersIfNeeded = () => {
      const totalMs = performance.now() - startTime;
      if (!headersApplied && !res.headersSent) {
        const timings = [`total;dur=${totalMs.toFixed(1)}`];
        if (tileDurationMs > 0) {
          timings.push(`tile;dur=${tileDurationMs.toFixed(1)}`);
        }
        if (encodeDurationMs > 0) {
          timings.push(`encode;dur=${encodeDurationMs.toFixed(1)}`);
        }
        res.setHeader('Server-Timing', timings.join(', '));
        const metrics = getTileMetrics();
        res.setHeader('X-Tiles-Active-Renders', String(metrics.activeRenders));
        res.setHeader('X-Tiles-Cold-Miss-P95', metrics.p95ColdRenderMs.toFixed(1));
        res.setHeader('X-Tiles-Cold-Miss-Count', String(metrics.totalColdMisses));
        res.setHeader('X-Tiles-Source', tileSource);
        headersApplied = true;
      }
      if (!slowLogged && totalMs > TILE_SLOW_LOG_THRESHOLD_MS) {
        console.warn(`[tiles] slow response dataset=${dataset} z=${z} x=${x} y=${y} source=${tileSource} (${totalMs.toFixed(1)}ms)`);
        slowLogged = true;
      }
    };

    if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y)) {
      tileSource = 'invalid-coords';
      applyHeadersIfNeeded();
      res.status(400).json({ error: 'Invalid tile coordinates' });
      return;
    }

    const minZoom = datasetConfig?.minZoom ?? (dataset === 'parking_tickets' ? TICKET_TILE_MIN_ZOOM : null);
    if (Number.isFinite(minZoom) && z < minZoom) {
      tileSource = 'zoom-restricted';
      applyHeadersIfNeeded();
      res.status(204).end();
      return;
    }

    if (!preferPostgis && !legacyRendererAllowed) {
      tileSource = 'disabled';
      applyHeadersIfNeeded();
      res.status(503).json({ error: 'Tile renderer unavailable' });
      return;
    }

    if (!preferPostgis && dataset !== 'parking_tickets') {
      tileSource = 'dataset-disabled';
      res.setHeader('Cache-Control', TILE_HTTP_CACHE_CONTROL);
      applyHeadersIfNeeded();
      res.status(204).end();
      return;
    }

    const hardTimeoutMs = Number.isFinite(TILE_HARD_TIMEOUT_MS) && TILE_HARD_TIMEOUT_MS > 0
      ? TILE_HARD_TIMEOUT_MS
      : 450;
    const controller = new AbortController();
    const timer = setTimeout(
      () => controller.abort(new Error('Tile render budget exceeded')),
      hardTimeoutMs,
    );

    const respondWithEmptyTile = (statusCode = 200, sourceTag = 'empty') => {
      tileSource = sourceTag;
      const acceptEncoding = req.headers['accept-encoding'] || '';
      const encodeStart = performance.now();
      const { buffer: encoded, encoding } = encodeTileBuffer(EMPTY_TILE_BUFFER, acceptEncoding);
      encodeDurationMs += performance.now() - encodeStart;
      res.setHeader('Content-Type', 'application/vnd.mapbox-vector-tile');
      res.setHeader('Cache-Control', TILE_HTTP_CACHE_CONTROL);
      res.setHeader('Vary', 'Accept-Encoding');
      if (encoding) {
        res.setHeader('Content-Encoding', encoding);
      }
      res.setHeader('ETag', 'W/"tile-empty"');
      res.setHeader('X-Tile-Fallback', 'empty');
      if (!res.headersSent) {
        res.status(statusCode);
      }
      applyHeadersIfNeeded();
      res.end(encoded);
    };

    try {
      let tile = null;
      let fetchError = null;

      if (preferPostgis) {
        const pgStart = performance.now();
        try {
          tile = await postgisTileService.getTile(dataset, z, x, y, {
            signal: controller.signal,
          });
        } catch (error) {
          if (error?.name === 'AbortError') {
            throw error;
          }
          fetchError = error;
        } finally {
          tileDurationMs += performance.now() - pgStart;
        }
        tileSource = tile?.source || 'postgis';
      }

      if (!tile && legacyRendererAllowed && dataset === 'parking_tickets') {
        const legacyStart = performance.now();
        try {
          tile = await tileService.getTile(z, x, y, {
            signal: controller.signal,
            allowStale: true,
            revalidate: true,
          });
          if (tile && !tileSource) {
            tileSource = 'legacy';
          }
        } finally {
          tileDurationMs += performance.now() - legacyStart;
        }
      } else if (!tile && legacyRendererAllowed) {
        tileSource = 'dataset-disabled';
      } else if (!tile && fetchError) {
        console.error('PostGIS tile fetch failed, no legacy fallback available:', fetchError);
      }

      tileSource = tile?.source || tileSource || (preferPostgis ? 'postgis' : 'legacy');

      if (!tile || !tile.buffer) {
        res.status(204);
        applyHeadersIfNeeded();
        res.end();
        return;
      }

      const etag = tile.etag || (tile.version !== null && tile.version !== undefined
        ? `W/"tickets:${tile.version}"`
        : null);
      if (etag && req.headers['if-none-match'] === etag) {
        tileSource = 'not-modified';
        applyHeadersIfNeeded();
        res.status(304).end();
        return;
      }

      const acceptEncoding = req.headers['accept-encoding'] || '';
      const encodeStart = performance.now();
      const { buffer: encoded, encoding } = encodeTileBuffer(tile.buffer, acceptEncoding);
      encodeDurationMs += performance.now() - encodeStart;

      const cacheSeconds = Number.isFinite(tile.cacheSeconds)
        ? Math.max(60, tile.cacheSeconds)
        : tileCacheRuntime.baseTtlSeconds;
      const staleRevalidateSeconds = 600;

      res.setHeader('Content-Type', 'application/vnd.mapbox-vector-tile');
      res.setHeader('Cache-Control', `public, max-age=${cacheSeconds}, stale-while-revalidate=${staleRevalidateSeconds}`);
      res.setHeader('X-Tile-Cache-Seconds', String(cacheSeconds));
      if (tile.mode) {
        res.setHeader('X-Tile-Mode', tile.mode);
      }
      if (Number.isFinite(tile.featureCount)) {
        res.setHeader('X-Tile-Features', String(tile.featureCount));
      }
      if (tile.partial) {
        res.setHeader('X-Tile-Partial', '1');
      }
      if (Number.isFinite(tile.gridMeters)) {
        res.setHeader('X-Tile-Grid-Meters', String(tile.gridMeters));
      }
      if (Number.isFinite(tile.sampleLimit)) {
        res.setHeader('X-Tile-Sample-Limit', String(tile.sampleLimit));
      }
      if (Number.isFinite(tile.pointLimit)) {
        res.setHeader('X-Tile-Point-Limit', String(tile.pointLimit));
      }
      res.setHeader('Vary', 'Accept-Encoding');
      if (encoding) {
        res.setHeader('Content-Encoding', encoding);
      }
      if (etag) {
        res.setHeader('ETag', etag);
      }
      if (Number.isFinite(tile.queryDurationMs)) {
        res.setHeader('X-Tile-Query-Ms', tile.queryDurationMs.toFixed(2));
      }
      if (tile.lastModified) {
        res.setHeader('Last-Modified', new Date(tile.lastModified).toUTCString());
      } else {
        const tileTimestamp = extractTimestamp(tile.version);
        if (tileTimestamp) {
          res.setHeader('Last-Modified', new Date(tileTimestamp).toUTCString());
        }
      }
      applyHeadersIfNeeded();
      res.end(encoded);
    } catch (error) {
      if (error?.name === 'AbortError') {
        console.warn(`Tile request exceeded hard timeout for ${z}/${x}/${y}`);
        respondWithEmptyTile(200, 'timeout');
        return;
      }
      console.error('Failed to serve vector tile', error);
      respondWithEmptyTile(200, 'error');
    } finally {
      clearTimeout(timer);
      applyHeadersIfNeeded();
    }
  });

  app.get('/metrics/tiles', (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.json({
      ...getTileMetrics(),
      timestamp: Date.now(),
    });
  });

  app.get('/api/app-data', async (req, res) => {
    try {
      const snapshot = await createAppData();
      const meta = getLatestAppDataMeta();
      if (meta) {
        res.setHeader('X-App-Data-Source', meta.fromCache ? 'cache' : 'refreshed');
        if (meta.refreshedAt) {
          res.setHeader('X-App-Data-Refreshed-At', meta.refreshedAt);
        }
      }
      res.setHeader('Cache-Control', 'public, max-age=900, stale-while-revalidate=60');
      res.json(snapshot);
    } catch (error) {
      console.error('Failed to load application data snapshot', error);
      res.status(500).json({ error: 'Failed to load app data' });
    }
  });

  app.get('/api/dataset-totals', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' ? req.query.dataset : 'parking_tickets';
    try {
      const totals = await getDatasetTotals(dataset);
      if (!totals) {
        res.status(503).json({ error: 'Dataset unavailable', dataset });
        return;
      }
      const featureCount = Number(totals.featureCount) || 0;
      const ticketCount = Number(totals.ticketCount ?? featureCount) || 0;
      const revenueValue = Number(totals.totalRevenue ?? 0);
      const totalRevenue = Number.isFinite(revenueValue)
        ? Number(revenueValue.toFixed(2))
        : 0;
      const payload = {
        dataset: totals.dataset || dataset,
        featureCount,
        ticketCount,
        totalRevenue,
        source: totals.source || 'postgres',
      };
      res.setHeader('Cache-Control', 'public, max-age=300, stale-while-revalidate=60');
      res.json(payload);
    } catch (error) {
      console.error('Failed to compute dataset totals', error);
      res.status(500).json({ error: 'Failed to compute dataset totals' });
    }
  });

  app.get('/tiles/wards/:dataset/:z/:x/:y.pbf', async (req, res) => {
    const { dataset } = req.params;
    const z = Number.parseInt(req.params.z, 10);
    const x = Number.parseInt(req.params.x, 10);
    const y = Number.parseInt(req.params.y, 10);

    if (!WARD_DATASETS.has(dataset)) {
      res.status(400).json({ error: 'Invalid ward dataset' });
      return;
    }
    if (![z, x, y].every(Number.isInteger)) {
      res.status(400).json({ error: 'Invalid tile coordinates' });
      return;
    }

    try {
      const tile = await getWardTile(dataset, z, x, y);
      const etag = tile?.version || null;
      if (etag && req.headers['if-none-match'] === etag) {
        res.status(304).end();
        return;
      }
      if (!tile || !tile.buffer) {
        res.status(204).end();
        return;
      }
      const acceptEncoding = req.headers['accept-encoding'] || '';
      const { buffer: encoded, encoding } = encodeTileBuffer(tile.buffer, acceptEncoding);
      res.setHeader('Content-Type', 'application/vnd.mapbox-vector-tile');
      res.setHeader('Cache-Control', TILE_HTTP_CACHE_CONTROL);
      res.setHeader('Vary', 'Accept-Encoding');
      if (encoding) {
        res.setHeader('Content-Encoding', encoding);
      }
      if (etag) {
        res.setHeader('ETag', etag);
        const wardTimestamp = extractTimestamp(etag);
        if (wardTimestamp) {
          res.setHeader('Last-Modified', new Date(wardTimestamp).toUTCString());
        }
      }
      res.end(encoded);
    } catch (error) {
      console.error('Failed to serve ward tile', error);
      res.status(500).json({ error: 'Failed to load ward tile' });
    }
  });

  app.get('/api/yearly/years', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' ? req.query.dataset : 'parking_tickets';
    try {
      const years = await getDatasetYears(dataset);
      res.setHeader('Cache-Control', 'public, max-age=600, stale-while-revalidate=120');
      res.json({ dataset, years });
    } catch (error) {
      console.error('Failed to load yearly metadata', error);
      res.status(500).json({ error: 'Failed to load yearly metadata' });
    }
  });

  app.get('/api/yearly/totals', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' ? req.query.dataset : 'parking_tickets';
    const year = Number.parseInt(req.query.year, 10);
    const yearValue = Number.isFinite(year) ? year : null;
    try {
      if (dataset === 'parking_tickets') {
        const totals = await getParkingTotals(yearValue);
        res.json({
          dataset,
          year: yearValue,
          ticketCount: totals.ticketCount,
          totalRevenue: Number(totals.totalRevenue.toFixed(2)),
          locationCount: totals.locationCount,
        });
        return;
      }
      const totals = await getCameraTotals(dataset, yearValue);
      res.json({
        dataset,
        year: yearValue,
        ticketCount: totals.ticketCount,
        totalRevenue: Number(totals.totalRevenue.toFixed(2)),
        locationCount: totals.locationCount,
      });
    } catch (error) {
      console.error('Failed to compute yearly totals', error);
      res.status(500).json({ error: 'Failed to compute yearly totals' });
    }
  });

  app.get('/api/yearly/top-streets', async (req, res) => {
    const year = Number.parseInt(req.query.year, 10);
    const limit = Number.isFinite(Number(req.query.limit)) ? Number(req.query.limit) : 10;
    if (req.query.dataset && req.query.dataset !== 'parking_tickets') {
      res.status(400).json({ error: 'Dataset must be parking_tickets for top streets' });
      return;
    }
    try {
      const streets = await getParkingTopStreets(Number.isFinite(year) ? year : null, limit);
      res.json({ dataset: 'parking_tickets', year: Number.isFinite(year) ? year : null, items: streets });
    } catch (error) {
      console.error('Failed to load yearly street rankings', error);
      res.status(500).json({ error: 'Failed to load yearly street rankings' });
    }
  });

  app.get('/api/yearly/top-neighbourhoods', async (req, res) => {
    const year = Number.parseInt(req.query.year, 10);
    const limit = Number.isFinite(Number(req.query.limit)) ? Number(req.query.limit) : 10;
    if (req.query.dataset && req.query.dataset !== 'parking_tickets') {
      res.status(400).json({ error: 'Dataset must be parking_tickets for neighbourhood rankings' });
      return;
    }
    try {
      const neighbourhoods = await getParkingTopNeighbourhoods(Number.isFinite(year) ? year : null, limit);
      res.json({ dataset: 'parking_tickets', year: Number.isFinite(year) ? year : null, items: neighbourhoods });
    } catch (error) {
      console.error('Failed to load yearly neighbourhood rankings', error);
      res.status(500).json({ error: 'Failed to load yearly neighbourhood rankings' });
    }
  });

  app.get('/api/yearly/top-locations', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' ? req.query.dataset : 'red_light_locations';
    const year = Number.parseInt(req.query.year, 10);
    const limit = Number.isFinite(Number(req.query.limit)) ? Number(req.query.limit) : 10;
    try {
      const items = await getCameraTopLocations(dataset, Number.isFinite(year) ? year : null, limit);
      res.json({ dataset, year: Number.isFinite(year) ? year : null, items });
    } catch (error) {
      console.error('Failed to load yearly camera rankings', error);
      res.status(500).json({ error: 'Failed to load yearly camera rankings' });
    }
  });

  app.get('/api/yearly/top-groups', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' ? req.query.dataset : 'red_light_locations';
    const year = Number.parseInt(req.query.year, 10);
    const limit = Number.isFinite(Number(req.query.limit)) ? Number(req.query.limit) : 10;
    try {
      const items = await getCameraTopGroups(dataset, Number.isFinite(year) ? year : null, limit);
      res.json({ dataset, year: Number.isFinite(year) ? year : null, items });
    } catch (error) {
      console.error('Failed to load yearly group rankings', error);
      res.status(500).json({ error: 'Failed to load yearly group rankings' });
    }
  });

  app.get('/api/yearly/location', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' ? req.query.dataset : 'parking_tickets';
    const year = Number.parseInt(req.query.year, 10);
    const yearValue = Number.isFinite(year) ? year : null;
    try {
      if (dataset === 'parking_tickets') {
        const location = typeof req.query.location === 'string' ? req.query.location : null;
        if (!location) {
          res.status(400).json({ error: 'location parameter is required for parking dataset' });
          return;
        }
        const detail = await getParkingLocationDetail(location.toUpperCase(), yearValue);
        if (!detail) {
          res.status(404).json({ error: 'Location not found' });
          return;
        }
        res.json({ dataset, year: yearValue, detail });
        return;
      }

      const code = typeof req.query.location === 'string' ? req.query.location : null;
      if (!code) {
        res.status(400).json({ error: 'location parameter is required' });
        return;
      }
      const detail = await getCameraLocationDetail(dataset, code, yearValue);
      if (!detail) {
        res.status(404).json({ error: 'Location not found' });
        return;
      }
      res.json({ dataset, year: yearValue, detail });
    } catch (error) {
      console.error('Failed to load location detail', error);
      res.status(500).json({ error: 'Failed to load location detail' });
    }
  });

  app.get('/api/wards/summary', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' ? req.query.dataset : 'red_light_locations';
    if (!WARD_DATASETS.has(dataset)) {
      res.status(400).json({ error: 'Dataset must be red_light_locations, ase_locations, or cameras_combined' });
      return;
    }
    try {
      const summary = await loadCameraWardSummary(dataset);
      if (!summary) {
        res.status(503).json({ error: 'Ward summary unavailable' });
        return;
      }
      const etag = summary.etag || (summary.version ? `W/"${summary.version}"` : null);
      if (etag && req.headers['if-none-match'] === etag) {
        res.status(304).end();
        return;
      }
      res.setHeader('Cache-Control', 'public, max-age=600, stale-while-revalidate=120');
      if (etag) {
        res.setHeader('ETag', etag);
      }
      res.json(summary.data);
    } catch (error) {
      console.error('Failed to load ward summary', error);
      res.status(500).json({ error: 'Failed to load ward summary' });
    }
  });

  app.get('/api/wards/geojson', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' ? req.query.dataset : 'red_light_locations';
    if (!WARD_DATASETS.has(dataset)) {
      res.status(400).json({ error: 'Dataset must be red_light_locations, ase_locations, or cameras_combined' });
      return;
    }
    try {
      const geojson = await loadCameraWardGeojson(dataset);
      if (!geojson) {
        res.status(503).json({ error: 'Ward geojson unavailable' });
        return;
      }
      const etag = geojson.etag || (geojson.version ? `W/"${geojson.version}"` : null);
      if (etag && req.headers['if-none-match'] === etag) {
        res.status(304).end();
        return;
      }
      res.setHeader('Cache-Control', 'public, max-age=600, stale-while-revalidate=120');
      if (etag) {
        res.setHeader('ETag', etag);
      }
      let payload;
      if (typeof geojson.raw === 'string') {
        payload = geojson.raw;
      } else if (typeof geojson.data === 'string') {
        payload = geojson.data;
      } else if (geojson.data) {
        payload = JSON.stringify(geojson.data);
      } else {
        payload = '{}';
      }
      res.type('application/json').send(payload);
    } catch (error) {
      console.error('Failed to load ward geojson', error);
      res.status(500).json({ error: 'Failed to load ward geojson' });
    }
  });

  app.post('/api/wards/prewarm', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' ? req.query.dataset : null;
    if (!dataset || !WARD_DATASETS.has(dataset)) {
      res.status(400).json({ error: 'Dataset must be red_light_locations, ase_locations, or cameras_combined' });
      return;
    }
    try {
      await prewarmWardTiles(dataset);
      res.status(204).end();
    } catch (error) {
      console.error('Failed to prewarm ward tiles', error);
      res.status(500).json({ error: 'Failed to prewarm ward tiles' });
    }
  });

  app.get('/api/map-summary', async (req, res) => {
    const dataset = typeof req.query.dataset === 'string' && req.query.dataset.trim().length > 0
      ? req.query.dataset.trim()
      : 'parking_tickets';
    if (dataset !== 'parking_tickets') {
      res.status(204).end();
      return;
    }
    const west = Number(req.query.west);
    const south = Number(req.query.south);
    const east = Number(req.query.east);
    const north = Number(req.query.north);
    const zoom = Number(req.query.zoom);

    if (![west, south, east, north, zoom].every(Number.isFinite)) {
      res.status(400).json({ error: 'Invalid bounds or zoom' });
      return;
    }

    const filters = {};
    if (req.query.year !== undefined) {
      const year = Number(req.query.year);
      if (Number.isFinite(year)) {
        filters.year = year;
      }
    }
    if (req.query.month !== undefined) {
      const month = Number(req.query.month);
      if (Number.isFinite(month)) {
        filters.month = month;
      }
    }

    try {
      const summary = await tileService.summarizeViewport({ west, south, east, north, zoom, filters });
      res.setHeader('Cache-Control', 'no-store');
      res.json(summary);
    } catch (error) {
      console.error('Failed to compute viewport summary', error);
      res.status(500).json({ error: 'Failed to compute summary' });
    }
  });

  app.get('/api/cluster-expansion', async (req, res) => {
    const clusterId = Number(req.query.clusterId);
    if (!Number.isFinite(clusterId)) {
      res.status(400).json({ error: 'clusterId is required' });
      return;
    }

    try {
      const zoom = await tileService.getClusterExpansionZoom(clusterId);
      if (zoom === null) {
        res.status(404).json({ error: 'Cluster not found' });
        return;
      }
      res.setHeader('Cache-Control', 'public, max-age=60');
      res.json({ zoom });
    } catch (error) {
      console.error('Failed to compute cluster expansion zoom', error);
      res.status(500).json({ error: 'Failed to compute expansion zoom' });
    }
  });

  app.get('/api/heatmap-points', async (req, res) => {
    const west = Number(req.query.west);
    const south = Number(req.query.south);
    const east = Number(req.query.east);
    const north = Number(req.query.north);

    if (![west, south, east, north].every(Number.isFinite)) {
      res.status(400).json({ error: 'Invalid bounds' });
      return;
    }

    const limit = req.query.limit !== undefined ? Number(req.query.limit) : undefined;
    const filters = {};
    if (req.query.year !== undefined) {
      const year = Number(req.query.year);
      if (Number.isFinite(year)) {
        filters.year = year;
      }
    }
    if (req.query.month !== undefined) {
      const month = Number(req.query.month);
      if (Number.isFinite(month)) {
        filters.month = month;
      }
    }

    try {
      const points = await tileService.getViewportPoints({ west, south, east, north, limit, filters });
      const featureCollection = {
        type: 'FeatureCollection',
        features: points.map((point) => ({
          type: 'Feature',
          geometry: {
            type: 'Point',
            coordinates: [point.longitude, point.latitude],
          },
          properties: {
            count: point.count,
          },
        })),
      };
      res.setHeader('Cache-Control', 'no-store');
      res.json(featureCollection);
    } catch (error) {
      console.error('Failed to compute heatmap payload', error);
      res.status(500).json({ error: 'Failed to compute heatmap data' });
    }
  });

  app.post('/api/client-metrics', clientMetricsParser, (req, res) => {
    const payload = req.body;
    if (!payload || typeof payload !== 'object') {
      res.status(400).json({ error: 'Invalid metrics payload' });
      return;
    }
    metrics.client.lastSubmission = Date.now();
    metrics.client.payload = {
      navigationStart: Number(payload.navigationStart) || null,
      mapReadyAt: Number(payload.mapReadyAt) || null,
      ticketsPaintAt: Number(payload.ticketsPaintAt) || null,
      fpsSamples: Array.isArray(payload.fpsSamples) ? payload.fpsSamples.slice(0, 8) : [],
      panFps: Array.isArray(payload.panFps) ? payload.panFps.slice(0, 8) : [],
      tileRequests: Number.parseInt(payload.tileRequests, 10) || 0,
      tileWindow: {
        count: Number.parseInt(payload?.tileWindow?.count, 10) || 0,
        windowMs: Number.parseInt(payload?.tileWindow?.windowMs, 10) || 10_000,
      },
      tileCompleted: Number.parseInt(payload.tileCompleted, 10) || 0,
      tileAborted: Number.parseInt(payload.tileAborted, 10) || 0,
      tileAbortRatio: Number.isFinite(Number(payload.tileAbortRatio)) ? Number(payload.tileAbortRatio) : null,
      tileTtfb: Array.isArray(payload.tileTtfb) ? payload.tileTtfb.slice(0, 16) : [],
      firstContentfulPaint: Number(payload.firstContentfulPaint) || null,
      firstPaint: Number(payload.firstPaint) || null,
      firstInputDelay: Number(payload.firstInputDelay) || null,
      jsBytes: Number.parseInt(payload.jsBytes, 10) || 0,
      generatedAt: payload.generatedAt || new Date().toISOString(),
    };
    console.log('[client-metrics]', metrics.client.payload);
    res.status(204).end();
  });
}

function registerDataRoutes(app, dataDirectory) {
  const resolveDataPath = (fileName) => path.join(dataDirectory, fileName);

  app.get('/api/pmtiles-manifest', (req, res) => {
    if (!pmtilesManifest.enabled) {
      res.status(204).end();
      return;
    }
    res.json(pmtilesManifest);
  });

  app.get('/data/tickets_summary.json', async (req, res) => {
    try {
      const summary = await loadTicketsSummary();
      if (summary?.data) {
        res.setHeader('Cache-Control', 'public, max-age=900, stale-while-revalidate=120');
        res.json(summary.data);
        return;
      }
    } catch (error) {
      console.warn('Failed to load tickets summary for /data route:', error.message);
    }
    res.status(503).json({ error: 'Summary unavailable' });
  });

  app.get('/data/street_stats.json', async (req, res) => {
    try {
      const stats = await loadStreetStats();
      if (stats?.data) {
        res.setHeader('Cache-Control', 'public, max-age=900, stale-while-revalidate=120');
        res.json(stats.data);
        return;
      }
    } catch (error) {
      console.warn('Failed to load street stats for /data route:', error.message);
    }
    res.status(503).json({ error: 'Street stats unavailable' });
  });

  app.get('/data/red_light_summary.json', async (req, res) => {
    try {
      const summary = await loadDatasetSummary('red_light_locations');
      if (summary?.data) {
        res.setHeader('Cache-Control', 'public, max-age=900, stale-while-revalidate=120');
        res.json(summary.data);
        return;
      }
    } catch (error) {
      console.warn('Failed to load red light summary for /data route:', error.message);
    }
    res.status(503).json({ error: 'Red light summary unavailable' });
  });

  app.get('/data/ase_summary.json', async (req, res) => {
    try {
      const summary = await loadDatasetSummary('ase_locations');
      if (summary?.data) {
        res.setHeader('Cache-Control', 'public, max-age=900, stale-while-revalidate=120');
        res.json(summary.data);
        return;
      }
    } catch (error) {
      console.warn('Failed to load ASE summary for /data route:', error.message);
    }
    res.status(503).json({ error: 'ASE summary unavailable' });
  });

  app.get('/data/neighbourhood_stats.json', async (req, res) => {
    try {
      const stats = await loadNeighbourhoodStats();
      if (stats?.data) {
        res.setHeader('Cache-Control', 'public, max-age=900, stale-while-revalidate=120');
        res.json(stats.data);
        return;
      }
    } catch (error) {
      console.warn('Failed to load neighbourhood stats for /data route:', error.message);
    }
    res.status(503).json({ error: 'Neighbourhood stats unavailable' });
  });

  app.get('/data/red_light_glow_lines.geojson', async (req, res) => {
    try {
      const glow = await loadCameraGlow('red_light_locations');
      if (glow?.raw) {
        const etag = glow.etag || (Number.isFinite(glow.version) ? `W/"${glow.version}"` : null);
        if (etag && req.headers['if-none-match'] === etag) {
          res.status(304).end();
          return;
        }
        res.setHeader('Cache-Control', 'public, max-age=1200, stale-while-revalidate=300');
        if (etag) {
          res.setHeader('ETag', etag);
        }
        const acceptEncoding = req.headers['accept-encoding'] || '';
        if (/\bbr\b/.test(acceptEncoding)) {
          res.setHeader('Content-Encoding', 'br');
          res.type('application/json').send(brotliCompressSync(Buffer.from(glow.raw)));
        } else if (/\bgzip\b/.test(acceptEncoding)) {
          res.setHeader('Content-Encoding', 'gzip');
          res.type('application/json').send(gzipSync(Buffer.from(glow.raw)));
        } else {
          res.type('application/json').send(glow.raw);
        }
        return;
      }
    } catch (error) {
      console.warn('Failed to load red light glow lines from data store:', error.message);
    }
    res.status(404).json({ error: 'Red light glow dataset unavailable' });
  });

  app.get('/data/ase_glow_lines.geojson', async (req, res) => {
    try {
      const glow = await loadCameraGlow('ase_locations');
      if (glow?.raw) {
        const etag = glow.etag || (Number.isFinite(glow.version) ? `W/"${glow.version}"` : null);
        if (etag && req.headers['if-none-match'] === etag) {
          res.status(304).end();
          return;
        }
        res.setHeader('Cache-Control', 'public, max-age=1200, stale-while-revalidate=300');
        if (etag) {
          res.setHeader('ETag', etag);
        }
        const acceptEncoding = req.headers['accept-encoding'] || '';
        if (/\bbr\b/.test(acceptEncoding)) {
          res.setHeader('Content-Encoding', 'br');
          res.type('application/json').send(brotliCompressSync(Buffer.from(glow.raw)));
        } else if (/\bgzip\b/.test(acceptEncoding)) {
          res.setHeader('Content-Encoding', 'gzip');
          res.type('application/json').send(gzipSync(Buffer.from(glow.raw)));
        } else {
          res.type('application/json').send(glow.raw);
        }
        return;
      }
    } catch (error) {
      console.warn('Failed to load ASE glow lines from data store:', error.message);
    }
    res.status(404).json({ error: 'ASE glow dataset unavailable' });
  });

  app.get('/data/red_light_locations.geojson', async (req, res) => {
    try {
      const payload = await loadCameraLocations('red_light_locations');
      if (payload?.raw) {
        const etag = payload.etag || (Number.isFinite(payload.version) ? `W/"${payload.version}"` : null);
        if (etag && req.headers['if-none-match'] === etag) {
          res.status(304).end();
          return;
        }
        res.setHeader('Cache-Control', 'public, max-age=1200, stale-while-revalidate=300');
        if (etag) {
          res.setHeader('ETag', etag);
        }
        res.type('application/json').send(payload.raw);
        return;
      }
    } catch (error) {
      console.warn('Failed to load red light locations from data store:', error.message);
    }
    res.status(404).json({ error: 'Red light locations unavailable' });
  });

  app.get('/data/ase_locations.geojson', async (req, res) => {
    try {
      const payload = await loadCameraLocations('ase_locations');
      if (payload?.raw) {
        const etag = payload.etag || (Number.isFinite(payload.version) ? `W/"${payload.version}"` : null);
        if (etag && req.headers['if-none-match'] === etag) {
          res.status(304).end();
          return;
        }
        res.setHeader('Cache-Control', 'public, max-age=1200, stale-while-revalidate=300');
        if (etag) {
          res.setHeader('ETag', etag);
        }
        res.type('application/json').send(payload.raw);
        return;
      }
    } catch (error) {
      console.warn('Failed to load ASE locations from data store:', error.message);
    }
    res.status(404).json({ error: 'ASE locations unavailable' });
  });

  app.get('/data/tickets_glow_lines.geojson', async (req, res) => {
    const glowPath = resolveDataPath('tickets_glow_lines.geojson');
    try {
      const raw = await fs.readFile(glowPath, 'utf-8');
      res.setHeader('Cache-Control', 'public, max-age=900, stale-while-revalidate=120');
      res.type('application/json').send(raw);
    } catch (error) {
      console.warn('Failed to read tickets glow lines from disk:', error.message);
      res.status(404).json({ error: 'Glow dataset unavailable' });
    }
  });

  app.use('/data', express.static(dataDirectory));
}

function injectTemplate(template, appHtml, initialData, manifestPayload) {
  const safeData = JSON.stringify(initialData).replace(/</g, '\\u003c');
  const safeManifest = manifestPayload
    ? JSON.stringify(manifestPayload).replace(/</g, '\\u003c')
    : 'null';
  const manifestScript = `<script>window.__PMTILES_MANIFEST__ = ${safeManifest};</script>`;
  return template
    .replace('<!--app-html-->', appHtml)
    .replace(
      '<!--initial-data-->',
      `<script>window.__INITIAL_DATA__ = ${safeData};</script>${manifestScript}`,
    );
}

async function createDevServer() {
  const app = express();
  registerTileRoutes(app);
  registerDataRoutes(app, dataDir);
  const vite = await createViteServer({
    server: { middlewareMode: 'ssr' },
    appType: 'custom',
  });

  app.use(vite.middlewares);

  app.get('/installHook.js.map', (req, res, next) => {
    if (req.accepts('text/html')) {
      next();
      return;
    }
    res.setHeader('Cache-Control', 'no-store');
    res.status(404).json({ error: 'installHook source map unavailable in dev' });
  });

  app.get('/react_devtools_backend_compact.js.map', (req, res, next) => {
    if (req.accepts('text/html')) {
      next();
      return;
    }
    res.setHeader('Cache-Control', 'no-store');
    res.status(404).json({ error: 'React DevTools source map unavailable in dev' });
  });

  // Heavy warmup tasks run in the background after the server starts listening.

  app.use('*', async (req, res) => {
    const url = req.originalUrl;
    const requestStarted = performance.now();

    try {
      let template = await fs.readFile(resolve('index.html'), 'utf-8');
      template = await vite.transformIndexHtml(url, template);

      const initialData = await createAppData();
      const appDataMeta = getLatestAppDataMeta();
      console.log('[ssr] dev render', {
        url,
        appDataSource: appDataMeta?.fromCache ? 'cache' : 'refreshed',
        refreshedAt: appDataMeta?.refreshedAt || null,
      });
      const { render } = await vite.ssrLoadModule('/src/entry-server.jsx');
      const { appHtml } = await render(url, { initialData });

      const html = injectTemplate(template, appHtml, initialData, pmtilesManifest);
      const renderDurationMs = performance.now() - requestStarted;
      recordSsrMetric({
        durationMs: renderDurationMs,
        fromCache: appDataMeta?.fromCache ?? null,
        appDataDurationMs: appDataMeta?.durationMs,
      });
      const headers = {
        'Content-Type': 'text/html',
        'X-Render-Duration': String(Math.round(renderDurationMs)),
        'Server-Timing': `render;dur=${renderDurationMs.toFixed(1)}`,
      };
      if (Number.isFinite(appDataMeta?.durationMs)) {
        headers['X-App-Data-Duration'] = String(Math.round(appDataMeta.durationMs));
      }
      if (Number.isFinite(appDataMeta?.sizeBytes)) {
        headers['X-App-Data-Bytes'] = String(appDataMeta.sizeBytes);
      }
      res.status(200).set(headers);
      if (appDataMeta) {
        res.setHeader('X-App-Data-Source', appDataMeta.fromCache ? 'cache' : 'refreshed');
        if (appDataMeta.refreshedAt) {
          res.setHeader('X-App-Data-Refreshed-At', appDataMeta.refreshedAt);
        }
      }
      res.end(html);
    } catch (err) {
      vite.ssrFixStacktrace(err);
      console.error(err);
      res.status(500).set({ 'Content-Type': 'text/plain' }).end('Internal Server Error');
    }
  });

  const port = Number(process.env.PORT ?? 5173);
  app.listen(port, () => {
    startupState.listening = true;
    startupState.readyAt = Date.now();
    console.log(`\nSSR dev server running at http://localhost:${port}`);
    warmInitialTiles('dev-server').catch((error) => {
      console.warn('[warmup] dev tile warmup failed:', error?.message || error);
    });
  });
}

async function createProdServer() {
  const app = express();
  const distPath = resolve('dist/client');
  const ssrEntry = resolve('dist/server/entry-server.js');

  registerTileRoutes(app);
  registerDataRoutes(app, dataDir);

  app.use(express.static(distPath, { index: false }));

  app.get('/installHook.js.map', (req, res, next) => {
    if (req.accepts('text/html')) {
      next();
      return;
    }
    res.setHeader('Cache-Control', 'no-store');
    res.status(404).json({ error: 'installHook source map unavailable in prod' });
  });

  app.get('/react_devtools_backend_compact.js.map', (req, res, next) => {
    if (req.accepts('text/html')) {
      next();
      return;
    }
    res.setHeader('Cache-Control', 'no-store');
    res.status(404).json({ error: 'React DevTools source map unavailable in prod' });
  });

  app.use('*', async (req, res) => {
    const url = req.originalUrl;
    const requestStarted = performance.now();

    try {
      const template = await fs.readFile(path.join(distPath, 'index.html'), 'utf-8');
      const { render } = await import(pathToFileURL(ssrEntry));
      const initialData = await createAppData();
      const appDataMeta = getLatestAppDataMeta();
      console.log('[ssr] prod render', {
        url,
        appDataSource: appDataMeta?.fromCache ? 'cache' : 'refreshed',
        refreshedAt: appDataMeta?.refreshedAt || null,
      });
      const { appHtml } = await render(url, { initialData });

      const html = injectTemplate(template, appHtml, initialData, pmtilesManifest);
      const renderDurationMs = performance.now() - requestStarted;
      recordSsrMetric({
        durationMs: renderDurationMs,
        fromCache: appDataMeta?.fromCache ?? null,
        appDataDurationMs: appDataMeta?.durationMs,
      });
      const headers = {
        'Content-Type': 'text/html',
        'X-Render-Duration': String(Math.round(renderDurationMs)),
        'Server-Timing': `render;dur=${renderDurationMs.toFixed(1)}`,
      };
      if (Number.isFinite(appDataMeta?.durationMs)) {
        headers['X-App-Data-Duration'] = String(Math.round(appDataMeta.durationMs));
      }
      if (Number.isFinite(appDataMeta?.sizeBytes)) {
        headers['X-App-Data-Bytes'] = String(appDataMeta.sizeBytes);
      }
      res.status(200).set(headers);
      if (appDataMeta) {
        res.setHeader('X-App-Data-Source', appDataMeta.fromCache ? 'cache' : 'refreshed');
        if (appDataMeta.refreshedAt) {
          res.setHeader('X-App-Data-Refreshed-At', appDataMeta.refreshedAt);
        }
      }
      res.end(html);
    } catch (err) {
      console.error(err);
      res.status(500).set({ 'Content-Type': 'text/plain' }).end('Internal Server Error');
    }
  });

  // Start listening FIRST so healthcheck can respond, then do warmup tasks
  const port = Number.parseInt(process.env.PORT ?? '8080', 10);
  const host = process.env.HOST || '0.0.0.0';
  app.listen(port, host, async () => {
    startupState.listening = true;
    startupState.readyAt = Date.now();
    console.log(`\nSSR production server running at http://${host}:${port}`);

    // Warmup tasks run after server is listening (non-blocking)
    try {
      console.time('app-data:warmup');
      await createAppData({ bypassCache: true });
      console.timeEnd('app-data:warmup');
    } catch (error) {
      console.warn('Unable to warm app data cache:', error.message);
    }

    // Skip GeoJSON tile service when PostGIS handles parking_tickets tiles
    // Loading the 132MB GeoJSON causes OOM on memory-constrained Railway containers
    if (!postgisTileService.isDatasetEnabled('parking_tickets')) {
      try {
        console.time('tile-service:warmup');
        await tileService.ensureLoaded();
        console.timeEnd('tile-service:warmup');
      } catch (error) {
        console.warn('Unable to warm tile cache:', error.message);
      }
    } else {
      console.log('[warmup] skipping GeoJSON tile service - PostGIS enabled for parking_tickets');
    }

    try {
      await warmInitialTiles('prod-startup');
    } catch (error) {
      console.warn('[warmup] production tile warmup failed:', error?.message || error);
    }

    scheduleProductionWarmup();
  });
}

if (process.env.NODE_ENV === 'production') {
  createProdServer();
} else {
  createDevServer();
}
