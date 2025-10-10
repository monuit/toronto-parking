/* eslint-env node */
import fs from 'fs/promises';
import { existsSync } from 'fs';
import express from 'express';
import path from 'path';
import process from 'node:process';
import { brotliCompressSync, gzipSync } from 'node:zlib';
import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'url';
import { createServer as createViteServer } from 'vite';
import { createClient } from 'redis';
import { createAppData } from './createAppData.js';
import { createTileService, getWardTile, prewarmWardTiles } from './tileService.js';
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
import { getRedisConfig } from './runtimeConfig.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const resolve = (p) => path.resolve(__dirname, '..', p);

function normaliseBaseUrl(baseUrl) {
  if (typeof baseUrl !== 'string' || !baseUrl.trim()) {
    return '';
  }
  return baseUrl.replace(/\/+$/u, '');
}

function sanitizeMaptilerUrl(raw, baseUrl = '') {
  if (typeof raw !== 'string' || raw.startsWith('/proxy/maptiler/')) {
    return raw;
  }
  const origin = normaliseBaseUrl(baseUrl);
  const prefix = origin ? `${origin}/proxy/maptiler/` : '/proxy/maptiler/';
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
    const remaining = url.searchParams.toString();
    return `${prefix}${pathname}${remaining ? `?${remaining}` : ''}`;
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
  const origin = normaliseBaseUrl(baseUrl);
  let sanitized = text.replace(/https?:\/\/api\.maptiler\.com\/[^\s"')]+/gi, (match) => sanitizeMaptilerUrl(match, origin));
  sanitized = sanitized.replace(/https:\\\/\\\/api\.maptiler\.com\\\/[^\s"')]+/gi, (match) => {
    const normalized = match.replace(/\\\//g, '/');
    const transformed = sanitizeMaptilerUrl(normalized, origin);
    if (typeof transformed !== 'string') {
      return match;
    }
    return transformed.startsWith('/proxy/maptiler/')
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
  key: null,
  mtime: null,
  template: null,
};
let loggedMissingMapKey = false;
const WARD_DATASETS = new Set(['red_light_locations', 'ase_locations', 'cameras_combined']);

const redisSettings = getRedisConfig();
const MAPTILER_REDIS_ENABLED = Boolean(redisSettings.enabled && redisSettings.url);
const MAPTILER_REDIS_NAMESPACE = process.env.MAP_DATA_REDIS_NAMESPACE || 'toronto:map-data';
const MAPTILER_CACHE_PREFIX = `${MAPTILER_REDIS_NAMESPACE}:maptiler:v1`;
const MAPTILER_PROXY_TIMEOUT_MS = Number.parseInt(process.env.MAPTILER_PROXY_TIMEOUT_MS || '', 10) || 12_000;
const MAPTILER_PROXY_MAX_RETRIES = Number.parseInt(process.env.MAPTILER_PROXY_MAX_RETRIES || '', 10) || 2;
const MAPTILER_PROXY_BACKOFF_MS = Number.parseInt(process.env.MAPTILER_PROXY_BACKOFF_MS || '', 10) || 500;
const MAPTILER_TILE_CACHE_CONTROL = 'public, max-age=21600, stale-while-revalidate=600';
const MAPTILER_FONT_CACHE_CONTROL = 'public, max-age=86400, stale-while-revalidate=3600';
const MAPTILER_DEFAULT_CACHE_CONTROL = 'public, max-age=3600, stale-while-revalidate=600';
const MAPTILER_FALLBACK_TIMEOUT_MS = Number.parseInt(process.env.MAPTILER_PROXY_FALLBACK_TIMEOUT_MS || '', 10) || 20_000;

let maptilerRedisPromise = null;
const maptilerInflight = new Map();
const TEXT_CONTENT_TYPE_REGEX = /json|text|javascript|xml/i;

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
  const key = process.env.MAPLIBRE_API_KEY || process.env.MAPTILER_API_KEY || '';
  if (!key && !loggedMissingMapKey) {
    console.warn('MAPLIBRE_API_KEY not set; serving MapTiler style with placeholder key.');
    loggedMissingMapKey = true;
  }

  let fileStats = null;
  try {
    fileStats = await fs.stat(stylePath);
  } catch (error) {
    throw new Error(`Base style file not found at ${stylePath}: ${error.message}`);
  }

  const mtimeMs = fileStats ? fileStats.mtimeMs : null;
  if (!styleCache.template || styleCache.key !== key || styleCache.mtime !== mtimeMs) {
    const raw = await fs.readFile(stylePath, 'utf-8');
    let parsed = null;
    try {
      parsed = JSON.parse(raw);
    } catch (error) {
      console.warn('Failed to parse base style JSON, serving raw fallback:', error.message);
    }

    if (parsed && typeof parsed === 'object') {
      if (!parsed.sources) {
        parsed.sources = {};
      }
      parsed.sources.openmaptiles = {
        type: 'vector',
        tiles: ['/proxy/maptiler/tiles/v3/{z}/{x}/{y}.pbf'],
        minzoom: 0,
        maxzoom: 14,
        attribution: parsed.sources?.openmaptiles?.attribution
          || '© OpenMapTiles © OpenStreetMap contributors',
      };
      parsed.glyphs = '/proxy/maptiler/fonts/{fontstack}/{range}.pbf';

      if (typeof parsed.sprite === 'string' && parsed.sprite.includes('get_your_own_OpIi9ZULNHzrESv6T2vL')) {
        parsed.sprite = parsed.sprite.replace('get_your_own_OpIi9ZULNHzrESv6T2vL', '');
      }

      styleCache.template = JSON.stringify(parsed);
    } else {
      styleCache.template = raw.replace(/get_your_own_OpIi9ZULNHzrESv6T2vL/g, '');
    }

    styleCache.key = key;
    styleCache.mtime = mtimeMs;
  }

  return sanitizeMaptilerText(styleCache.template, '');
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

async function bootstrapStartup() {
  try {
    console.log('🚀 Initializing server...');
    const wakeResults = await wakeRemoteServices();
    console.log(
      `   Redis: ${wakeResults.redis.enabled ? (wakeResults.redis.awake ? 'awake' : 'sleeping') : 'disabled'} | ` +
        `Postgres: ${wakeResults.postgres.enabled ? (wakeResults.postgres.awake ? 'awake' : 'sleeping') : 'disabled'}`,
    );
    await mergeGeoJSONChunks();

    const refreshIntervalSeconds = Number.parseInt(
      process.env.APP_DATA_REFRESH_SECONDS || (isProd ? '900' : '0'),
      10,
    );

    const backgroundRefresh = startBackgroundAppDataRefresh({
      intervalSeconds: refreshIntervalSeconds,
      createSnapshot: () => createAppData(),
      onAfterRefresh: async () => {
        try {
          await tileService.ensureLoaded();
          await Promise.all([...WARD_DATASETS].map((dataset) => prewarmWardTiles(dataset)));
        } catch (error) {
          console.warn('Background tile priming failed:', error.message);
        }
      },
    });
    if (backgroundRefresh && isProd) {
      backgroundRefresh.triggerNow();
    }

    await Promise.all(
      ['red_light_locations', 'ase_locations'].map((cameraDataset) =>
        loadCameraLocations(cameraDataset).catch((error) => {
          console.warn(`Camera dataset preload failed for ${cameraDataset}:`, error.message);
          return null;
        }),
      ),
    );
  } catch (error) {
    console.error('Startup initialization failed:', error);
  }
}

bootstrapStartup();

function registerTileRoutes(app) {
  app.get('/proxy/maptiler/:path(*)', async (req, res) => {
    const key = process.env.MAPLIBRE_API_KEY || process.env.MAPTILER_API_KEY || '';
    if (!key) {
      res.status(503).json({ error: 'Map base unavailable' });
      return;
    }

    const resourcePath = req.params.path || '';
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
        if (error instanceof MaptilerHttpError && error.status >= 400 && error.status < 500 && error.status !== 429) {
          res.status(error.status).send(error.body || '');
          return;
        }

        console.warn(`MapTiler proxy primary fetch failed for ${resourcePath}:`, error.message);
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
        } catch (fallbackError) {
          const status = fallbackError instanceof MaptilerHttpError && Number.isInteger(fallbackError.status)
            ? fallbackError.status
            : 502;
          const body = fallbackError instanceof MaptilerHttpError ? fallbackError.body : '';
          const reason = isAbortError(error) ? 'aborted' : fallbackError.message;
          console.error(`MapTiler fallback fetch failed for ${resourcePath}:`, reason);
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
      res.setHeader('Cache-Control', 'public, max-age=300');
      res.send(style);
    } catch (error) {
      console.error('Failed to load base style', error);
      res.status(500).json({ error: 'Failed to load base map style' });
    }
  });

  app.get('/tiles/:z/:x/:y.pbf', async (req, res) => {
    const z = Number.parseInt(req.params.z, 10);
    const x = Number.parseInt(req.params.x, 10);
    const y = Number.parseInt(req.params.y, 10);
    const dataset = typeof req.query.dataset === 'string' && req.query.dataset.trim().length > 0
      ? req.query.dataset.trim()
      : 'parking_tickets';

    if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y)) {
      res.status(400).json({ error: 'Invalid tile coordinates' });
      return;
    }

    if (dataset === 'parking_tickets' && z < TICKET_TILE_MIN_ZOOM) {
      res.status(204).end();
      return;
    }

    if (dataset !== 'parking_tickets') {
      // Camera datasets are delivered via static GeoJSON sources on the client.
      res.setHeader('Cache-Control', 'public, max-age=86400, immutable');
      res.status(204).end();
      return;
    }

    try {
      const tile = await tileService.getTile(z, x, y);
      if (!tile) {
        res.status(204).end();
        return;
      }
      const etag = tile.version !== null && tile.version !== undefined
        ? `W/"tickets:${tile.version}"`
        : null;
      if (etag && req.headers['if-none-match'] === etag) {
        res.status(304).end();
        return;
      }

      const acceptEncoding = req.headers['accept-encoding'] || '';
      const { buffer: encoded, encoding } = encodeTileBuffer(tile.buffer, acceptEncoding);
      res.setHeader('Content-Type', 'application/vnd.mapbox-vector-tile');
      res.setHeader('Cache-Control', 'public, max-age=86400, s-maxage=86400, stale-while-revalidate=3600');
      res.setHeader('Vary', 'Accept-Encoding');
      if (encoding) {
        res.setHeader('Content-Encoding', encoding);
      }
      if (etag) {
        res.setHeader('ETag', etag);
      }
      const tileTimestamp = extractTimestamp(tile.version);
      if (tileTimestamp) {
        res.setHeader('Last-Modified', new Date(tileTimestamp).toUTCString());
      }
      res.end(encoded);
    } catch (error) {
      console.error('Failed to serve vector tile', error);
      res.status(500).json({ error: 'Failed to generate tile' });
    }
  });

  app.get('/api/app-data', async (req, res) => {
    try {
      const snapshot = await createAppData();
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
      res.setHeader('Cache-Control', 'public, max-age=86400, s-maxage=86400, stale-while-revalidate=3600');
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
}

function registerDataRoutes(app, dataDirectory) {
  const resolveDataPath = (fileName) => path.join(dataDirectory, fileName);

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

function injectTemplate(template, appHtml, initialData) {
  const safeData = JSON.stringify(initialData).replace(/</g, '\\u003c');
  return template
    .replace('<!--app-html-->', appHtml)
    .replace('<!--initial-data-->', `<script>window.__INITIAL_DATA__ = ${safeData};</script>`);
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

  try {
    console.time('app-data:warmup');
    await createAppData();
    console.timeEnd('app-data:warmup');
  } catch (error) {
    console.warn('Unable to warm app data cache:', error.message);
  }

  try {
    console.time('tile-service:warmup');
    await tileService.ensureLoaded();
    console.timeEnd('tile-service:warmup');
  } catch (error) {
    console.warn('Unable to warm tile cache:', error.message);
  }

  app.use('*', async (req, res) => {
    const url = req.originalUrl;

    try {
      let template = await fs.readFile(resolve('index.html'), 'utf-8');
      template = await vite.transformIndexHtml(url, template);

      const initialData = await createAppData();
      const { render } = await vite.ssrLoadModule('/src/entry-server.jsx');
      const { appHtml } = await render(url, { initialData });

      const html = injectTemplate(template, appHtml, initialData);

      res.status(200).set({ 'Content-Type': 'text/html' }).end(html);
    } catch (err) {
      vite.ssrFixStacktrace(err);
      console.error(err);
      res.status(500).set({ 'Content-Type': 'text/plain' }).end('Internal Server Error');
    }
  });

  const port = Number(process.env.PORT ?? 5173);
  app.listen(port, () => {
    console.log(`\nSSR dev server running at http://localhost:${port}`);
  });
}

async function createProdServer() {
  const app = express();
  const distPath = resolve('dist/client');
  const ssrEntry = resolve('dist/server/entry-server.js');

  registerTileRoutes(app);
  registerDataRoutes(app, dataDir);

  app.use(express.static(distPath, { index: false }));

  try {
    console.time('app-data:warmup');
    await createAppData();
    console.timeEnd('app-data:warmup');
  } catch (error) {
    console.warn('Unable to warm app data cache:', error.message);
  }

  try {
    console.time('tile-service:warmup');
    await tileService.ensureLoaded();
    console.timeEnd('tile-service:warmup');
  } catch (error) {
    console.warn('Unable to warm tile cache:', error.message);
  }

  app.use('*', async (req, res) => {
    const url = req.originalUrl;

    try {
      const template = await fs.readFile(path.join(distPath, 'index.html'), 'utf-8');
      const { render } = await import(pathToFileURL(ssrEntry));
      const initialData = await createAppData();
      const { appHtml } = await render(url, { initialData });

      const html = injectTemplate(template, appHtml, initialData);

      res.status(200).set({ 'Content-Type': 'text/html' }).end(html);
    } catch (err) {
      console.error(err);
      res.status(500).set({ 'Content-Type': 'text/plain' }).end('Internal Server Error');
    }
  });

  const port = Number(process.env.PORT ?? 4173);
  app.listen(port, () => {
    console.log(`\nSSR production server running at http://localhost:${port}`);
  });
}

if (process.env.NODE_ENV === 'production') {
  createProdServer();
} else {
  createDevServer();
}
