/* eslint-env node */
import fs from 'fs/promises';
import { existsSync } from 'fs';
import express from 'express';
import path from 'path';
import process from 'node:process';
import { Readable } from 'node:stream';
import { brotliCompressSync, gzipSync } from 'node:zlib';
import { Buffer } from 'node:buffer';
import { fileURLToPath, pathToFileURL } from 'url';
import { createServer as createViteServer } from 'vite';
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

      const forwardedProto = req.get('x-forwarded-proto');
      const forwardedHost = req.get('x-forwarded-host');
      const hostHeader = forwardedHost || req.get('host');
      const originHint = hostHeader
        ? `${forwardedProto || req.protocol || 'https'}://${hostHeader}`
        : null;

      const proxyHeaders = {
        Accept: req.headers.accept || '*/*',
        'User-Agent': process.env.MAPTILER_USER_AGENT || 'toronto-parking-proxy/1.0',
      };
      if (originHint) {
        proxyHeaders.Referer = originHint;
        proxyHeaders.Origin = originHint;
      }

      const upstreamResponse = await fetch(upstreamUrl, {
        headers: proxyHeaders,
        redirect: 'follow',
      });

      if (!upstreamResponse.ok) {
        const responseText = await upstreamResponse.text().catch(() => '');
        console.warn(`MapTiler proxy ${upstreamUrl.pathname} responded with ${upstreamResponse.status}`);
        res.status(upstreamResponse.status);
        if (responseText) {
          res.setHeader('Content-Type', upstreamResponse.headers.get('content-type') || 'text/plain');
          res.send(responseText);
        } else {
          res.end();
        }
        return;
      }

      res.status(upstreamResponse.status);
      upstreamResponse.headers.forEach((value, header) => {
        const lower = header.toLowerCase();
        if (lower === 'transfer-encoding'
          || lower === 'content-encoding'
          || lower === 'content-length'
          || lower === 'set-cookie'
          || lower === 'set-cookie2'
          || lower.includes('maptiler')) {
          return;
        }
        if (typeof value === 'string' && (value.includes('api.maptiler.com') || value.toLowerCase().includes('key='))) {
          if (lower === 'location' || lower === 'content-location') {
            const sanitizedLocation = sanitizeMaptilerUrl(value);
            if (sanitizedLocation && sanitizedLocation !== value) {
              res.setHeader(header, sanitizedLocation);
            }
          }
          return;
        }
        res.setHeader(header, value);
      });
      res.setHeader('Cache-Control', 'public, max-age=86400, immutable');

      if (!upstreamResponse.body) {
        res.end();
        return;
      }

      const contentType = upstreamResponse.headers.get('content-type') || '';
      const isTextLike = /json|text|javascript|xml/i.test(contentType);
      if (isTextLike) {
        const text = await upstreamResponse.text();
        const sanitizedBody = sanitizeMaptilerText(text);
        res.send(sanitizedBody);
        return;
      }

      const stream = Readable.fromWeb(upstreamResponse.body);
      stream.on('error', (error) => {
        console.error('MapTiler proxy stream error:', error.message);
        res.destroy(error);
      });
      stream.pipe(res);
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
