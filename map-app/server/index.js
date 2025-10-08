/* eslint-env node */
import fs from 'fs/promises';
import { existsSync } from 'fs';
import express from 'express';
import path from 'path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'url';
import { createServer as createViteServer } from 'vite';
import { createAppData } from './createAppData.js';
import { createTileService } from './tileService.js';
import { mergeGeoJSONChunks } from './mergeGeoJSONChunks.js';
import { getDatasetTotals } from './datasetTotalsService.js';
import { wakeRemoteServices } from './wakeRemoteServices.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const resolve = (p) => path.resolve(__dirname, '..', p);

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

// Set data directory for bundled server modules
const isProd = process.env.NODE_ENV === 'production';
const dataDir = resolveDataDirectory(isProd);
if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = dataDir;
}
if (isProd && !dataDir.includes('dist/client/data')) {
  console.warn(`Using fallback data directory: ${dataDir}`);
}

// Merge split GeoJSON chunks at startup
console.log('🚀 Initializing server...');
const wakeResults = await wakeRemoteServices();
console.log(
  `   Redis: ${wakeResults.redis.enabled ? (wakeResults.redis.awake ? 'awake' : 'sleeping') : 'disabled'} | ` +
    `Postgres: ${wakeResults.postgres.enabled ? (wakeResults.postgres.awake ? 'awake' : 'sleeping') : 'disabled'}`,
);
await mergeGeoJSONChunks();

const tileService = createTileService();

function registerTileRoutes(app) {
  app.get('/tiles/:z/:x/:y.pbf', async (req, res) => {
    const z = Number.parseInt(req.params.z, 10);
    const x = Number.parseInt(req.params.x, 10);
    const y = Number.parseInt(req.params.y, 10);

    if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y)) {
      res.status(400).json({ error: 'Invalid tile coordinates' });
      return;
    }

    try {
      const tile = await tileService.getTile(z, x, y);
      if (!tile) {
        res.status(204).end();
        return;
      }
      res.setHeader('Content-Type', 'application/x-protobuf');
      res.setHeader('Cache-Control', 'public, max-age=86400, immutable');
      res.end(tile);
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
      const totals = await getDatasetTotals(dataset, { dataDir });
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

  app.get('/api/map-summary', async (req, res) => {
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

function injectTemplate(template, appHtml, initialData) {
  const safeData = JSON.stringify(initialData).replace(/</g, '\\u003c');
  return template
    .replace('<!--app-html-->', appHtml)
    .replace('<!--initial-data-->', `<script>window.__INITIAL_DATA__ = ${safeData};</script>`);
}

async function createDevServer() {
  const app = express();
  registerTileRoutes(app);
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
      res.status(500).end(err.message);
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
      res.status(500).end(err.message);
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
