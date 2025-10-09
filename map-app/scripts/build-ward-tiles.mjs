#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'fs/promises';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

import geojsonvt from 'geojson-vt';
import vtpbf from 'vt-pbf';

import { WARD_TILE_SOURCE_LAYER } from '../shared/mapConstants.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '..');
const DEFAULT_DATA_DIR = path.resolve(PROJECT_ROOT, 'public', 'data');
const OUTPUT_ROOT = path.resolve(DEFAULT_DATA_DIR, 'ward_tiles');
const SUPPORTED_DATASETS = new Set(['ase_locations', 'red_light_locations', 'cameras_combined']);
const DATASET_GEOJSON_FILES = {
  ase_locations: 'ase_ward_choropleth.geojson',
  red_light_locations: 'red_light_ward_choropleth.geojson',
  cameras_combined: 'cameras_combined_ward_choropleth.geojson',
};

function parseArgs() {
  const args = process.argv.slice(2);
  const datasets = [];
  let dataDir = DEFAULT_DATA_DIR;
  let minZoom = 0;
  let maxZoom = 12;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg.startsWith('--dataset=')) {
      datasets.push(arg.split('=')[1]);
    } else if (arg === '--dataset') {
      const next = args[i + 1];
      if (next) {
        datasets.push(next);
        i += 1;
      }
    } else if (arg.startsWith('--data-dir=')) {
      dataDir = path.resolve(arg.split('=')[1]);
    } else if (arg === '--data-dir') {
      const next = args[i + 1];
      if (next) {
        dataDir = path.resolve(next);
        i += 1;
      }
    } else if (arg.startsWith('--max-zoom=')) {
      maxZoom = Number.parseInt(arg.split('=')[1], 10);
    } else if (arg === '--max-zoom') {
      const next = args[i + 1];
      if (next) {
        maxZoom = Number.parseInt(next, 10);
        i += 1;
      }
    } else if (arg.startsWith('--min-zoom=')) {
      minZoom = Number.parseInt(arg.split('=')[1], 10);
    } else if (arg === '--min-zoom') {
      const next = args[i + 1];
      if (next) {
        minZoom = Number.parseInt(next, 10);
        i += 1;
      }
    }
  }

  if (!Number.isFinite(minZoom) || minZoom < 0) {
    minZoom = 0;
  }
  if (!Number.isFinite(maxZoom) || maxZoom < minZoom) {
    maxZoom = Math.max(12, minZoom);
  }

  const resolvedDatasets = datasets.filter((dataset) => SUPPORTED_DATASETS.has(dataset));
  if (resolvedDatasets.length === 0) {
    resolvedDatasets.push(...SUPPORTED_DATASETS);
  }

  return {
    datasets: Array.from(new Set(resolvedDatasets)),
    dataDir,
    outputDir: path.resolve(dataDir, 'ward_tiles'),
    minZoom,
    maxZoom,
  };
}

function clampLat(lat) {
  return Math.max(-85.05112878, Math.min(85.05112878, lat));
}

function lonToTileX(lon, zoom) {
  const scale = 2 ** zoom;
  return Math.floor(((lon + 180) / 360) * scale);
}

function latToTileY(lat, zoom) {
  const scale = 2 ** zoom;
  const clamped = clampLat(lat);
  const rad = (clamped * Math.PI) / 180;
  return Math.floor(
    ((1 - Math.log(Math.tan(rad) + 1 / Math.cos(rad)) / Math.PI) / 2) * scale,
  );
}

function computeBounds(geojson) {
  let minLng = Infinity;
  let minLat = Infinity;
  let maxLng = -Infinity;
  let maxLat = -Infinity;

  const update = (lng, lat) => {
    if (!Number.isFinite(lng) || !Number.isFinite(lat)) {
      return;
    }
    if (lng < minLng) minLng = lng;
    if (lng > maxLng) maxLng = lng;
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
  };

  const walk = (coords) => {
    if (Array.isArray(coords[0])) {
      for (const entry of coords) {
        walk(entry);
      }
    } else if (coords.length >= 2) {
      update(Number(coords[0]), Number(coords[1]));
    }
  };

  for (const feature of geojson.features || []) {
    const geometry = feature && feature.geometry;
    if (!geometry || !geometry.coordinates) {
      continue;
    }
    if (geometry.type === 'Point') {
      walk(geometry.coordinates);
    } else if (geometry.type === 'MultiPoint') {
      walk(geometry.coordinates);
    } else if (geometry.type === 'LineString' || geometry.type === 'MultiLineString') {
      walk(geometry.coordinates);
    } else if (geometry.type === 'Polygon' || geometry.type === 'MultiPolygon') {
      walk(geometry.coordinates);
    }
  }

  if (!Number.isFinite(minLng) || !Number.isFinite(minLat)) {
    return { minLng: -79.7, minLat: 43.4, maxLng: -79.1, maxLat: 43.9 };
  }
  return { minLng, minLat, maxLng, maxLat };
}

function getTileRange(bounds, zoom) {
  const scale = 2 ** zoom;
  const minX = Math.max(0, Math.min(scale - 1, lonToTileX(bounds.minLng, zoom)));
  const maxX = Math.max(0, Math.min(scale - 1, lonToTileX(bounds.maxLng, zoom)));
  const minY = Math.max(0, Math.min(scale - 1, latToTileY(bounds.maxLat, zoom)));
  const maxY = Math.max(0, Math.min(scale - 1, latToTileY(bounds.minLat, zoom)));
  return {
    minX: Math.min(minX, maxX),
    maxX: Math.max(minX, maxX),
    minY: Math.min(minY, maxY),
    maxY: Math.max(minY, maxY),
  };
}

async function ensureCleanOutput(dir) {
  await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  await fs.mkdir(dir, { recursive: true });
}

async function writeTile(outputDir, dataset, z, x, y, buffer) {
  const tileDir = path.join(outputDir, dataset, String(z), String(x));
  await fs.mkdir(tileDir, { recursive: true });
  const tilePath = path.join(tileDir, `${y}.pbf`);
  await fs.writeFile(tilePath, buffer);
}

async function generateTilesForDataset(dataset, config) {
  const filename = DATASET_GEOJSON_FILES[dataset];
  if (!filename) {
    console.warn(`[ward-tiles] Unsupported dataset: ${dataset}`);
    return;
  }
  const geojsonPath = path.join(config.dataDir, filename);
  try {
    await fs.access(geojsonPath);
  } catch {
    console.warn(`[ward-tiles] Missing GeoJSON for ${dataset}: ${geojsonPath}`);
    return;
  }

  const raw = await fs.readFile(geojsonPath, 'utf-8');
  const checksum = crypto.createHash('sha256').update(raw).digest('hex');
  const geojson = JSON.parse(raw);
  const bounds = computeBounds(geojson);
  const index = geojsonvt(geojson, {
    maxZoom: config.maxZoom,
    buffer: 2,
    tolerance: 4,
    extent: 4096,
  });

  const datasetOutputDir = path.join(config.outputDir, dataset);
  await ensureCleanOutput(datasetOutputDir);

  let tileCount = 0;
  for (let zoom = config.minZoom; zoom <= config.maxZoom; zoom += 1) {
    const range = getTileRange(bounds, zoom);
    for (let x = range.minX; x <= range.maxX; x += 1) {
      for (let y = range.minY; y <= range.maxY; y += 1) {
        const tile = index.getTile(zoom, x, y);
        if (!tile || !tile.features || tile.features.length === 0) {
          continue;
        }
        const buffer = Buffer.from(
          vtpbf.fromGeojsonVt({ [WARD_TILE_SOURCE_LAYER]: tile }, { extent: 4096, version: 2 }),
        );
        await writeTile(config.outputDir, dataset, zoom, x, y, buffer);
        tileCount += 1;
      }
    }
  }

  const manifest = {
    dataset,
    generatedAt: new Date().toISOString(),
    minZoom: config.minZoom,
    maxZoom: config.maxZoom,
    tileCount,
    checksum,
    version: Number.parseInt(checksum.slice(0, 12), 16),
    etag: `W/"${dataset}:ward-tiles:${checksum}"`,
  };

  await fs.writeFile(
    path.join(datasetOutputDir, 'manifest.json'),
    JSON.stringify(manifest, null, 2),
    'utf-8',
  );

  console.log(`[ward-tiles] Generated ${tileCount} tiles for ${dataset} (zoom ${config.minZoom}-${config.maxZoom}).`);
}

async function main() {
  const config = parseArgs();
  if (config.datasets.length === 0) {
    console.warn('[ward-tiles] No datasets requested; exiting.');
    return;
  }

  await fs.mkdir(config.outputDir, { recursive: true });

  for (const dataset of config.datasets) {
    try {
      await generateTilesForDataset(dataset, config);
    } catch (error) {
      console.error(`[ward-tiles] Failed to generate tiles for ${dataset}:`, error);
      process.exitCode = 1;
    }
  }
}

main().catch((error) => {
  console.error('[ward-tiles] Unhandled error:', error);
  process.exit(1);
});
