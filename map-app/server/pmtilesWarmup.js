import process from 'node:process';

import { PMTiles } from 'pmtiles';

let warmupTimer = null;
let isRunning = false;

function clampLat(lat) {
  return Math.max(Math.min(lat, 85.05112878), -85.05112878);
}

function lngLatToTile(lng, lat, zoom) {
  const scale = 2 ** zoom;
  const x = Math.floor(((lng + 180) / 360) * scale);
  const latRad = (clampLat(lat) * Math.PI) / 180;
  const y = Math.floor(
    ((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2) * scale,
  );
  return { x, y };
}

async function warmShard(shard, zooms, centerLng, centerLat) {
  const pmtiles = new PMTiles(shard.url);
  for (const zoom of zooms) {
    const { x, y } = lngLatToTile(centerLng, centerLat, zoom);
    try {
      await pmtiles.getZxy(zoom, x, y);
    } catch (error) {
      if (process.env.NODE_ENV !== 'production') {
        console.warn(`PMTiles warmup failed for ${shard.url} z${zoom}/${x}/${y}`, error?.message || error);
      }
    }
  }
}

async function runWarmup(manifest, runtimeConfig) {
  if (!manifest?.enabled) {
    return;
  }
  if (isRunning) {
    return;
  }
  isRunning = true;
  try {
    const zooms = runtimeConfig?.warmupZooms || [10, 11, 12, 13];
    const [centerLng, centerLat] = runtimeConfig?.warmupCenter || [-79.3832, 43.6532];

    const collections = [manifest.datasets || {}, manifest.wardDatasets || {}];
    for (const collection of collections) {
      for (const dataset of Object.values(collection)) {
        if (!dataset) {
          continue;
        }
        const shards = Array.isArray(dataset.shards) && dataset.shards.length > 0
          ? dataset.shards
          : [dataset];
        // Prioritise the first shard, assumed to be the Ontario/GTA edge shard.
        const [primaryShard, ...fallbacks] = shards;
        if (!primaryShard?.url) {
          continue;
        }
        await warmShard(primaryShard, zooms, centerLng, centerLat);
        for (const shard of fallbacks) {
          if (!shard?.url) {
            continue;
          }
          await warmShard(shard, zooms.slice(0, 2), centerLng, centerLat);
        }
      }
    }
  } finally {
    isRunning = false;
  }
}

export function schedulePmtilesWarmup(manifest, runtimeConfig) {
  if (!manifest?.enabled) {
    return;
  }
  const interval = runtimeConfig?.warmupIntervalMs || 60 * 60 * 1000;
  runWarmup(manifest, runtimeConfig).catch((error) => {
    console.warn('Initial PMTiles warmup failed:', error?.message || error);
  });
  if (warmupTimer) {
    clearInterval(warmupTimer);
  }
  warmupTimer = setInterval(() => {
    runWarmup(manifest, runtimeConfig).catch((error) => {
      if (process.env.NODE_ENV !== 'production') {
        console.warn('Scheduled PMTiles warmup failed:', error?.message || error);
      }
    });
  }, interval);
}

export default schedulePmtilesWarmup;
