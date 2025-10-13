import process from 'node:process';
import { performance } from 'node:perf_hooks';

import { PMTiles } from 'pmtiles';

let warmupTimer = null;
let isRunning = false;

const DEFAULT_TILE_OFFSETS = [
  [0, 0],
  [1, 0],
  [-1, 0],
  [0, 1],
  [0, -1],
  [1, 1],
  [-1, -1],
  [1, -1],
  [-1, 1],
];

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

function buildWarmupTiles(centerLng, centerLat, zooms, offsets = DEFAULT_TILE_OFFSETS) {
  const tiles = [];
  const seen = new Set();
  for (const zoom of zooms) {
    const base = lngLatToTile(centerLng, centerLat, zoom);
    const worldSize = 2 ** zoom;
    for (const [dx, dy] of offsets) {
      const wrappedX = ((base.x + dx) % worldSize + worldSize) % worldSize;
      const clampedY = Math.min(Math.max(base.y + dy, 0), worldSize - 1);
      const key = `${zoom}:${wrappedX}:${clampedY}`;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      tiles.push({ zoom, x: wrappedX, y: clampedY });
    }
  }
  return tiles;
}

async function warmShard(shard, tiles, options = {}) {
  const { onTileFetched } = options;
  if (!shard?.url && !shard?.originUrl) {
    return;
  }
  const urlVariants = [];
  const originUrl = shard.originUrl || shard.url;
  if (originUrl) {
    urlVariants.push({ url: originUrl, source: 'origin' });
  }
  if (shard.url && shard.url !== originUrl) {
    urlVariants.push({ url: shard.url, source: 'cdn' });
  }

  for (const variant of urlVariants) {
    const pmtiles = new PMTiles(variant.url);
    for (const tile of tiles) {
      const startedAt = performance.now();
      let success = false;
      let errorMessage = null;
      try {
        await pmtiles.getZxy(tile.zoom, tile.x, tile.y);
        success = true;
      } catch (error) {
        errorMessage = error?.message || String(error);
        if (process.env.NODE_ENV !== 'production') {
          console.warn(`PMTiles warmup failed for ${variant.url} z${tile.zoom}/${tile.x}/${tile.y}`, errorMessage);
        }
      } finally {
        if (typeof onTileFetched === 'function') {
          onTileFetched({
            ...tile,
            url: variant.url,
            source: variant.source,
            durationMs: performance.now() - startedAt,
            success,
            errorMessage,
          });
        }
      }
    }
  }
}

async function runWarmup(manifest, runtimeConfig, options = {}) {
  if (!manifest?.enabled || isRunning) {
    return;
  }
  isRunning = true;
  const callbacks = options || {};
  const runStartedAt = Date.now();
  const runStartHr = performance.now();
  let tileCount = 0;
  let failureCount = 0;
  let originTiles = 0;
  let cdnTiles = 0;
  let lastErrorMessage = null;

  const wrappedOptions = {
    onTileFetched: (details) => {
      tileCount += 1;
      if (details.source === 'cdn') {
        cdnTiles += 1;
      } else {
        originTiles += 1;
      }
      if (!details.success) {
        failureCount += 1;
        if (!lastErrorMessage) {
          lastErrorMessage = details.errorMessage || null;
        }
      }
      if (typeof callbacks.onTileFetched === 'function') {
        callbacks.onTileFetched(details);
      }
    },
  };

  try {
    const zooms = runtimeConfig?.warmupZooms || [10, 11, 12, 13];
    const [centerLng, centerLat] = runtimeConfig?.warmupCenter || [-79.3832, 43.6532];
    const tileOffsets = Array.isArray(runtimeConfig?.warmupOffsets) && runtimeConfig.warmupOffsets.length > 0
      ? runtimeConfig.warmupOffsets
      : DEFAULT_TILE_OFFSETS;
    const tiles = buildWarmupTiles(centerLng, centerLat, zooms, tileOffsets);
    const fallbackZooms = new Set(zooms.slice(0, Math.min(2, zooms.length)));
    const fallbackTiles = tiles.filter((tile) => fallbackZooms.has(tile.zoom));

    const collections = [manifest.datasets || {}, manifest.wardDatasets || {}, manifest.glowDatasets || {}];
    for (const collection of collections) {
      for (const dataset of Object.values(collection)) {
        if (!dataset) {
          continue;
        }
        const shards = Array.isArray(dataset.shards) && dataset.shards.length > 0
          ? dataset.shards
          : [dataset];
        const [primaryShard, ...fallbacks] = shards;
        if (primaryShard?.url || primaryShard?.originUrl) {
          await warmShard(primaryShard, tiles, wrappedOptions);
        }
        for (const shard of fallbacks) {
          if (!shard?.url && !shard?.originUrl) {
            continue;
          }
          await warmShard(shard, fallbackTiles, wrappedOptions);
        }
      }
    }
  } catch (error) {
    if (!lastErrorMessage) {
      lastErrorMessage = error?.message || String(error);
    }
    throw error;
  } finally {
    isRunning = false;
    if (typeof callbacks.onRunComplete === 'function') {
      callbacks.onRunComplete({
        startedAt: runStartedAt,
        durationMs: performance.now() - runStartHr,
        tileCount,
        failureCount,
        originTiles,
        cdnTiles,
        errorMessage: lastErrorMessage,
      });
    }
  }
}

export function schedulePmtilesWarmup(manifest, runtimeConfig, options = {}) {
  if (!manifest?.enabled) {
    return;
  }
  const interval = runtimeConfig?.warmupIntervalMs || 60 * 60 * 1000;
  runWarmup(manifest, runtimeConfig, options).catch((error) => {
    console.warn('Initial PMTiles warmup failed:', error?.message || error);
  });
  if (warmupTimer) {
    clearInterval(warmupTimer);
  }
  warmupTimer = setInterval(() => {
    runWarmup(manifest, runtimeConfig, options).catch((error) => {
      if (process.env.NODE_ENV !== 'production') {
        console.warn('Scheduled PMTiles warmup failed:', error?.message || error);
      }
    });
  }, interval);
}

export default schedulePmtilesWarmup;
