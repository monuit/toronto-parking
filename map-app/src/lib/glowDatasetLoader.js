import { MAP_CONFIG } from './mapSources.js';

const GLOW_DATASETS = new Map([
  ['parking_tickets', MAP_CONFIG.DATA_PATHS.CITY_GLOW_LINES],
  ['red_light_locations', MAP_CONFIG.DATA_PATHS.RED_LIGHT_GLOW_LINES],
  ['ase_locations', MAP_CONFIG.DATA_PATHS.ASE_GLOW_LINES],
]);

const glowCache = new Map();
const GLOW_CACHE_TTL_MS = 10 * 60 * 1000;

function resolveCacheEntry(key) {
  const entry = glowCache.get(key);
  if (!entry) {
    return null;
  }
  if (entry.expiresAt && entry.expiresAt <= Date.now()) {
    glowCache.delete(key);
    return null;
  }
  return entry;
}

function normalise(dataset) {
  if (!dataset || !GLOW_DATASETS.has(dataset)) {
    return 'parking_tickets';
  }
  return dataset;
}

export function loadGlowDataset(dataset) {
  const key = normalise(dataset);
  const cached = resolveCacheEntry(key);
  if (cached) {
    return cached.promise;
  }

  const url = GLOW_DATASETS.get(key);
  const request = fetch(url, {
    cache: 'force-cache',
    credentials: 'same-origin',
  })
    .then((response) => {
      if (!response.ok) {
        throw new Error(`Failed to load glow dataset ${key} (${response.status})`);
      }
      return response.json();
    })
    .then((data) => {
      glowCache.set(key, {
        promise: Promise.resolve(data),
        expiresAt: Date.now() + GLOW_CACHE_TTL_MS,
      });
      return data;
    })
    .catch((error) => {
      glowCache.delete(key);
      throw error;
    });

  glowCache.set(key, {
    promise: request,
    expiresAt: Date.now() + GLOW_CACHE_TTL_MS,
  });
  return request;
}

export function prefetchGlowDatasets(datasets = ['parking_tickets', 'red_light_locations', 'ase_locations']) {
  const tasks = [];
  datasets.forEach((dataset) => {
    const key = normalise(dataset);
    if (resolveCacheEntry(key)) {
      return;
    }
    tasks.push(
      loadGlowDataset(key).catch((error) => {
        console.warn(`Glow dataset prefetch failed for ${key}:`, error.message);
        return null;
      }),
    );
  });
  if (tasks.length === 0) {
    return Promise.resolve([]);
  }
  return Promise.all(tasks);
}

export function clearGlowDatasetCache(dataset = null) {
  if (!dataset) {
    glowCache.clear();
    return;
  }
  const key = normalise(dataset);
  glowCache.delete(key);
}
