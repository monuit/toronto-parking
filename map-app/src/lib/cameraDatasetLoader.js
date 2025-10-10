import { MAP_CONFIG } from './mapSources.js';

const CAMERA_DATASETS = new Map([
  ['red_light_locations', MAP_CONFIG.DATA_PATHS.RED_LIGHT_LOCATIONS],
  ['ase_locations', MAP_CONFIG.DATA_PATHS.ASE_LOCATIONS],
]);

const cameraGeojsonCache = new Map();

function normaliseDataset(dataset) {
  return CAMERA_DATASETS.has(dataset) ? dataset : null;
}

export function loadCameraDataset(dataset) {
  const key = normaliseDataset(dataset);
  if (!key) {
    return Promise.resolve(null);
  }

  const existing = cameraGeojsonCache.get(key);
  if (existing) {
    return existing;
  }

  const url = CAMERA_DATASETS.get(key);
  const request = fetch(url, {
    cache: 'force-cache',
    credentials: 'same-origin',
  })
    .then((response) => {
      if (!response.ok) {
        throw new Error(`Failed to load ${key} geojson (${response.status})`);
      }
      return response.json();
    })
    .then((data) => {
      cameraGeojsonCache.set(key, Promise.resolve(data));
      return data;
    })
    .catch((error) => {
      cameraGeojsonCache.delete(key);
      throw error;
    });

  cameraGeojsonCache.set(key, request);
  return request;
}

export function prefetchCameraDatasets(datasets = ['red_light_locations', 'ase_locations']) {
  const tasks = [];
  for (const dataset of datasets) {
    const key = normaliseDataset(dataset);
    if (!key || cameraGeojsonCache.has(key)) {
      continue;
    }
    tasks.push(
      loadCameraDataset(key).catch((error) => {
        console.warn(`Camera dataset prefetch failed for ${key}:`, error.message);
        return null;
      }),
    );
  }
  if (tasks.length === 0) {
    return Promise.resolve([]);
  }
  return Promise.all(tasks);
}

export function clearCameraDatasetCache(dataset = null) {
  if (!dataset) {
    cameraGeojsonCache.clear();
    return;
  }
  const key = normaliseDataset(dataset);
  if (!key) {
    return;
  }
  cameraGeojsonCache.delete(key);
}
