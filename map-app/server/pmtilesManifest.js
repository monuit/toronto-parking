import { getPmtilesRuntimeConfig } from './runtimeConfig.js';
import { getShardConfig } from '../../shared/pmtiles/index.js';

const FALLBACK_DATASET_CONFIG = {
  parking_tickets: {
    label: 'Parking tickets (Ontario primary shard)',
    vectorLayer: 'parking_tickets',
    shards: [
      {
        id: 'ontario',
        filename: 'parking_tickets-ontario.pmtiles',
        bounds: [-81.0, 42.0, -78.0, 44.4],
        minZoom: 8,
        maxZoom: 16,
      },
      {
        id: 'canada',
        filename: 'parking_tickets-canada.pmtiles',
        bounds: [-142.0, 41.0, -52.0, 70.0],
        minZoom: 6,
        maxZoom: 12,
      },
    ],
  },
  red_light_locations: {
    label: 'Red light cameras',
    vectorLayer: 'red_light_locations',
    shards: [
      {
        id: 'ontario',
        filename: 'red-light-ontario.pmtiles',
        bounds: [-81.0, 42.0, -78.0, 44.4],
        minZoom: 8,
        maxZoom: 16,
      },
      {
        id: 'canada',
        filename: 'red-light-canada.pmtiles',
        bounds: [-142.0, 41.0, -52.0, 70.0],
        minZoom: 6,
        maxZoom: 12,
      },
    ],
  },
  ase_locations: {
    label: 'Automated speed enforcement cameras',
    vectorLayer: 'ase_locations',
    shards: [
      {
        id: 'ontario',
        filename: 'ase-ontario.pmtiles',
        bounds: [-81.0, 42.0, -78.0, 44.4],
        minZoom: 8,
        maxZoom: 16,
      },
      {
        id: 'canada',
        filename: 'ase-canada.pmtiles',
        bounds: [-142.0, 41.0, -52.0, 70.0],
        minZoom: 6,
        maxZoom: 12,
      },
    ],
  },
};

const FALLBACK_WARD_DATASET_CONFIG = {
  red_light_locations: {
    label: 'Red light ward choropleth',
    vectorLayer: 'ward_polygons',
    filename: 'red_light_ward_choropleth.pmtiles',
    minZoom: 8,
    maxZoom: 12,
  },
  ase_locations: {
    label: 'ASE ward choropleth',
    vectorLayer: 'ward_polygons',
    filename: 'ase_ward_choropleth.pmtiles',
    minZoom: 8,
    maxZoom: 12,
  },
  cameras_combined: {
    label: 'Camera totals ward choropleth',
    vectorLayer: 'ward_polygons',
    filename: 'cameras_combined_ward_choropleth.pmtiles',
    minZoom: 8,
    maxZoom: 12,
  },
};

let DEFAULT_DATASET_CONFIG = FALLBACK_DATASET_CONFIG;
let DEFAULT_WARD_DATASET_CONFIG = FALLBACK_WARD_DATASET_CONFIG;

try {
  const shardConfig = getShardConfig();
  DEFAULT_DATASET_CONFIG = shardConfig.datasets || FALLBACK_DATASET_CONFIG;
  DEFAULT_WARD_DATASET_CONFIG = shardConfig.wardDatasets || FALLBACK_WARD_DATASET_CONFIG;
} catch (error) {
  console.warn('[pmtilesManifest] Failed to load shard configuration JSON:', error.message);
}

function normalizeFilename(filename) {
  if (!filename) {
    throw new Error('PMTiles shard is missing filename');
  }
  return filename.startsWith('/') ? filename.slice(1) : filename;
}

function resolveShardConfig(base, shard, prefix) {
  const bounds = Array.isArray(shard.bounds) && shard.bounds.length === 4
    ? shard.bounds.map((value) => Number(value))
    : [-180, -90, 180, 90];
  const minZoom = Number.isFinite(shard.minZoom) ? shard.minZoom : 0;
  const maxZoom = Number.isFinite(shard.maxZoom) ? shard.maxZoom : Math.max(minZoom, 16);
  const filename = normalizeFilename(shard.filename);
  const normalizedPrefix = prefix ? prefix.replace(/^\/+/, '').replace(/\/+$/, '') : '';
  const urlBase = normalizedPrefix ? `${base}/${normalizedPrefix}` : base;
  const url = `${urlBase}/${filename}`;

  return {
    id: shard.id,
    url,
    filename,
    bounds,
    minZoom,
    maxZoom,
  };
}

function mergeDatasetConfig(baseConfig, overrides) {
  if (!overrides) {
    return baseConfig;
  }
  const merged = { ...baseConfig };
  for (const [dataset, override] of Object.entries(overrides)) {
    if (!override) {
      continue;
    }
    const current = baseConfig[dataset] || {};
    merged[dataset] = {
      ...current,
      ...override,
      shards: Array.isArray(override.shards) && override.shards.length > 0
        ? override.shards
        : current.shards,
    };
  }
  return merged;
}

function mergeWardDatasetConfig(baseConfig, overrides) {
  if (!overrides) {
    return baseConfig;
  }
  return {
    ...baseConfig,
    ...overrides,
  };
}

function resolveWardDataset(base, datasetKey, config, prefix) {
  if (!config?.filename) {
    throw new Error(`Ward dataset ${datasetKey} missing PMTiles filename`);
  }
  const filename = normalizeFilename(config.filename);
  const normalizedPrefix = prefix ? prefix.replace(/^\/+/, '').replace(/\/+$/, '') : '';
  const urlBase = normalizedPrefix ? `${base}/${normalizedPrefix}` : base;
  return {
    label: config.label || datasetKey,
    vectorLayer: config.vectorLayer || 'ward_polygons',
    url: `${urlBase}/${filename}`,
    filename,
    minZoom: Number.isFinite(config.minZoom) ? config.minZoom : 8,
    maxZoom: Number.isFinite(config.maxZoom) ? config.maxZoom : 12,
  };
}

export function buildPmtilesManifest(runtimeConfig) {
  const runtime = runtimeConfig || getPmtilesRuntimeConfig();
  if (!runtime.enabled || !runtime.publicBaseUrl) {
    return {
      enabled: false,
      datasets: {},
      updatedAt: new Date().toISOString(),
    };
  }

  const baseUrl = runtime.publicBaseUrl.replace(/\/$/, '');
  const objectPrefix = runtime.objectPrefix ? String(runtime.objectPrefix).replace(/^\/+/, '').replace(/\/+$/, '') : '';
  const datasetConfig = mergeDatasetConfig(DEFAULT_DATASET_CONFIG, runtime.datasetOverrides);
  const wardDatasetConfig = mergeWardDatasetConfig(DEFAULT_WARD_DATASET_CONFIG, runtime.wardDatasetOverrides);
  const manifestDatasets = {};
  const wardDatasets = {};
  const warmupInfo = {
    center: runtime.warmupCenter,
    zooms: runtime.warmupZooms,
  };

  for (const [dataset, config] of Object.entries(datasetConfig)) {
    if (!config || !Array.isArray(config.shards) || config.shards.length === 0) {
      continue;
    }
    manifestDatasets[dataset] = {
      label: config.label || dataset,
      vectorLayer: config.vectorLayer || dataset,
      shards: config.shards.map((shard) => resolveShardConfig(baseUrl, shard, objectPrefix)),
    };
  }

  for (const [dataset, config] of Object.entries(wardDatasetConfig)) {
    if (!config) {
      continue;
    }
    wardDatasets[dataset] = resolveWardDataset(baseUrl, dataset, config, objectPrefix);
  }

  return {
    enabled: true,
    baseUrl,
    objectPrefix,
    bucket: runtime.bucket,
    region: runtime.region,
    updatedAt: new Date().toISOString(),
    warmup: warmupInfo,
    datasets: manifestDatasets,
    wardDatasets,
  };
}

export default buildPmtilesManifest;
