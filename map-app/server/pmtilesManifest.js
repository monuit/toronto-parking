import { getPmtilesRuntimeConfig } from './runtimeConfig.js';
import { getShardConfig } from '../../shared/pmtiles/index.js';

const FALLBACK_DATASET_CONFIG = {
  parking_tickets: {
    label: 'Parking tickets (Ontario primary shard)',
    vectorLayer: 'parking_tickets',
    shards: [
      {
        id: 'overview',
        filename: 'parking_tickets-overview.pmtiles',
        bounds: [-170.0, 35.0, -50.0, 75.0],
        minZoom: 0,
        maxZoom: 7,
      },
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
        id: 'overview',
        filename: 'red-light-overview.pmtiles',
        bounds: [-170.0, 35.0, -50.0, 75.0],
        minZoom: 0,
        maxZoom: 7,
      },
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
        id: 'overview',
        filename: 'ase-overview.pmtiles',
        bounds: [-170.0, 35.0, -50.0, 75.0],
        minZoom: 0,
        maxZoom: 7,
      },
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

function resolveShardConfig(base, originBase, shard, prefix) {
  const bounds = Array.isArray(shard.bounds) && shard.bounds.length === 4
    ? shard.bounds.map((value) => Number(value))
    : [-180, -90, 180, 90];
  const normalizedBounds = [
    Math.min(bounds[0], bounds[2]),
    Math.min(bounds[1], bounds[3]),
    Math.max(bounds[0], bounds[2]),
    Math.max(bounds[1], bounds[3]),
  ];
  const minZoom = Number.isFinite(shard.minZoom) ? shard.minZoom : 0;
  const maxZoom = Number.isFinite(shard.maxZoom) ? shard.maxZoom : Math.max(minZoom, 16);
  const filename = normalizeFilename(shard.filename);
  const normalizedPrefix = prefix ? prefix.replace(/^\/+/, '').replace(/\/+$/, '') : '';
  const baseUrl = base ? (normalizedPrefix ? `${base}/${normalizedPrefix}` : base) : null;
  const originUrlBase = originBase ? (normalizedPrefix ? `${originBase}/${normalizedPrefix}` : originBase) : baseUrl;
  const originUrl = originUrlBase ? `${originUrlBase}/${filename}` : `${filename}`;
  const url = baseUrl ? `${baseUrl}/${filename}` : originUrl;

  return {
    id: shard.id,
    url,
    originUrl,
    filename,
    bounds: normalizedBounds,
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

function resolveWardDataset(base, originBase, datasetKey, config, prefix) {
  if (!config?.filename) {
    throw new Error(`Ward dataset ${datasetKey} missing PMTiles filename`);
  }
  const filename = normalizeFilename(config.filename);
  const normalizedPrefix = prefix ? prefix.replace(/^\/+/, '').replace(/\/+$/, '') : '';
  const baseUrl = base ? (normalizedPrefix ? `${base}/${normalizedPrefix}` : base) : null;
  const originUrlBase = originBase
    ? (normalizedPrefix ? `${originBase}/${normalizedPrefix}` : originBase)
    : baseUrl;
  const originUrl = originUrlBase ? `${originUrlBase}/${filename}` : `${filename}`;
  return {
    label: config.label || datasetKey,
    vectorLayer: config.vectorLayer || 'ward_polygons',
    url: baseUrl ? `${baseUrl}/${filename}` : originUrl,
    originUrl,
    filename,
    minZoom: Number.isFinite(config.minZoom) ? config.minZoom : 8,
    maxZoom: Number.isFinite(config.maxZoom) ? config.maxZoom : 12,
  };
}

function validateDatasetShards(datasetKey, shards) {
  if (!Array.isArray(shards) || shards.length === 0) {
    return;
  }
  const hasOverview = shards.some((shard) => Number(shard.minZoom) <= 1);
  if (!hasOverview) {
    console.warn(`[pmtilesManifest] Dataset ${datasetKey} is missing an overview shard (minZoom <= 1).`);
  }
  for (const shard of shards) {
    const [west, south, east, north] = shard.bounds;
    if (!Number.isFinite(west) || !Number.isFinite(south) || !Number.isFinite(east) || !Number.isFinite(north)) {
      console.warn(`[pmtilesManifest] Dataset ${datasetKey} shard ${shard.id} has invalid bounds`, shard.bounds);
    }
    if (west < -180 || east > 180 || south < -90 || north > 90) {
      console.warn(`[pmtilesManifest] Dataset ${datasetKey} shard ${shard.id} bounds exceed Web Mercator limits`, shard.bounds);
    }
  }
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

  const originBaseUrl = runtime.publicBaseUrl.replace(/\/$/, '');
  const cdnBaseUrl = runtime.cdnBaseUrl ? runtime.cdnBaseUrl.replace(/\/$/, '') : null;
  const baseUrl = cdnBaseUrl || originBaseUrl;
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
    const resolvedShards = config.shards
      .map((shard) => resolveShardConfig(baseUrl, originBaseUrl, shard, objectPrefix))
      .sort((a, b) => (a.minZoom - b.minZoom) || (a.maxZoom - b.maxZoom));
    validateDatasetShards(dataset, resolvedShards);
    manifestDatasets[dataset] = {
      label: config.label || dataset,
      vectorLayer: config.vectorLayer || dataset,
      shards: resolvedShards,
    };
  }

  for (const [dataset, config] of Object.entries(wardDatasetConfig)) {
    if (!config) {
      continue;
    }
    wardDatasets[dataset] = resolveWardDataset(baseUrl, originBaseUrl, dataset, config, objectPrefix);
  }

  return {
    enabled: true,
    baseUrl,
    originBaseUrl,
    cdnBaseUrl,
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
