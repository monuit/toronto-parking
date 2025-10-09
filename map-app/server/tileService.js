import process from 'node:process';
import { Buffer } from 'node:buffer';
import Supercluster from 'supercluster';
import geojsonvt from 'geojson-vt';
import vtpbf from 'vt-pbf';
import Flatbush from 'flatbush';
import { createClient } from 'redis';
import { normalizeStreetName } from '../shared/streetUtils.js';
import {
  RAW_POINT_ZOOM_THRESHOLD,
  SUMMARY_ZOOM_THRESHOLD,
  TILE_LAYER_NAME,
  WARD_TILE_SOURCE_LAYER,
} from '../shared/mapConstants.js';
import {
  getTicketChunks,
  loadTicketChunk,
  getTicketsRaw,
  loadCameraWardGeojson,
} from './ticketsDataStore.js';
import { getParkingLocationYearMap } from './yearlyMetricsService.js';
import { getRedisConfig } from './runtimeConfig.js';

const TILE_CACHE_LIMIT = 512;
const SUMMARY_LIMIT = 5;
const YEARS_LIMIT = 32;
const MONTHS_LIMIT = 24;

const redisSettings = getRedisConfig();
const TILE_REDIS_ENABLED = Boolean(redisSettings.enabled && redisSettings.url);
const TILE_REDIS_NAMESPACE = process.env.MAP_DATA_REDIS_NAMESPACE || 'toronto:map-data';
const TILE_REDIS_PREFIX = `${TILE_REDIS_NAMESPACE}:tiles:parking:v2`;
const TILE_REDIS_TTL_SECONDS = Number.parseInt(process.env.MAP_TILE_REDIS_TTL || '43200', 10);

let tileRedisPromise = null;

async function getTileRedisClient() {
  if (!TILE_REDIS_ENABLED) {
    return null;
  }
  if (tileRedisPromise) {
    try {
      const existing = await tileRedisPromise;
      if (existing && existing.isOpen) {
        return existing;
      }
    } catch (error) {
      console.warn('Vector tile Redis client error:', error.message);
    }
    tileRedisPromise = null;
  }

  tileRedisPromise = (async () => {
    const client = createClient({ url: redisSettings.url });
    const reset = () => {
      if (tileRedisPromise) {
        tileRedisPromise = null;
      }
    };
    client.on('error', (error) => {
      console.warn('Vector tile Redis connection error:', error.message);
    });
    client.on('end', reset);
    client.on('close', reset);
    try {
      await client.connect();
      return client;
    } catch (error) {
      reset();
      console.warn('Failed to connect to Redis for tiles:', error.message);
      try {
        await client.disconnect();
      } catch (disconnectError) {
        console.warn('Error closing tile Redis client after failure:', disconnectError.message);
      }
      return null;
    }
  })();

  const client = await tileRedisPromise;
  return client && client.isOpen ? client : null;
}

async function readTileFromRedis(version, z, x, y) {
  if (!TILE_REDIS_ENABLED) {
    return null;
  }
  const client = await getTileRedisClient();
  if (!client) {
    return null;
  }
  const key = `${TILE_REDIS_PREFIX}:${version ?? 'noversion'}:${z}:${x}:${y}`;
  try {
    const encoded = await client.get(key);
    if (!encoded) {
      return null;
    }
    return Buffer.from(encoded, 'base64');
  } catch (error) {
    console.warn('Failed to read tile from Redis:', error.message);
    return null;
  }
}

async function writeTileToRedis(version, z, x, y, buffer) {
  if (!TILE_REDIS_ENABLED || !buffer) {
    return;
  }
  const client = await getTileRedisClient();
  if (!client) {
    return;
  }
  const key = `${TILE_REDIS_PREFIX}:${version ?? 'noversion'}:${z}:${x}:${y}`;
  const encoded = buffer.toString('base64');
  client.set(key, encoded, { EX: TILE_REDIS_TTL_SECONDS }).catch((error) => {
    console.warn('Failed to cache tile in Redis:', error.message);
  });
}

const CLUSTER_VISIBILITY_RULES = [
  {
    minCount: 1000,
    opacityLow: 0.7,
    opacityMid: 0.7,
    opacityHigh: 0.7,
    labelLow: 1,
    labelMid: 1,
    labelHigh: 1,
  },
  {
    minCount: 300,
    opacityLow: 0,
    opacityMid: 0.7,
    opacityHigh: 0.7,
    labelLow: 0,
    labelMid: 1,
    labelHigh: 1,
  },
  {
    minCount: 75,
    opacityLow: 0,
    opacityMid: 0.55,
    opacityHigh: 0.65,
    labelLow: 0,
    labelMid: 0.75,
    labelHigh: 0.9,
  },
];

function decorateClusterFeature(tags) {
  const count = Number(tags.point_count || tags.count || tags.ticketCount || 0);
  if (!Number.isFinite(count) || count <= 0) {
    tags.cluster_opacity_low = 0;
    tags.cluster_opacity_mid = 0;
    tags.cluster_opacity_high = 0;
    tags.cluster_label_low = 0;
    tags.cluster_label_mid = 0;
    tags.cluster_label_high = 0;
    return;
  }

  const rule = CLUSTER_VISIBILITY_RULES.find((entry) => count >= entry.minCount);
  const fallbackOpacity = count >= 10 ? 0.4 : 0;
  const fallbackLabel = count >= 10 ? 0.6 : 0;

  const selected = rule || {
    opacityLow: 0,
    opacityMid: fallbackOpacity,
    opacityHigh: Math.min(0.5, fallbackOpacity),
    labelLow: 0,
    labelMid: fallbackLabel,
    labelHigh: Math.min(0.7, fallbackLabel),
  };

  tags.cluster_opacity_low = selected.opacityLow;
  tags.cluster_opacity_mid = selected.opacityMid;
  tags.cluster_opacity_high = selected.opacityHigh;
  tags.cluster_label_low = selected.labelLow;
  tags.cluster_label_mid = selected.labelMid;
  tags.cluster_label_high = selected.labelHigh;
}

const YEAR_OFFSET = 2000;
const YEAR_MASK_WIDTH = 32;
const MONTH_MASK_WIDTH = 12;

const WARD_TILE_OPTIONS = {
  maxZoom: 12,
  tolerance: 4,
  extent: 4096,
  buffer: 2,
};
const WARD_TILE_CACHE_LIMIT = 256;
const wardTileIndexes = new Map();
const wardTileVersions = new Map();
const wardTileCache = new Map();
const wardTilePromises = new Map();

function toNumberArray(value, limit) {
  if (!Array.isArray(value)) {
    return [];
  }
  const unique = new Set();
  for (const entry of value) {
    const num = Number(entry);
    if (!Number.isFinite(num)) {
      continue;
    }
    unique.add(num);
    if (unique.size >= limit) {
      break;
    }
  }
  return Array.from(unique).sort((a, b) => a - b).slice(0, limit);
}

function encodeYearMask(values) {
  if (!Array.isArray(values) || values.length === 0) {
    return 0;
  }
  let mask = 0;
  for (const value of values) {
    const offset = Math.trunc(value) - YEAR_OFFSET;
    if (offset < 0 || offset >= YEAR_MASK_WIDTH) {
      continue;
    }
    mask |= 1 << offset;
  }
  return mask >>> 0;
}

function decodeYearMask(mask) {
  if (!Number.isFinite(mask) || mask === 0) {
    return [];
  }
  const years = [];
  for (let offset = 0; offset < YEAR_MASK_WIDTH; offset += 1) {
    if ((mask & (1 << offset)) !== 0) {
      years.push(YEAR_OFFSET + offset);
    }
  }
  return years;
}

function yearMaskIncludes(mask, value) {
  if (!Number.isFinite(mask) || !Number.isFinite(value)) {
    return false;
  }
  const offset = Math.trunc(value) - YEAR_OFFSET;
  if (offset < 0 || offset >= YEAR_MASK_WIDTH) {
    return false;
  }
  return (mask & (1 << offset)) !== 0;
}

function encodeMonthMask(values) {
  if (!Array.isArray(values) || values.length === 0) {
    return 0;
  }
  let mask = 0;
  for (const value of values) {
    const offset = Math.trunc(value) - 1;
    if (offset < 0 || offset >= MONTH_MASK_WIDTH) {
      continue;
    }
    mask |= 1 << offset;
  }
  return mask >>> 0;
}

function decodeMonthMask(mask) {
  if (!Number.isFinite(mask) || mask === 0) {
    return [];
  }
  const months = [];
  for (let offset = 0; offset < MONTH_MASK_WIDTH; offset += 1) {
    if ((mask & (1 << offset)) !== 0) {
      months.push(offset + 1);
    }
  }
  return months;
}

function monthMaskIncludes(mask, value) {
  if (!Number.isFinite(mask) || !Number.isFinite(value)) {
    return false;
  }
  const offset = Math.trunc(value) - 1;
  if (offset < 0 || offset >= MONTH_MASK_WIDTH) {
    return false;
  }
  return (mask & (1 << offset)) !== 0;
}

function safeParseGeojson(raw) {
  if (!raw) {
    return null;
  }
  if (typeof raw === 'object') {
    return raw;
  }
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw);
    } catch (error) {
      console.warn('Failed to parse ward GeoJSON payload:', error.message);
      return null;
    }
  }
  return null;
}

function clearWardTileCache(dataset) {
  const prefix = `${dataset}:`;
  for (const key of wardTileCache.keys()) {
    if (key.startsWith(prefix)) {
      wardTileCache.delete(key);
    }
  }
}

function getWardTileCacheKey(dataset, z, x, y) {
  return `${dataset}:${z}/${x}/${y}`;
}

function buildTileKey(z, x, y) {
  return `${z}/${x}/${y}`;
}

function hydrateTemporalTags(tile) {
  if (!tile || !Array.isArray(tile.features)) {
    return;
  }
  for (const feature of tile.features) {
    const tags = feature?.tags;
    if (!tags) {
      continue;
    }
    if (typeof tags.yearMask === 'number') {
      tags.years = decodeYearMask(tags.yearMask);
      delete tags.yearMask;
    }
    if (typeof tags.monthMask === 'number') {
      tags.months = decodeMonthMask(tags.monthMask);
      delete tags.monthMask;
    }
  }
}

async function ensureWardTileIndex(dataset) {
  if (!dataset) {
    throw new Error('Dataset must be provided for ward tiles');
  }
  if (wardTileIndexes.has(dataset)) {
    return wardTileIndexes.get(dataset);
  }
  if (wardTilePromises.has(dataset)) {
    return wardTilePromises.get(dataset);
  }

  const promise = (async () => {
    const resource = await loadCameraWardGeojson(dataset);
    if (!resource) {
      throw new Error(`Ward dataset ${dataset} is unavailable`);
    }
    const payload = safeParseGeojson(resource.raw || resource.data);
    if (!payload || !Array.isArray(payload.features)) {
      throw new Error(`Invalid ward GeoJSON payload for ${dataset}`);
    }

    const index = geojsonvt(payload, WARD_TILE_OPTIONS);
    wardTileIndexes.set(dataset, index);
    const resolvedVersion = typeof resource.etag === 'string'
      ? resource.etag
      : (resource.version !== null && resource.version !== undefined
        ? `W/"${resource.version}"`
        : `W/"${Date.now()}"`);
    wardTileVersions.set(dataset, resolvedVersion);
    clearWardTileCache(dataset);
    return index;
  })()
    .catch((error) => {
      wardTileIndexes.delete(dataset);
      wardTileVersions.delete(dataset);
      clearWardTileCache(dataset);
      throw error;
    })
    .finally(() => {
      wardTilePromises.delete(dataset);
    });

  wardTilePromises.set(dataset, promise);
  return promise;
}

export async function prewarmWardTiles(dataset) {
  try {
    await ensureWardTileIndex(dataset);
  } catch (error) {
    console.warn('Failed to prewarm ward tiles:', error.message);
    throw error;
  }
}

export async function getWardTile(dataset, zValue, xValue, yValue) {
  const z = Number.parseInt(zValue, 10);
  const x = Number.parseInt(xValue, 10);
  const y = Number.parseInt(yValue, 10);
  if (![z, x, y].every(Number.isFinite)) {
    return null;
  }

  const cacheKey = getWardTileCacheKey(dataset, z, x, y);
  if (wardTileCache.has(cacheKey)) {
    return wardTileCache.get(cacheKey);
  }

  const index = await ensureWardTileIndex(dataset);
  if (!index) {
    return null;
  }
  const tile = index.getTile(z, x, y);
  const version = wardTileVersions.get(dataset) || null;
  if (!tile) {
    const payload = { buffer: null, version };
    wardTileCache.set(cacheKey, payload);
    if (wardTileCache.size > WARD_TILE_CACHE_LIMIT) {
      const oldestKey = wardTileCache.keys().next().value;
      if (oldestKey) {
        wardTileCache.delete(oldestKey);
      }
    }
    return payload;
  }

  const layerName = WARD_TILE_SOURCE_LAYER || 'ward_polygons';
  const normalizedTile = {
    ...tile,
    extent: typeof tile.extent === 'number' ? tile.extent : 4096,
    version: 2,
  };
  const buffer = vtpbf.fromGeojsonVt({ [layerName]: normalizedTile }, { extent: 4096, version: 2 });
  const payload = { buffer, version };
  wardTileCache.set(cacheKey, payload);
  if (wardTileCache.size > WARD_TILE_CACHE_LIMIT) {
    const oldestKey = wardTileCache.keys().next().value;
    if (oldestKey) {
      wardTileCache.delete(oldestKey);
    }
  }
  return payload;
}

// MARK: TileService
class TileService {
  constructor() {
    this.dataVersion = null;
    this.loadingPromise = null;
    this.clusterIndex = null;
    this.pointIndex = null;
    this.summaryPoints = [];
    this.locationTable = [];
    this.streetTable = [];
    this.summaryTree = null;
    this.tileCache = new Map();
    this.locationYearCounts = null;
  }

  async ensureLoaded() {
    const { version, chunks } = await getTicketChunks();
    const resolvedVersion = version ?? null;
    if (
      resolvedVersion !== null &&
      this.dataVersion !== null &&
      this.dataVersion === resolvedVersion &&
      this.clusterIndex &&
      this.pointIndex
    ) {
      return;
    }
    if (this.loadingPromise) {
      await this.loadingPromise;
      if (
        resolvedVersion !== null &&
        this.dataVersion !== null &&
        this.dataVersion === resolvedVersion &&
        this.clusterIndex &&
        this.pointIndex
      ) {
        return;
      }
    }
    this.loadingPromise = this.loadFromChunks({ version: resolvedVersion, chunks });
    try {
      await this.loadingPromise;
    } finally {
      this.loadingPromise = null;
    }
  }

  async loadFromChunks({ version, chunks }) {
    let chunkDescriptors = Array.isArray(chunks) ? chunks : [];

    if (chunkDescriptors.length === 0) {
      try {
        const resource = await getTicketsRaw();
        const parsed = JSON.parse(resource.raw);
        chunkDescriptors = [
          {
            features: Array.isArray(parsed?.features) ? parsed.features : [],
            featureCount: Array.isArray(parsed?.features) ? parsed.features.length : 0,
            source: resource.source,
            version: resource.version,
          },
        ];
      } catch (error) {
        console.error('Failed to load tickets GeoJSON fallback:', error.message);
        throw error;
      }
    }

    const locationYearCounts = await getParkingLocationYearMap().catch(() => new Map());
    this.locationYearCounts = locationYearCounts;

    const sanitized = [];
    const summaryPoints = [];
    const locationTable = [];
    const streetTable = [];
    const locationLookup = new Map();
    const streetLookup = new Map();
    let featureId = 0;

    const internString = (value, lookup, table) => {
      if (!value) {
        return null;
      }
      const existing = lookup.get(value);
      if (existing !== undefined) {
        return existing;
      }
      const id = table.length;
      table.push(value);
      lookup.set(value, id);
      return id;
    };

    for (const descriptor of chunkDescriptors) {
      const chunk = descriptor.features ? descriptor : await loadTicketChunk(descriptor);
      const features = Array.isArray(chunk?.features) ? chunk.features : [];
      for (const feature of features) {
        const coords = feature?.geometry?.coordinates;
        if (!Array.isArray(coords) || coords.length < 2) {
          continue;
        }
        const [longitude, latitude] = coords;
        if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
          continue;
        }

        const props = feature?.properties || {};
        const count = Number(props.count) || 0;
        const totalRevenue = Number(props.total_revenue) || 0;
        if (count <= 0) {
          continue;
        }

        const years = toNumberArray(props.years, YEARS_LIMIT);
        const months = toNumberArray(props.months, MONTHS_LIMIT);
        const yearMask = encodeYearMask(years);
        const monthMask = encodeMonthMask(months);
        const location = props.location || null;
        const topInfraction = props.top_infraction || null;
        const streetName = normalizeStreetName(location || props.address || '');

        const cleanedProperties = {
          id: featureId,
          location,
          count,
          total_revenue: Number(totalRevenue.toFixed(2)),
          top_infraction: topInfraction,
          yearMask,
          monthMask,
        };

        if (location && locationYearCounts.has(location)) {
          const yearEntry = locationYearCounts.get(location);
          cleanedProperties.year_counts = JSON.stringify(yearEntry);
        }

        if (!cleanedProperties.top_infraction) {
          delete cleanedProperties.top_infraction;
        }

        sanitized.push({
          type: 'Feature',
          geometry: {
            type: 'Point',
            coordinates: [longitude, latitude],
          },
          properties: cleanedProperties,
        });

        const locationId = internString(location || streetName, locationLookup, locationTable);
        const streetId = internString(streetName || location, streetLookup, streetTable);

        summaryPoints.push({
          longitude,
          latitude,
          ticketCount: count,
          totalRevenue,
          locationId,
          streetId,
          yearMask,
          monthMask,
          location,
        });

        featureId += 1;
      }

      if (!descriptor.features && chunk) {
        chunk.features = [];
      }
    }

    const clusterIndex = new Supercluster({
      minZoom: 0,
      maxZoom: 16,
      radius: 110,
      extent: 4096,
      map: (properties) => ({
        ...properties,
        ticketCount: properties.count,
        yearMask: properties.yearMask ?? 0,
        monthMask: properties.monthMask ?? 0,
      }),
      reduce: (acc, props) => {
        acc.total_revenue = (acc.total_revenue || 0) + (props.total_revenue || 0);
        acc.count = (acc.count || 0) + (props.count || props.ticketCount || 0);
        acc.ticketCount = (acc.ticketCount || 0) + (props.ticketCount || props.count || 0);
        acc.yearMask = (acc.yearMask || 0) | (props.yearMask || 0);
        acc.monthMask = (acc.monthMask || 0) | (props.monthMask || 0);
      },
    });

    clusterIndex.load([...sanitized]);

    const vtCollection = { type: 'FeatureCollection', features: sanitized };
    const pointIndex = geojsonvt(vtCollection, {
      maxZoom: 16,
      extent: 4096,
      buffer: 64,
      indexMaxZoom: 16,
      indexMaxPoints: 0,
    });

    vtCollection.features = [];
    const featureCount = summaryPoints.length || 1;
    sanitized.length = 0;

    const spatialIndex = new Flatbush(featureCount);
    for (const point of summaryPoints) {
      const { longitude, latitude } = point;
      if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
        continue;
      }
      spatialIndex.add(longitude, latitude, longitude, latitude);
    }
    spatialIndex.finish();

    this.clusterIndex = clusterIndex;
    this.pointIndex = pointIndex;
    this.summaryPoints = summaryPoints;
    this.locationTable = locationTable;
    this.streetTable = streetTable;
    this.summaryTree = spatialIndex;
    this.dataVersion = version ?? Date.now();
    this.tileCache.clear();
  }

  async getTile(z, x, y) {
    await this.ensureLoaded();
    if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y)) {
      return null;
    }
    const key = buildTileKey(z, x, y);
    if (this.tileCache.has(key)) {
      return this.tileCache.get(key);
    }
    const cacheVersion = this.dataVersion ?? null;
    const redisBuffer = await readTileFromRedis(cacheVersion, z, x, y);
    if (redisBuffer) {
      this.tileCache.set(key, redisBuffer);
      return redisBuffer;
    }

    const layers = {};

    if (z < RAW_POINT_ZOOM_THRESHOLD) {
      const clusterTile = this.clusterIndex.getTile(z, x, y);
      if (clusterTile) {
        hydrateTemporalTags(clusterTile);
        for (const feature of clusterTile.features || []) {
          if (feature && feature.tags) {
            decorateClusterFeature(feature.tags);
          }
        }
        layers[TILE_LAYER_NAME] = {
          ...clusterTile,
          extent: typeof clusterTile.extent === 'number' ? clusterTile.extent : 4096,
          version: 2,
        };
      }
    } else {
      const pointTile = this.pointIndex.getTile(z, x, y);
      if (pointTile) {
        hydrateTemporalTags(pointTile);
        layers[TILE_LAYER_NAME] = {
          ...pointTile,
          extent: typeof pointTile.extent === 'number' ? pointTile.extent : 4096,
          version: 2,
        };
      }
    }

    if (Object.keys(layers).length === 0) {
      return null;
    }

    const buffer = vtpbf.fromGeojsonVt(layers, { extent: 4096, version: 2 });
    this.tileCache.set(key, buffer);
    writeTileToRedis(cacheVersion, z, x, y, buffer);
    if (this.tileCache.size > TILE_CACHE_LIMIT) {
      const oldestKey = this.tileCache.keys().next().value;
      this.tileCache.delete(oldestKey);
    }
    return buffer;
  }

  async summarizeViewport({ west, south, east, north, zoom, filters }) {
    await this.ensureLoaded();
    const numericZoom = Number(zoom);
    if (!Number.isFinite(numericZoom) || numericZoom < SUMMARY_ZOOM_THRESHOLD) {
      return {
        zoomRestricted: true,
        topStreets: [],
      };
    }

    if (!this.summaryTree || !this.summaryPoints.length) {
      return {
        zoomRestricted: false,
        visibleCount: 0,
        visibleRevenue: 0,
        topStreets: [],
      };
    }

    const indices = this.summaryTree.search(west, south, east, north);
    let visibleCount = 0;
    let visibleRevenue = 0;
    const streetMap = new Map();
    const yearCountsMap = this.locationYearCounts || null;

    for (const index of indices) {
      const point = this.summaryPoints[index];
      if (!point) {
        continue;
      }
      if (filters?.year && !yearMaskIncludes(point.yearMask, filters.year)) {
        continue;
      }
      if (filters?.month && !monthMaskIncludes(point.monthMask, filters.month)) {
        continue;
      }
      let ticketCount = point.ticketCount;
      let totalRevenue = point.totalRevenue;
      if (filters?.year && yearCountsMap && point.location) {
        const locationEntry = yearCountsMap.get(point.location);
        if (locationEntry && locationEntry[filters.year]) {
          ticketCount = locationEntry[filters.year].ticketCount;
          totalRevenue = locationEntry[filters.year].totalRevenue;
        } else {
          ticketCount = 0;
          totalRevenue = 0;
        }
      }
      if (ticketCount <= 0) {
        continue;
      }

      visibleCount += ticketCount;
      visibleRevenue += totalRevenue;

      const streetName =
        Number.isInteger(point.streetId) ? this.streetTable[point.streetId] : null;
      const locationName =
        Number.isInteger(point.locationId) ? this.locationTable[point.locationId] : null;
      const streetKey = streetName || locationName || 'Unknown';
      if (!streetMap.has(streetKey)) {
        streetMap.set(streetKey, {
          name: streetKey,
          ticketCount: 0,
          totalRevenue: 0,
          sampleLocation: locationName || streetKey,
        });
      }
      const streetEntry = streetMap.get(streetKey);
      streetEntry.ticketCount += ticketCount;
      streetEntry.totalRevenue += totalRevenue;
    }

    const topStreets = Array.from(streetMap.values())
      .sort((a, b) => (b.totalRevenue || 0) - (a.totalRevenue || 0))
      .slice(0, SUMMARY_LIMIT)
      .map((entry) => ({
        ...entry,
        totalRevenue: Number(entry.totalRevenue.toFixed(2)),
      }));

    return {
      zoomRestricted: false,
      visibleCount,
      visibleRevenue: Number(visibleRevenue.toFixed(2)),
      topStreets,
    };
  }

  async getViewportPoints({ west, south, east, north, limit = 5000, filters }) {
    await this.ensureLoaded();
    if (!this.summaryTree || !this.summaryPoints.length) {
      return [];
    }

    const indices = this.summaryTree.search(west, south, east, north);
    if (!indices || indices.length === 0) {
      return [];
    }

    const maxPoints = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 20000) : 5000;
    const result = [];

    const yearCountsMap = this.locationYearCounts || null;

    for (let i = 0; i < indices.length; i += 1) {
      const point = this.summaryPoints[indices[i]];
      if (!point) {
        continue;
      }

      if (filters?.year && !yearMaskIncludes(point.yearMask, filters.year)) {
        continue;
      }
      if (filters?.month && !monthMaskIncludes(point.monthMask, filters.month)) {
        continue;
      }

      let ticketCount = point.ticketCount;
      if (filters?.year && yearCountsMap && point.location) {
        const locationEntry = yearCountsMap.get(point.location);
        if (locationEntry && locationEntry[filters.year]) {
          ticketCount = locationEntry[filters.year].ticketCount;
        } else {
          ticketCount = 0;
        }
      }
      if (ticketCount <= 0) {
        continue;
      }

      result.push({
        longitude: point.longitude,
        latitude: point.latitude,
        count: ticketCount,
      });

      if (result.length >= maxPoints) {
        break;
      }
    }

    return result;
  }

  async getClusterExpansionZoom(clusterId) {
    await this.ensureLoaded();
    if (!this.clusterIndex || !Number.isFinite(clusterId)) {
      return null;
    }
    try {
      return this.clusterIndex.getClusterExpansionZoom(clusterId);
    } catch (error) {
      console.warn('Failed to resolve cluster expansion zoom', error.message);
      return null;
    }
  }
}

export function createTileService() {
  // Use DATA_DIR from environment (set by index.js)
  const dataDir = process.env.DATA_DIR;
  if (!dataDir) {
    throw new Error('DATA_DIR environment variable must be set');
  }
  return new TileService();
}
