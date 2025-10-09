import { readdir, readFile, writeFile, stat } from 'fs/promises';
import { Buffer } from 'node:buffer';
import path from 'path';
import process from 'node:process';
import { fileURLToPath } from 'url';
import { createClient } from 'redis';
import { gzipSync, gunzipSync } from 'node:zlib';
import { getRedisConfig } from './runtimeConfig.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../public/data');
const TICKETS_FILE = path.join(DATA_DIR, 'tickets_aggregated.geojson');
const SUMMARY_FILE = path.join(DATA_DIR, 'tickets_summary.json');
const STREET_STATS_FILE = path.join(DATA_DIR, 'street_stats.json');
const NEIGHBOURHOOD_STATS_FILE = path.join(DATA_DIR, 'neighbourhood_stats.json');
const RED_LIGHT_SUMMARY_FILE = path.join(DATA_DIR, 'red_light_summary.json');
const ASE_SUMMARY_FILE = path.join(DATA_DIR, 'ase_summary.json');
const RED_LIGHT_GEOJSON_FILE = path.join(DATA_DIR, 'red_light_locations.geojson');
const ASE_GEOJSON_FILE = path.join(DATA_DIR, 'ase_locations.geojson');
const RED_LIGHT_GLOW_FILE = path.join(DATA_DIR, 'red_light_glow_lines.geojson');
const ASE_GLOW_FILE = path.join(DATA_DIR, 'ase_glow_lines.geojson');
const RED_LIGHT_WARD_GEOJSON_FILE = path.join(DATA_DIR, 'red_light_ward_choropleth.geojson');
const ASE_WARD_GEOJSON_FILE = path.join(DATA_DIR, 'ase_ward_choropleth.geojson');
const COMBINED_WARD_GEOJSON_FILE = path.join(DATA_DIR, 'cameras_combined_ward_choropleth.geojson');
const RED_LIGHT_WARD_SUMMARY_FILE = path.join(DATA_DIR, 'red_light_ward_summary.json');
const ASE_WARD_SUMMARY_FILE = path.join(DATA_DIR, 'ase_ward_summary.json');
const COMBINED_WARD_SUMMARY_FILE = path.join(DATA_DIR, 'cameras_combined_ward_summary.json');
const CHUNK_PREFIX = 'tickets_aggregated_part';

const redisSettings = getRedisConfig();
const REDIS_URL = redisSettings.url;
const REDIS_ENABLED = redisSettings.enabled && !!REDIS_URL;
const REDIS_NAMESPACE = process.env.MAP_DATA_REDIS_NAMESPACE || 'toronto:map-data';
const REDIS_KEY = `${REDIS_NAMESPACE}:tickets:aggregated:v1`;
const REDIS_MANIFEST_KEY = `${REDIS_NAMESPACE}:tickets:aggregated:v1:chunks`;
const CHUNK_KEY_PREFIX = `${REDIS_NAMESPACE}:tickets:aggregated:v1:chunk:`;
const REDIS_SUMMARY_KEY = `${REDIS_NAMESPACE}:tickets:summary:v1`;
const REDIS_STREET_STATS_KEY = `${REDIS_NAMESPACE}:tickets:street-stats:v1`;
const REDIS_NEIGHBOURHOOD_STATS_KEY = `${REDIS_NAMESPACE}:tickets:neighbourhood-stats:v1`;
const REDIS_CAMERA_SUMMARY_KEYS = {
  red_light_locations: `${REDIS_NAMESPACE}:red_light_locations:summary:v1`,
  ase_locations: `${REDIS_NAMESPACE}:ase_locations:summary:v1`,
};
const REDIS_CAMERA_GLOW_KEYS = {
  red_light_locations: `${REDIS_NAMESPACE}:red_light_locations:glow:v1`,
  ase_locations: `${REDIS_NAMESPACE}:ase_locations:glow:v1`,
};
const REDIS_CAMERA_LOCATION_KEYS = {
  red_light_locations: `${REDIS_NAMESPACE}:red_light_locations:locations:v1`,
  ase_locations: `${REDIS_NAMESPACE}:ase_locations:locations:v1`,
};
const REDIS_CAMERA_WARD_GEO_KEYS = {
  red_light_locations: `${REDIS_NAMESPACE}:red_light:wards:geojson:v1`,
  ase_locations: `${REDIS_NAMESPACE}:ase:wards:geojson:v1`,
  cameras_combined: `${REDIS_NAMESPACE}:cameras:wards:geojson:v1`,
};
const REDIS_CAMERA_WARD_SUMMARY_KEYS = {
  red_light_locations: `${REDIS_NAMESPACE}:red_light:wards:summary:v1`,
  ase_locations: `${REDIS_NAMESPACE}:ase:wards:summary:v1`,
  cameras_combined: `${REDIS_NAMESPACE}:cameras:wards:summary:v1`,
};
const REDIS_TTL_SECONDS = Number.parseInt(process.env.MAP_DATA_REDIS_TTL || '86400', 10);

let redisClientPromise = null;
let cachedManifest = null;
let cachedManifestVersion = null;

async function getRedisClient() {
  if (!REDIS_ENABLED) {
    return null;
  }
  if (redisClientPromise) {
    try {
      const existing = await redisClientPromise;
      if (existing && existing.isOpen) {
        return existing;
      }
    } catch (error) {
      console.warn('Previous Redis connection attempt failed:', error.message);
    }
    redisClientPromise = null;
  }

  redisClientPromise = (async () => {
    const client = createClient({ url: REDIS_URL });
    const reset = () => {
      if (redisClientPromise) {
        redisClientPromise = null;
      }
    };
    client.on('error', (error) => {
      console.warn('Redis client error:', error.message);
    });
    client.on('end', reset);
    client.on('close', reset);
    try {
      await client.connect();
      return client;
    } catch (error) {
      reset();
      console.warn('Failed to connect to Redis, continuing without cache:', error.message);
      try {
        await client.disconnect();
      } catch (disconnectError) {
        console.warn('Error while closing Redis client after failed connection:', disconnectError.message);
      }
      return null;
    }
  })();

  const client = await redisClientPromise;
  return client && client.isOpen ? client : null;
}

function compress(raw) {
  return gzipSync(Buffer.from(raw, 'utf-8')).toString('base64');
}

function decompress(encoded) {
  return gunzipSync(Buffer.from(encoded, 'base64')).toString('utf-8');
}

async function readJsonFileWithMeta(filePath) {
  try {
    const raw = await readFile(filePath, 'utf-8');
    const json = JSON.parse(raw);
    const { mtimeMs } = await stat(filePath);
    const version = Math.trunc(mtimeMs);
    return {
      data: json,
      version,
      source: filePath,
      etag: `W/"${version}"`,
    };
  } catch {
    return null;
  }
}

async function readCameraSummaryFromDisk(dataset) {
  if (dataset === 'red_light_locations') {
    return readJsonFileWithMeta(RED_LIGHT_SUMMARY_FILE);
  }
  if (dataset === 'ase_locations') {
    return readJsonFileWithMeta(ASE_SUMMARY_FILE);
  }
  return null;
}

function resolveGlowFile(dataset) {
  if (dataset === 'red_light_locations') {
    return RED_LIGHT_GLOW_FILE;
  }
  if (dataset === 'ase_locations') {
    return ASE_GLOW_FILE;
  }
  return null;
}

async function readCameraGlowFromDisk(dataset) {
  const filePath = resolveGlowFile(dataset);
  if (!filePath) {
    return null;
  }
  try {
    const raw = await readFile(filePath, 'utf-8');
    const { mtimeMs } = await stat(filePath);
    const version = Math.trunc(mtimeMs);
    return {
      raw,
      version,
      source: filePath,
      etag: `W/"${version}"`,
    };
  } catch (error) {
    console.warn(`Failed to read glow file for ${dataset}:`, error.message);
    return null;
  }
}

function resolveCameraLocationsFile(dataset) {
  if (dataset === 'red_light_locations') {
    return RED_LIGHT_GEOJSON_FILE;
  }
  if (dataset === 'ase_locations') {
    return ASE_GEOJSON_FILE;
  }
  return null;
}

function resolveWardGeojsonFile(dataset) {
  if (dataset === 'red_light_locations') {
    return RED_LIGHT_WARD_GEOJSON_FILE;
  }
  if (dataset === 'ase_locations') {
    return ASE_WARD_GEOJSON_FILE;
  }
  if (dataset === 'cameras_combined') {
    return COMBINED_WARD_GEOJSON_FILE;
  }
  return null;
}

function resolveWardSummaryFile(dataset) {
  if (dataset === 'red_light_locations') {
    return RED_LIGHT_WARD_SUMMARY_FILE;
  }
  if (dataset === 'ase_locations') {
    return ASE_WARD_SUMMARY_FILE;
  }
  if (dataset === 'cameras_combined') {
    return COMBINED_WARD_SUMMARY_FILE;
  }
  return null;
}

async function readCameraLocationsFromDisk(dataset) {
  const filePath = resolveCameraLocationsFile(dataset);
  if (!filePath) {
    return null;
  }
  try {
    const raw = await readFile(filePath, 'utf-8');
    const { mtimeMs } = await stat(filePath);
    return {
      raw,
      version: Math.trunc(mtimeMs),
      source: filePath,
    };
  } catch (error) {
    console.warn(`Failed to read camera locations for ${dataset}:`, error.message);
    return null;
  }
}

async function readWardGeojsonFromDisk(dataset) {
  const filePath = resolveWardGeojsonFile(dataset);
  if (!filePath) {
    return null;
  }
  try {
    const raw = await readFile(filePath, 'utf-8');
    const { mtimeMs } = await stat(filePath);
    let data = null;
    try {
      data = JSON.parse(raw);
    } catch (error) {
      console.warn(`Failed to parse ward geojson for ${dataset} from disk:`, error.message);
    }
    const version = Math.trunc(mtimeMs);
    return {
      raw,
      data,
      version,
      source: filePath,
      etag: `W/"${version}"`,
    };
  } catch (error) {
    console.warn(`Failed to read ward geojson for ${dataset}:`, error.message);
    return null;
  }
}

async function readWardSummaryFromDisk(dataset) {
  const filePath = resolveWardSummaryFile(dataset);
  if (!filePath) {
    return null;
  }
  return readJsonFileWithMeta(filePath);
}

function resolveNumericVersion(payload) {
  if (!payload) {
    return null;
  }
  const value = payload.version;
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  if (payload.updatedAt) {
    const parsedDate = Date.parse(payload.updatedAt);
    if (Number.isFinite(parsedDate)) {
      return parsedDate;
    }
  }
  return null;
}

function hasWardFeatures(payload) {
  const features = payload?.data?.features;
  return Array.isArray(features) && features.length > 0;
}

function normaliseRedisWrapper(payload, fallbackSource) {
  if (!payload) {
    return null;
  }
  const versionFromPayload = Number.isFinite(payload.version) ? Number(payload.version) : null;
  const parsedUpdatedAt = payload.updatedAt ? Date.parse(payload.updatedAt) : null;
  const version = versionFromPayload ?? (Number.isFinite(parsedUpdatedAt) ? parsedUpdatedAt : null);
  const etag = typeof payload.etag === 'string'
    ? payload.etag
    : (version !== null ? `W/"${version}"` : null);
  let data = payload.data ?? null;
  if (typeof data === 'string' && payload.encoding === 'gzip+base64') {
    try {
      data = JSON.parse(decompress(data));
    } catch (error) {
      console.warn('Failed to parse compressed Redis payload:', error.message);
      return null;
    }
  }
  if (data === null || data === undefined) {
    return null;
  }
  return {
    data,
    version,
    source: fallbackSource || 'redis',
    etag,
  };
}

async function readAggregateFromRedis() {
  const client = await getRedisClient();
  if (!client) {
    return null;
  }
  try {
    const stored = await client.get(REDIS_KEY);
    if (!stored) {
      return null;
    }
    const payload = JSON.parse(stored);
    if (!payload || typeof payload.raw !== 'string') {
      return null;
    }
    const raw = decompress(payload.raw);
    return {
      raw,
      version: payload.version || null,
      source: 'redis',
    };
  } catch (error) {
    console.warn('Failed to read tickets data from Redis:', error.message);
    return null;
  }
}

async function readJsonWrapperFromRedis(key) {
  if (!REDIS_ENABLED) {
    return null;
  }
  const client = await getRedisClient();
  if (!client) {
    return null;
  }
  try {
    const payload = await client.get(key);
    if (!payload) {
      return null;
    }
    const parsed = JSON.parse(payload);
    return normaliseRedisWrapper(parsed, 'redis');
  } catch (error) {
    console.warn(`Failed to read JSON blob from Redis (${key}):`, error.message);
    return null;
  }
}

async function readRawWrapperFromRedis(key) {
  if (!REDIS_ENABLED) {
    return null;
  }
  const client = await getRedisClient();
  if (!client) {
    return null;
  }
  try {
    const payload = await client.get(key);
    if (!payload) {
      return null;
    }
    const parsed = JSON.parse(payload);
    if (!parsed || typeof parsed.raw !== 'string') {
      return null;
    }
    const raw = decompress(parsed.raw);
    const versionFromPayload = Number.isFinite(parsed.version) ? Number(parsed.version) : null;
    const parsedUpdatedAt = parsed.updatedAt ? Date.parse(parsed.updatedAt) : null;
    const version = versionFromPayload ?? (Number.isFinite(parsedUpdatedAt) ? parsedUpdatedAt : null);
    return {
      raw,
      version,
      source: 'redis',
      etag: typeof parsed.etag === 'string' ? parsed.etag : (version !== null ? `W/"${version}"` : null),
    };
  } catch (error) {
    console.warn(`Failed to read raw blob from Redis (${key}):`, error.message);
    return null;
  }
}

async function readCameraSummaryFromRedis(dataset) {
  const key = REDIS_CAMERA_SUMMARY_KEYS[dataset];
  if (!key) {
    return null;
  }
  return readJsonWrapperFromRedis(key);
}

async function readWardSummaryFromRedis(dataset) {
  const key = REDIS_CAMERA_WARD_SUMMARY_KEYS[dataset];
  if (!key) {
    return null;
  }
  return readJsonWrapperFromRedis(key);
}

async function readCameraGlowFromRedis(dataset) {
  const key = REDIS_CAMERA_GLOW_KEYS[dataset];
  if (!key) {
    return null;
  }
  return readRawWrapperFromRedis(key);
}

async function readCameraLocationsFromRedis(dataset) {
  const key = REDIS_CAMERA_LOCATION_KEYS[dataset];
  if (!key) {
    return null;
  }
  return readRawWrapperFromRedis(key);
}

async function readWardGeojsonFromRedis(dataset) {
  const key = REDIS_CAMERA_WARD_GEO_KEYS[dataset];
  if (!key) {
    return null;
  }
  return readJsonWrapperFromRedis(key);
}

async function writeAggregateToRedis(raw, version) {
  if (!REDIS_ENABLED) {
    return;
  }
  const client = await getRedisClient();
  if (!client) {
    return;
  }
  try {
    const payload = {
      version: version || Date.now(),
      updatedAt: new Date().toISOString(),
      raw: compress(raw),
    };
    const options = {};
    if (Number.isFinite(REDIS_TTL_SECONDS) && REDIS_TTL_SECONDS > 0) {
      options.EX = REDIS_TTL_SECONDS;
    }
    await client.set(REDIS_KEY, JSON.stringify(payload), options);
  } catch (error) {
    console.warn('Failed to cache tickets data in Redis:', error.message);
  }
}

async function readAggregateFromDisk() {
  const raw = await readFile(TICKETS_FILE, 'utf-8');
  const stats = await stat(TICKETS_FILE);
  return {
    raw,
    version: Math.trunc(stats.mtimeMs),
    source: 'disk',
  };
}

async function fetchAggregateResource() {
  const cached = await readAggregateFromRedis();
  if (cached) {
    return cached;
  }
  const resource = await readAggregateFromDisk();
  await writeAggregateToRedis(resource.raw, resource.version);
  return resource;
}

async function readChunkManifestFromRedis() {
  if (!REDIS_ENABLED) {
    return null;
  }
  const client = await getRedisClient();
  if (!client) {
    return null;
  }
  try {
    const payload = await client.get(REDIS_MANIFEST_KEY);
    if (!payload) {
      return null;
    }
    const parsed = JSON.parse(payload);
    if (!parsed || !Array.isArray(parsed.chunks)) {
      return null;
    }
    return parsed;
  } catch (error) {
    console.warn('Failed to read tickets chunk manifest from Redis:', error.message);
    return null;
  }
}

async function readChunkFromRedis(key) {
  if (!REDIS_ENABLED) {
    return null;
  }
  const client = await getRedisClient();
  if (!client) {
    return null;
  }
  try {
    const payload = await client.get(key);
    if (!payload) {
      return null;
    }
    const parsed = JSON.parse(payload);
    if (!parsed || typeof parsed.raw !== 'string') {
      return null;
    }
    const raw = decompress(parsed.raw);
    const json = JSON.parse(raw);
    const features = Array.isArray(json?.features) ? json.features : [];
    return {
      features,
      featureCount: parsed.featureCount ?? features.length,
      source: parsed.source || 'redis',
      version: parsed.version || null,
      neighbourhood: parsed.neighbourhood || null,
      slug: parsed.slug || null,
    };
  } catch (error) {
    console.warn(`Failed to read tickets chunk ${key} from Redis:`, error.message);
    return null;
  }
}

async function listChunkFiles() {
  try {
    const entries = await readdir(DATA_DIR, { withFileTypes: true });
    return entries
      .filter((entry) => entry.isFile() && entry.name.startsWith(CHUNK_PREFIX) && entry.name.endsWith('.geojson'))
      .map((entry) => path.join(DATA_DIR, entry.name))
      .sort();
  } catch (error) {
    console.warn('Failed to list chunk files:', error.message);
    return [];
  }
}

async function readChunkFromDisk(chunkPath) {
  try {
    const raw = await readFile(chunkPath, 'utf-8');
    const json = JSON.parse(raw);
    const features = Array.isArray(json?.features) ? json.features : [];
    const { mtimeMs } = await stat(chunkPath);
    return {
      features,
      featureCount: features.length,
      source: chunkPath,
      version: Math.trunc(mtimeMs),
      neighbourhood: null,
      slug: null,
    };
  } catch (error) {
    console.warn(`Failed to read chunk from disk (${chunkPath}):`, error.message);
    return {
      features: [],
      featureCount: 0,
      source: chunkPath,
      version: null,
    };
  }
}

async function resolveChunkManifest() {
  if (Array.isArray(cachedManifest) && cachedManifest.length > 0) {
    return { version: cachedManifestVersion, chunks: cachedManifest };
  }

  const manifest = await readChunkManifestFromRedis();
  if (manifest) {
    const version = manifest.updatedAt ? Date.parse(manifest.updatedAt) : Date.now();
    cachedManifestVersion = Number.isNaN(version) ? Date.now() : version;
    cachedManifest = manifest.chunks.map((chunk, index) => ({
      id: chunk.key || `${CHUNK_KEY_PREFIX}${index + 1}`,
      key: chunk.key || null,
      path: chunk.source || null,
      featureCount: chunk.featureCount ?? null,
      neighbourhood: chunk.neighbourhood || null,
      slug: chunk.slug || null,
    }));
    return { version: cachedManifestVersion, chunks: cachedManifest };
  }

  const files = await listChunkFiles();
  if (files.length > 0) {
    const stats = await stat(files[0]);
    cachedManifestVersion = Math.trunc(stats.mtimeMs);
    cachedManifest = files.map((file, index) => ({
      id: `file:${index + 1}`,
      key: null,
      path: file,
      featureCount: null,
    }));
    return { version: cachedManifestVersion, chunks: cachedManifest };
  }

  return { version: null, chunks: [] };
}

export async function getTicketChunks() {
  return resolveChunkManifest();
}

export async function loadTicketChunk(descriptor) {
  if (!descriptor) {
    return { features: [], featureCount: 0, source: null, version: null };
  }

  if (descriptor.key) {
    const chunk = await readChunkFromRedis(descriptor.key);
    if (chunk) {
      return chunk;
    }
  }

  if (descriptor.path) {
    return readChunkFromDisk(descriptor.path);
  }

  return {
    features: [],
    featureCount: 0,
    source: descriptor.key || null,
    version: null,
    neighbourhood: descriptor.neighbourhood || null,
    slug: descriptor.slug || null,
  };
}

export async function ensureTicketsFileFromRedis() {
  try {
    if (!REDIS_ENABLED) {
      return false;
    }
    const cached = await readAggregateFromRedis();
    if (!cached) {
      return false;
    }
    await writeFile(TICKETS_FILE, cached.raw, 'utf-8');
    return true;
  } catch (error) {
    console.warn('Failed to materialize tickets data from Redis:', error.message);
    return false;
  }
}

export async function storeTicketsRaw(raw, version) {
  await writeAggregateToRedis(raw, version);
  try {
    await writeFile(TICKETS_FILE, raw, 'utf-8');
  } catch (error) {
    console.warn('Failed to persist tickets data to disk:', error.message);
  }
}

export async function getTicketsRaw() {
  return fetchAggregateResource();
}

export function clearTicketsCache() {
  cachedManifest = null;
  cachedManifestVersion = null;
}

export async function loadDatasetSummary(dataset) {
  if (dataset === 'parking_tickets') {
    const redisResult = await readJsonWrapperFromRedis(REDIS_SUMMARY_KEY);
    if (redisResult) {
      return { ...redisResult, source: 'redis' };
    }
    const diskResult = await readJsonFileWithMeta(SUMMARY_FILE);
    if (diskResult) {
      return { ...diskResult, source: 'disk' };
    }
    return null;
  }

  if (dataset === 'ase_locations') {
    const redisResult = await readCameraSummaryFromRedis(dataset);
    if (redisResult?.data?.totals) {
      const wardSummary = await loadCameraWardSummary('ase_locations');
      const wardTotals = wardSummary?.data?.totals;
      if (wardTotals) {
        redisResult.data = {
          ...redisResult.data,
          totals: {
            locationCount: Number(wardTotals.locationCount ?? redisResult.data.totals.locationCount) || 0,
            ticketCount: Number(wardTotals.ticketCount ?? redisResult.data.totals.ticketCount) || 0,
            totalRevenue: Number(wardTotals.totalRevenue ?? redisResult.data.totals.totalRevenue) || 0,
          },
        };
      }
      return { ...redisResult, source: 'redis' };
    }

    const diskResult = await readCameraSummaryFromDisk(dataset);
    if (diskResult?.data?.totals) {
      const wardSummary = await loadCameraWardSummary('ase_locations');
      const wardTotals = wardSummary?.data?.totals;
      if (wardTotals) {
        diskResult.data = {
          ...diskResult.data,
          totals: {
            locationCount: Number(wardTotals.locationCount ?? diskResult.data.totals.locationCount) || 0,
            ticketCount: Number(wardTotals.ticketCount ?? diskResult.data.totals.ticketCount) || 0,
            totalRevenue: Number(wardTotals.totalRevenue ?? diskResult.data.totals.totalRevenue) || 0,
          },
        };
      }
      return { ...diskResult, source: 'disk' };
    }
  }

  const redisResult = await readCameraSummaryFromRedis(dataset);
  if (redisResult) {
    return { ...redisResult, source: 'redis' };
  }
  const diskResult = await readCameraSummaryFromDisk(dataset);
  if (diskResult) {
    return { ...diskResult, source: 'disk' };
  }
  return null;
}

export async function loadTicketsSummary() {
  return loadDatasetSummary('parking_tickets');
}

export async function loadCameraGlow(dataset) {
  if (dataset !== 'red_light_locations' && dataset !== 'ase_locations') {
    return null;
  }

  const redisResult = await readCameraGlowFromRedis(dataset);
  if (redisResult) {
    return { ...redisResult, source: 'redis' };
  }
  const diskResult = await readCameraGlowFromDisk(dataset);
  if (diskResult) {
    return { ...diskResult, source: 'disk' };
  }
  return null;
}

export async function loadCameraLocations(dataset) {
  if (dataset !== 'red_light_locations' && dataset !== 'ase_locations') {
    return null;
  }

  const redisResult = await readCameraLocationsFromRedis(dataset);
  if (redisResult) {
    return { ...redisResult, source: 'redis' };
  }
  const diskResult = await readCameraLocationsFromDisk(dataset);
  if (diskResult) {
    return { ...diskResult, source: 'disk' };
  }
  return null;
}

export async function loadCameraWardGeojson(dataset) {
  if (!REDIS_CAMERA_WARD_GEO_KEYS[dataset]) {
    return null;
  }

  const [redisResult, diskResult] = await Promise.all([
    readWardGeojsonFromRedis(dataset),
    readWardGeojsonFromDisk(dataset),
  ]);

  if (redisResult && diskResult) {
    const redisVersion = resolveNumericVersion(redisResult);
    const diskVersion = resolveNumericVersion(diskResult);
    const redisHasFeatures = hasWardFeatures(redisResult);
    const diskHasFeatures = hasWardFeatures(diskResult);

    if (diskHasFeatures && !redisHasFeatures) {
      return { ...diskResult, source: diskResult.source || 'disk' };
    }
    if (diskVersion !== null && redisVersion !== null) {
      if (diskVersion >= redisVersion) {
        return { ...diskResult, source: diskResult.source || 'disk' };
      }
      return { ...redisResult, source: 'redis' };
    }
    if (diskVersion !== null && redisVersion === null) {
      return { ...diskResult, source: diskResult.source || 'disk' };
    }
    if (redisVersion !== null && diskVersion === null) {
      return { ...redisResult, source: 'redis' };
    }
    return { ...redisResult, source: 'redis' };
  }

  if (redisResult) {
    return { ...redisResult, source: 'redis' };
  }
  if (diskResult) {
    return { ...diskResult, source: diskResult.source || 'disk' };
  }
  return null;
}

export async function loadCameraWardSummary(dataset) {
  if (!REDIS_CAMERA_WARD_SUMMARY_KEYS[dataset]) {
    return null;
  }

  const [redisResult, diskResult] = await Promise.all([
    readWardSummaryFromRedis(dataset),
    readWardSummaryFromDisk(dataset),
  ]);

  if (redisResult && diskResult) {
    const redisVersion = resolveNumericVersion(redisResult);
    const diskVersion = resolveNumericVersion(diskResult);
    if (diskVersion !== null && redisVersion !== null) {
      if (diskVersion >= redisVersion) {
        return { ...diskResult, source: diskResult.source || 'disk' };
      }
      return { ...redisResult, source: 'redis' };
    }
    if (diskVersion !== null && redisVersion === null) {
      return { ...diskResult, source: diskResult.source || 'disk' };
    }
    if (redisVersion !== null && diskVersion === null) {
      return { ...redisResult, source: 'redis' };
    }
    return { ...redisResult, source: 'redis' };
  }

  if (redisResult) {
    return { ...redisResult, source: 'redis' };
  }
  if (diskResult) {
    return { ...diskResult, source: diskResult.source || 'disk' };
  }
  return null;
}

export async function loadStreetStats() {
  const redisResult = await readJsonWrapperFromRedis(REDIS_STREET_STATS_KEY);
  if (redisResult) {
    return { ...redisResult, source: 'redis' };
  }
  const diskResult = await readJsonFileWithMeta(STREET_STATS_FILE);
  if (diskResult) {
    return { ...diskResult, source: 'disk' };
  }
  return null;
}

export async function loadNeighbourhoodStats() {
  const redisResult = await readJsonWrapperFromRedis(REDIS_NEIGHBOURHOOD_STATS_KEY);
  if (redisResult) {
    return { ...redisResult, source: 'redis' };
  }
  const diskResult = await readJsonFileWithMeta(NEIGHBOURHOOD_STATS_FILE);
  if (diskResult) {
    return { ...diskResult, source: 'disk' };
  }
  return null;
}

export { TICKETS_FILE };
