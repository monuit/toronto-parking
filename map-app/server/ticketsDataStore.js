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
const CHUNK_PREFIX = 'tickets_aggregated_part';

const redisSettings = getRedisConfig();
const REDIS_URL = redisSettings.url;
const REDIS_ENABLED = redisSettings.enabled && !!REDIS_URL;
const REDIS_NAMESPACE = process.env.MAP_DATA_REDIS_NAMESPACE || 'toronto:map-data';
const REDIS_KEY = `${REDIS_NAMESPACE}:tickets:aggregated:v1`;
const REDIS_MANIFEST_KEY = `${REDIS_NAMESPACE}:tickets:aggregated:v1:chunks`;
const CHUNK_KEY_PREFIX = `${REDIS_NAMESPACE}:tickets:aggregated:v1:chunk:`;
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

export { TICKETS_FILE };
