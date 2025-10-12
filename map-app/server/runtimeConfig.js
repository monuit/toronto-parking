import path from 'path';
import process from 'node:process';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '../..');

const DOTENV_PATH = path.join(PROJECT_ROOT, '.env');
dotenv.config({ path: DOTENV_PATH });

const DEFAULT_PORT = 5173;

function parsePort(value) {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) ? parsed : DEFAULT_PORT;
}

function isForcedLocal() {
  return process.env.FORCE_LOCAL_CACHE === '1' || process.env.FORCE_LOCAL_DB === '1';
}

function isForcedRemote() {
  return process.env.FORCE_REMOTE_CACHE === '1' || process.env.FORCE_REMOTE_DB === '1';
}

export function isLocalDevServer() {
  if (isForcedRemote()) {
    return false;
  }
  if (isForcedLocal()) {
    return true;
  }
  const nodeEnv = process.env.NODE_ENV ?? 'development';
  const port = parsePort(process.env.PORT);
  const host = process.env.HOST || process.env.HOSTNAME || '';
  const isLocalHost = !host || host.includes('localhost') || host === '127.0.0.1';
  return nodeEnv !== 'production' && port === DEFAULT_PORT && isLocalHost;
}

export function getRedisConfig() {
  const url =
    process.env.REDIS_URL || process.env.REDIS_PUBLIC_URL || process.env.REDIS_CONNECTION || null;
  if (!url) {
    return { enabled: false, url: null };
  }
  const enabled = !isLocalDevServer();
  return {
    enabled,
    url: enabled ? url : null,
    rawUrl: url,
  };
}

export function getPostgresConfig() {
  const connectionString =
    process.env.DATABASE_URL ||
    process.env.DATABASE_PUBLIC_URL ||
    process.env.POSTGRES_URL ||
    null;
  const replicaConnectionString =
    process.env.DATABASE_REPLICA_URL ||
    process.env.DATABASE_RO_URL ||
    process.env.POSTGRES_READONLY_URL ||
    null;
  const poolConnectionString =
    process.env.DATABASE_POOL_URL ||
    process.env.PGBOUNCER_URL ||
    null;
  if (!connectionString) {
    return {
      enabled: false,
      connectionString: null,
      readOnlyConnectionString: null,
      poolConnectionString: null,
    };
  }
  const forceDisable = process.env.FORCE_LOCAL_DB === '1';
  const disableInDev = process.env.DISABLE_DB_IN_DEV === '1';
  let enabled = true;
  if (forceDisable) {
    enabled = false;
  } else if (isLocalDevServer()) {
    enabled = !disableInDev;
  }
  const sslRequired =
    process.env.DATABASE_SSL === '1' ||
    process.env.PGSSLMODE === 'require' ||
    connectionString.includes('railway');
  const defaultRejectUnauthorized = connectionString.includes('railway') ? false : true;
  const rejectEnv = process.env.DATABASE_SSL_REJECT_UNAUTHORIZED;
  const rejectUnauthorized =
    rejectEnv !== undefined ? rejectEnv !== '0' : defaultRejectUnauthorized;
  const sslOptions = sslRequired ? { rejectUnauthorized } : undefined;
  return {
    enabled,
    connectionString: enabled ? connectionString : null,
    rawConnectionString: connectionString,
    readOnlyConnectionString: enabled ? (replicaConnectionString || poolConnectionString || connectionString) : null,
    poolConnectionString: enabled ? (poolConnectionString || null) : null,
    replicaConnectionString: enabled ? (replicaConnectionString || null) : null,
    ssl: sslOptions,
  };
}

function parseJsonConfig(rawValue, fallback) {
  if (!rawValue) {
    return fallback;
  }
  try {
    const parsed = JSON.parse(rawValue);
    return parsed && typeof parsed === 'object' ? parsed : fallback;
  } catch (error) {
    console.warn('Failed to parse PMTiles JSON config, using defaults:', error);
    return fallback;
  }
}

function parseZoomList(raw) {
  if (!raw) {
    return [10, 11, 12, 13];
  }
  return raw
    .split(',')
    .map((segment) => Number.parseInt(segment.trim(), 10))
    .filter((value) => Number.isFinite(value));
}

function normalizePrefix(raw, defaultValue = 'pmtiles') {
  if (raw === undefined || raw === null) {
    return defaultValue;
  }
  const trimmed = String(raw).trim();
  if (!trimmed) {
    return '';
  }
  return trimmed.replace(/^\/+/, '').replace(/\/+$/, '');
}

function ensureBucketPath(url, bucket) {
  if (!url || !bucket) {
    return url;
  }
  try {
    const parsed = new URL(url);
    const hostLower = parsed.hostname.toLowerCase();
    const bucketLower = bucket.toLowerCase();
    const hostHasBucket = hostLower === bucketLower || hostLower.startsWith(`${bucketLower}.`);
    if (!hostHasBucket) {
      const segments = parsed.pathname.split('/').filter(Boolean);
      const pathHasBucket = segments.some((segment) => segment.toLowerCase() === bucketLower);
      if (!pathHasBucket) {
        segments.push(bucket);
        parsed.pathname = `/${segments.join('/')}`;
      }
    }
    return parsed.toString().replace(/\/+$/, '');
  } catch {
    const normalized = url.replace(/\/+$/, '');
    const suffix = `/${bucket}`;
    if (normalized.endsWith(suffix) || normalized.toLowerCase().endsWith(suffix.toLowerCase())) {
      return normalized;
    }
    return `${normalized}/${bucket}`.replace(/\/+/, '/');
  }
}

export function getPmtilesRuntimeConfig() {
  const basePublic = process.env.PMTILES_PUBLIC_BASE_URL
    || (process.env.MINIO_PUBLIC_ENDPOINT ? process.env.MINIO_PUBLIC_ENDPOINT.replace(/\/?$/, '') : null);
  const basePrivate = process.env.PMTILES_PRIVATE_BASE_URL
    || (process.env.MINIO_PRIVATE_ENDPOINT ? process.env.MINIO_PRIVATE_ENDPOINT.replace(/\/?$/, '') : null);
  const cdnBase = process.env.PMTILES_CDN_BASE_URL
    ? process.env.PMTILES_CDN_BASE_URL.replace(/\/?$/, '')
    : null;
  const bucket = process.env.PMTILES_BUCKET || 'pmtiles';
  const region = process.env.MINIO_REGION || 'us-east-1';
  const objectPrefix = normalizePrefix(process.env.PMTILES_PREFIX);
  const warmupMinutes = Number.parseInt(process.env.PMTILES_WARMUP_MINUTES || '60', 10);
  const warmupIntervalMs = Number.isFinite(warmupMinutes) && warmupMinutes > 0
    ? warmupMinutes * 60 * 1000
    : 60 * 60 * 1000;
  const warmupZooms = parseZoomList(process.env.PMTILES_WARMUP_ZOOMS);
  const warmupLongitude = Number.parseFloat(process.env.PMTILES_WARMUP_LONGITUDE || '-79.3832');
  const warmupLatitude = Number.parseFloat(process.env.PMTILES_WARMUP_LATITUDE || '43.6532');
  const datasetOverrides = parseJsonConfig(process.env.PMTILES_DATASETS, null);
  const wardDatasetOverrides = parseJsonConfig(process.env.PMTILES_WARD_DATASETS, null);

  const enabled = Boolean(basePublic);

  const publicBaseUrl = !process.env.PMTILES_PUBLIC_BASE_URL && basePublic
    ? ensureBucketPath(basePublic, bucket)
    : (basePublic ? basePublic.replace(/\/+$/, '') : null);
  const privateBaseUrl = !process.env.PMTILES_PRIVATE_BASE_URL && basePrivate
    ? ensureBucketPath(basePrivate, bucket)
    : (basePrivate ? basePrivate.replace(/\/+$/, '') : null);
  const cdnBaseUrl = cdnBase ? ensureBucketPath(cdnBase, bucket) : null;

  return {
    enabled,
    publicBaseUrl,
    privateBaseUrl,
    cdnBaseUrl,
    bucket,
    region,
    objectPrefix,
    warmupIntervalMs,
    warmupZooms: warmupZooms.length > 0 ? warmupZooms : [10, 11, 12, 13],
    warmupCenter: [warmupLongitude, warmupLatitude],
    datasetOverrides,
    wardDatasetOverrides,
  };
}

export function getPmtilesBuildConfig() {
  const stream = process.env.PMTILES_BUILD_STREAM || 'pmtiles:build:requests';
  const failureStream = process.env.PMTILES_BUILD_FAILURE_STREAM || 'pmtiles:build:failures';
  const progressPrefix = process.env.PMTILES_BUILD_PROGRESS_PREFIX || 'pmtiles:build:progress';
  const consumerGroup = process.env.PMTILES_BUILD_CONSUMER_GROUP || 'pmtiles-workers';
  const stagingDir = process.env.PMTILES_STAGING_DIR || path.resolve(PROJECT_ROOT, '..', 'pmtiles', 'staging');
  const maxAttempts = Number.parseInt(process.env.PMTILES_BUILD_MAX_ATTEMPTS || '5', 10);
  const batchCount = Number.parseInt(process.env.PMTILES_BUILD_BATCH_COUNT || '10', 10);
  const rebuildInterval = Number.parseInt(process.env.PMTILES_REBUILD_INTERVAL || '500', 10);
  const uploadOnRebuild = process.env.PMTILES_REBUILD_UPLOAD === '1';

  return {
    stream,
    failureStream,
    progressPrefix,
    consumerGroup,
    stagingDir,
    maxAttempts: Number.isFinite(maxAttempts) && maxAttempts > 0 ? maxAttempts : 5,
    batchCount: Number.isFinite(batchCount) && batchCount > 0 ? batchCount : 10,
    rebuildInterval: Number.isFinite(rebuildInterval) && rebuildInterval > 0 ? rebuildInterval : 500,
    uploadOnRebuild,
  };
}
export function getDataDir(defaultDir) {
  return process.env.DATA_DIR || defaultDir;
}

export const runtimeInfo = {
  projectRoot: PROJECT_ROOT,
  dotenvPath: DOTENV_PATH,
};
