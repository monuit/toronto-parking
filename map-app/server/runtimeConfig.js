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
    process.env.DATABASE_PUBLIC_URL ||
    process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    null;
  if (!connectionString) {
    return { enabled: false, connectionString: null };
  }
  const enabled = !isLocalDevServer();
  const sslRequired =
    process.env.DATABASE_SSL === '1' ||
    process.env.PGSSLMODE === 'require' ||
    connectionString.includes('railway');
  const sslOptions = sslRequired
    ? { rejectUnauthorized: process.env.DATABASE_SSL_REJECT_UNAUTHORIZED !== '0' }
    : undefined;
  return {
    enabled,
    connectionString: enabled ? connectionString : null,
    rawConnectionString: connectionString,
    ssl: sslOptions,
  };
}

export function getDataDir(defaultDir) {
  return process.env.DATA_DIR || defaultDir;
}

export const runtimeInfo = {
  projectRoot: PROJECT_ROOT,
  dotenvPath: DOTENV_PATH,
};
