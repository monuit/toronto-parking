import process from 'node:process';
import { setTimeout as sleep } from 'node:timers/promises';
import { createClient } from 'redis';
import { getRedisConfig, getPostgresConfig } from './runtimeConfig.js';

const DEFAULT_ATTEMPTS = Number.parseInt(process.env.REMOTE_WAKE_ATTEMPTS || '6', 10);
const DEFAULT_INITIAL_DELAY = Number.parseInt(process.env.REMOTE_WAKE_DELAY_MS || '500', 10);
const MAX_DELAY = Number.parseInt(process.env.REMOTE_WAKE_MAX_DELAY_MS || '5000', 10);

function resolveAttempts(customAttempts) {
  const parsed = Number.parseInt(customAttempts ?? '', 10);
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return DEFAULT_ATTEMPTS;
}

function resolveInitialDelay(customDelay) {
  const parsed = Number.parseInt(customDelay ?? '', 10);
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return DEFAULT_INITIAL_DELAY;
}

async function wakeRedis({ attempts, initialDelay }) {
  const redisConfig = getRedisConfig();
  if (!redisConfig.enabled || !redisConfig.url) {
    return { enabled: false, awake: false };
  }

  const totalAttempts = resolveAttempts(attempts);
  let delay = resolveInitialDelay(initialDelay);

  for (let attempt = 1; attempt <= totalAttempts; attempt += 1) {
    const client = createClient({ url: redisConfig.url });
    client.on('error', (error) => {
      console.warn('Redis wake error:', error.message);
    });
    try {
      await client.connect();
      await client.ping();
      await client.disconnect();
      console.log(`🔌 Redis awake after ${attempt} attempt${attempt === 1 ? '' : 's'}.`);
      return { enabled: true, awake: true };
    } catch (error) {
      console.warn(`Redis wake attempt ${attempt} failed: ${error.message}`);
      try {
        await client.disconnect();
      } catch {
        // ignore disconnect errors
      }
      if (attempt >= totalAttempts) {
        return { enabled: true, awake: false };
      }
      await sleep(delay);
      delay = Math.min(delay * 2, MAX_DELAY);
    }
  }
  return { enabled: true, awake: false };
}

async function wakePostgres({ attempts, initialDelay }) {
  const postgresConfig = getPostgresConfig();
  if (!postgresConfig.enabled || !postgresConfig.connectionString) {
    return { enabled: false, awake: false };
  }

  const totalAttempts = resolveAttempts(attempts);
  let delay = resolveInitialDelay(initialDelay);

  for (let attempt = 1; attempt <= totalAttempts; attempt += 1) {
    let pool;
    try {
      const { Pool } = await import('pg');
      pool = new Pool({
        connectionString: postgresConfig.connectionString,
        ssl: postgresConfig.ssl,
        max: 1,
        idleTimeoutMillis: 1000,
        connectionTimeoutMillis: 3000,
      });
      await pool.query('SELECT 1');
      await pool.end();
      console.log(`🔌 Postgres awake after ${attempt} attempt${attempt === 1 ? '' : 's'}.`);
      return { enabled: true, awake: true };
    } catch (error) {
      console.warn(`Postgres wake attempt ${attempt} failed: ${error.message}`);
      if (pool) {
        try {
          await pool.end();
        } catch {
          // ignore pool end errors
        }
      }
      if (attempt >= totalAttempts) {
        return { enabled: true, awake: false };
      }
      await sleep(delay);
      delay = Math.min(delay * 2, MAX_DELAY);
    }
  }

  return { enabled: true, awake: false };
}

export async function wakeRemoteServices(options = {}) {
  const attempts = resolveAttempts(options.attempts);
  const initialDelay = resolveInitialDelay(options.initialDelay);

  const [redisResult, postgresResult] = await Promise.all([
    wakeRedis({ attempts, initialDelay }),
    wakePostgres({ attempts, initialDelay }),
  ]);

  if (redisResult.enabled && !redisResult.awake) {
    console.warn('Redis did not wake after retries; proceeding with local fallbacks.');
  }
  if (postgresResult.enabled && !postgresResult.awake) {
    console.warn('Postgres did not wake after retries; API fallbacks may be used.');
  }

  return {
    redis: redisResult,
    postgres: postgresResult,
  };
}

export async function wakeRedisOnly(options = {}) {
  return wakeRedis({ attempts: options.attempts, initialDelay: options.initialDelay });
}

export async function wakePostgresOnly(options = {}) {
  return wakePostgres({ attempts: options.attempts, initialDelay: options.initialDelay });
}
