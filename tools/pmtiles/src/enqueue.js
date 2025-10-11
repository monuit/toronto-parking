#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import dotenv from 'dotenv';
import { createClient } from 'redis';
import { Pool } from 'pg';

import { getDatasetConfig } from '../../../shared/pmtiles/index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PACKAGE_ROOT = path.resolve(__dirname, '..');
const REPO_ROOT = path.resolve(PACKAGE_ROOT, '..', '..');

const envPath = path.join(REPO_ROOT, '.env');
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
}

const DATASET_SOURCES = {
  parking_tickets: {
    table: 'parking_tickets',
    geomColumn: 'geom_3857',
  },
  red_light_locations: {
    table: 'red_light_camera_locations',
    geomColumn: 'geom_3857',
  },
  ase_locations: {
    table: 'ase_camera_locations',
    geomColumn: 'geom_3857',
  },
};

function getEnv(name, fallback = undefined) {
  const value = process.env[name];
  return value !== undefined ? value : fallback;
}

function getBuildConfig() {
  const stream = getEnv('PMTILES_BUILD_STREAM', 'pmtiles:build:requests');
  const failureStream = getEnv('PMTILES_BUILD_FAILURE_STREAM', 'pmtiles:build:failures');
  const progressPrefix = getEnv('PMTILES_BUILD_PROGRESS_PREFIX', 'pmtiles:build:progress');
  const consumerGroup = getEnv('PMTILES_BUILD_CONSUMER_GROUP', 'pmtiles-workers');
  return {
    stream,
    failureStream,
    progressPrefix,
    consumerGroup,
  };
}

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    reset: false,
    datasets: null,
    shards: null,
  };
  for (const arg of args) {
    if (arg === '--reset') {
      options.reset = true;
    } else if (arg.startsWith('--datasets=')) {
      const value = arg.substring('--datasets='.length).trim();
      if (value) {
        options.datasets = new Set(value.split(',').map((entry) => entry.trim()).filter(Boolean));
      }
    } else if (arg.startsWith('--shards=')) {
      const value = arg.substring('--shards='.length).trim();
      if (value) {
        options.shards = new Set(value.split(',').map((entry) => entry.trim()).filter(Boolean));
      }
    } else {
      console.warn(`Unrecognised argument '${arg}' (ignored)`);
    }
  }
  return options;
}

function resolveDatabaseDsn() {
  const candidates = [
    getEnv('DATABASE_PRIVATE_URL'),
    getEnv('DATABASE_URL'),
    getEnv('POSTGRES_URL'),
    getEnv('DATABASE_PUBLIC_URL'),
  ];
  for (const candidate of candidates) {
    if (candidate) {
      return candidate;
    }
  }
  const host = getEnv('POSTGRES_HOST') || getEnv('PGHOST');
  const user = getEnv('POSTGRES_USER') || getEnv('PGUSER');
  const password = getEnv('POSTGRES_PASSWORD') || getEnv('PGPASSWORD') || '';
  const database = getEnv('POSTGRES_DB') || getEnv('POSTGRES_DATABASE') || getEnv('PGDATABASE') || 'postgres';
  const port = getEnv('POSTGRES_PORT') || getEnv('PGPORT') || '5432';
  if (host && user) {
    const auth = password
      ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}`
      : encodeURIComponent(user);
    return `postgresql://${auth}@${host}:${port}/${database}`;
  }
  throw new Error('Unable to resolve Postgres connection string. Set DATABASE_URL or POSTGRES_HOST/USER.');
}

function quadkeyToZxy(quadkey) {
  if (!quadkey) {
    throw new Error('Quadkey must be non-empty');
  }
  const z = quadkey.length;
  let x = 0;
  let y = 0;
  for (let i = 0; i < quadkey.length; i += 1) {
    const digit = Number.parseInt(quadkey[i], 10);
    const mask = 1 << (z - i - 1);
    if ((digit & 1) !== 0) {
      x |= mask;
    }
    if ((digit & 2) !== 0) {
      y |= mask;
    }
  }
  return { z, x, y };
}

async function collectTiles(pool, shard) {
  const source = DATASET_SOURCES[shard.dataset];
  if (!source) {
    throw new Error(`Dataset '${shard.dataset}' is not configured for tiling`);
  }

  const bounds = shard.bounds;
  const sql = `
    WITH bounds AS (
      SELECT ST_Transform(ST_SetSRID(ST_MakeEnvelope($1, $2, $3, $4), 4326), 3857) AS geom
    ), features AS (
      SELECT ${source.geomColumn} AS geom_3857
      FROM ${source.table}
      CROSS JOIN bounds
      WHERE ${source.geomColumn} IS NOT NULL
        AND ${source.geomColumn} && bounds.geom
    ), series AS (
      SELECT DISTINCT zoom, mercator_quadkey_prefix(geom_3857, zoom, zoom) AS quadkey
      FROM features, generate_series($5::int, $6::int) AS zoom
    )
    SELECT zoom, quadkey
    FROM series
    WHERE quadkey IS NOT NULL AND quadkey <> ''
  `;

  const tiles = new Set();
  const result = await pool.query(sql, [bounds[0], bounds[1], bounds[2], bounds[3], shard.minZoom, shard.maxZoom]);
  for (const row of result.rows) {
    const { z, x, y } = quadkeyToZxy(String(row.quadkey));
    tiles.add(`${z}:${x}:${y}`);
  }

  const parsed = [];
  for (const entry of tiles) {
    const [zStr, xStr, yStr] = entry.split(':');
    parsed.push({ z: Number.parseInt(zStr, 10), x: Number.parseInt(xStr, 10), y: Number.parseInt(yStr, 10) });
  }
  parsed.sort((a, b) => (a.z - b.z) || (a.x - b.x) || (a.y - b.y));
  return parsed;
}

function selectShards(options) {
  const shards = [];
  const datasetConfig = getDatasetConfig();
  for (const [dataset, config] of Object.entries(datasetConfig)) {
    if (!config || !Array.isArray(config.shards)) {
      continue;
    }
    if (options.datasets && !options.datasets.has(dataset)) {
      continue;
    }
    for (const shard of config.shards) {
      const shardKey = `${dataset}:${shard.id}`;
      if (options.shards && !options.shards.has(shardKey)) {
        continue;
      }
      shards.push({
        dataset,
        shardId: shard.id,
        bounds: shard.bounds,
        minZoom: Number.parseInt(shard.minZoom, 10),
        maxZoom: Number.parseInt(shard.maxZoom, 10),
        filename: shard.filename,
      });
    }
  }
  return shards;
}

async function ensureConsumerGroup(redisClient, stream, group) {
  try {
    await redisClient.xGroupCreate(stream, group, '0', { MKSTREAM: true });
    console.log(`[enqueue] Created consumer group '${group}' on stream '${stream}'`);
  } catch (error) {
    if (!String(error?.message).includes('BUSYGROUP')) {
      throw error;
    }
  }
}

async function main() {
  const options = parseArgs();
  const buildConfig = getBuildConfig();
  const { stream, consumerGroup, progressPrefix } = buildConfig;

  const shards = selectShards(options);
  if (shards.length === 0) {
    console.log('No shards selected; exiting.');
    return;
  }

  const redisUrl = getEnv('REDIS_URL') || getEnv('REDIS_PUBLIC_URL') || getEnv('REDIS_CONNECTION');
  if (!redisUrl) {
    throw new Error('REDIS_URL (or REDIS_PUBLIC_URL) must be set');
  }

  const redisClient = createClient({ url: redisUrl });
  redisClient.on('error', (error) => {
    console.error('[enqueue] Redis error:', error);
  });
  await redisClient.connect();

  if (options.reset) {
    await redisClient.del(stream);
  }
  await ensureConsumerGroup(redisClient, stream, consumerGroup);

  const dsn = resolveDatabaseDsn();
  const pool = new Pool({ connectionString: dsn });

  try {
    for (const shard of shards) {
      const progressKey = `${progressPrefix}:${shard.dataset}:${shard.shardId}`;
      if (options.reset) {
        await redisClient.del(progressKey);
      }

      console.log(`[enqueue] Computing tiles for ${shard.dataset}:${shard.shardId}`);
      const tiles = await collectTiles(pool, shard);
      console.log(`[enqueue] Found ${tiles.length} tiles for ${shard.dataset}:${shard.shardId}`);

      if (tiles.length === 0) {
        await redisClient.hSet(progressKey, {
          dataset: shard.dataset,
          shard: shard.shardId,
          status: 'empty',
          total_tiles: 0,
          completed_tiles: 0,
          updated_at: new Date().toISOString(),
        });
        continue;
      }

      const completedExisting = await redisClient.hGet(progressKey, 'completed_tiles');
      await redisClient.hSet(progressKey, {
        dataset: shard.dataset,
        shard: shard.shardId,
        status: 'queued',
        total_tiles: tiles.length,
        completed_tiles: options.reset ? 0 : Number.parseInt(completedExisting || '0', 10),
        pending_since_rebuild: 0,
        updated_at: new Date().toISOString(),
      });

      let batch = redisClient.multi();
      let batchCount = 0;
      for (const { z, x, y } of tiles) {
        const priority = Math.max(0, 100 - z);
        batch.xAdd(stream, '*', {
          dataset: shard.dataset,
          shard: shard.shardId,
          z: String(z),
          x: String(x),
          y: String(y),
          priority: String(priority),
          attempt: '0',
          shard_id: `${shard.dataset}:${shard.shardId}`,
        });
        batchCount += 1;
        if (batchCount >= 512) {
          await batch.exec();
          batch = redisClient.multi();
          batchCount = 0;
        }
      }
      if (batchCount > 0) {
        await batch.exec();
      }

      console.log(`[enqueue] Enqueued tiles for ${shard.dataset}:${shard.shardId}`);
    }
  } finally {
    await pool.end();
    await redisClient.quit();
  }

  console.log('[enqueue] Completed queue bootstrap');
}

main().catch((error) => {
  console.error('[enqueue] Fatal error:', error);
  process.exitCode = 1;
});
