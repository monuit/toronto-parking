#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';

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
  },
  red_light_locations: {
    table: 'red_light_camera_locations',
  },
  ase_locations: {
    table: 'ase_camera_locations',
  },
};

const TILE_SQL = {
  parking_tickets: `
    WITH bounds AS (
      SELECT tile_envelope_3857($1::integer, $2::integer, $3::integer) AS geom
    ), ranked AS (
      SELECT
        COALESCE(centreline_id::text, street_normalized, location1, ticket_hash) AS feature_id,
        geom_3857,
        street_normalized,
        centreline_id,
        COUNT(*) OVER (PARTITION BY COALESCE(centreline_id::text, street_normalized, location1, ticket_hash)) AS ticket_count,
        SUM(COALESCE(set_fine_amount, 0)) OVER (PARTITION BY COALESCE(centreline_id::text, street_normalized, location1, ticket_hash)) AS total_fine_amount,
        ROW_NUMBER() OVER (
          PARTITION BY COALESCE(centreline_id::text, street_normalized, location1, ticket_hash)
          ORDER BY date_of_infraction DESC NULLS LAST, time_of_infraction DESC NULLS LAST
        ) AS rn
      FROM parking_tickets
      WHERE geom_3857 IS NOT NULL
    ), aggregated AS (
      SELECT
        feature_id,
        ticket_count,
        total_fine_amount,
        geom_3857,
        street_normalized,
        centreline_id
      FROM ranked
      WHERE rn = 1
    ), variants AS (
      SELECT
        agg.feature_id,
        agg.ticket_count,
        agg.total_fine_amount,
        agg.street_normalized,
        agg.centreline_id,
        variant.min_zoom,
        variant.max_zoom,
        (ST_Dump(variant.geom_variant)).geom AS geom
      FROM aggregated AS agg
      CROSS JOIN LATERAL (
        SELECT 0 AS min_zoom, 10 AS max_zoom, subdivided.geom AS geom_variant
        FROM ST_Subdivide(ST_SimplifyPreserveTopology(agg.geom_3857, 25), 32) AS subdivided(geom)
        UNION ALL
        SELECT 11 AS min_zoom, 16 AS max_zoom, subdivided.geom AS geom_variant
        FROM ST_Subdivide(agg.geom_3857, 4) AS subdivided(geom)
      ) AS variant
    ), features AS (
      SELECT
        ST_AsMVTGeom(
          variants.geom,
          bounds.geom,
          4096,
          64,
          true
        ) AS geom,
        'parking_tickets' AS dataset,
        variants.feature_id,
        variants.ticket_count,
        variants.total_fine_amount,
        variants.street_normalized,
        variants.centreline_id::BIGINT AS centreline_id
      FROM variants
      CROSS JOIN bounds
      WHERE variants.geom && bounds.geom
        AND $1 BETWEEN variants.min_zoom AND variants.max_zoom
    )
    SELECT ST_AsMVT(features, $4, 4096, 'geom') FROM features;
  `,
  red_light_locations: `
    WITH bounds AS (
      SELECT tile_envelope_3857($1::integer, $2::integer, $3::integer) AS geom
    ), base AS (
      SELECT
        intersection_id,
        location_name,
        ticket_count,
        total_fine_amount,
        ward_1,
        geom_3857
      FROM red_light_camera_locations
      WHERE geom_3857 IS NOT NULL
    ), variants AS (
      SELECT
        base.intersection_id,
        base.location_name,
        base.ticket_count,
        base.total_fine_amount,
        base.ward_1,
        variant.min_zoom,
        variant.max_zoom,
        (ST_Dump(variant.geom_variant)).geom AS geom
      FROM base
      CROSS JOIN LATERAL (
        SELECT 0 AS min_zoom, 11 AS max_zoom, subdivided.geom AS geom_variant
        FROM ST_Subdivide(ST_SimplifyPreserveTopology(base.geom_3857, 30), 32) AS subdivided(geom)
        UNION ALL
        SELECT 12 AS min_zoom, 16 AS max_zoom, subdivided.geom AS geom_variant
        FROM ST_Subdivide(base.geom_3857, 4) AS subdivided(geom)
      ) AS variant
    ), features AS (
      SELECT
        ST_AsMVTGeom(
          variants.geom,
          bounds.geom,
          4096,
          64,
          true
        ) AS geom,
        'red_light_locations' AS dataset,
        variants.intersection_id::TEXT AS feature_id,
        variants.ticket_count,
        variants.total_fine_amount,
        variants.location_name,
        variants.ward_1 AS ward
      FROM variants
      CROSS JOIN bounds
      WHERE variants.geom && bounds.geom
        AND $1 BETWEEN variants.min_zoom AND variants.max_zoom
    )
    SELECT ST_AsMVT(features, $4, 4096, 'geom') FROM features;
  `,
  ase_locations: `
    WITH bounds AS (
      SELECT tile_envelope_3857($1::integer, $2::integer, $3::integer) AS geom
    ), base AS (
      SELECT
        location_code,
        location,
        status,
        ward,
        ticket_count,
        total_fine_amount,
        geom_3857
      FROM ase_camera_locations
      WHERE geom_3857 IS NOT NULL
    ), variants AS (
      SELECT
        base.location_code,
        base.location,
        base.status,
        base.ward,
        base.ticket_count,
        base.total_fine_amount,
        variant.min_zoom,
        variant.max_zoom,
        (ST_Dump(variant.geom_variant)).geom AS geom
      FROM base
      CROSS JOIN LATERAL (
        SELECT 0 AS min_zoom, 11 AS max_zoom, subdivided.geom AS geom_variant
        FROM ST_Subdivide(ST_SimplifyPreserveTopology(base.geom_3857, 30), 32) AS subdivided(geom)
        UNION ALL
        SELECT 12 AS min_zoom, 16 AS max_zoom, subdivided.geom AS geom_variant
        FROM ST_Subdivide(base.geom_3857, 4) AS subdivided(geom)
      ) AS variant
    ), features AS (
      SELECT
        ST_AsMVTGeom(
          variants.geom,
          bounds.geom,
          4096,
          64,
          true
        ) AS geom,
        'ase_locations' AS dataset,
        variants.location_code::TEXT AS feature_id,
        variants.ticket_count,
        variants.total_fine_amount,
        variants.location,
        variants.status,
        variants.ward
      FROM variants
      CROSS JOIN bounds
      WHERE variants.geom && bounds.geom
        AND $1 BETWEEN variants.min_zoom AND variants.max_zoom
    )
    SELECT ST_AsMVT(features, $4, 4096, 'geom') FROM features;
  `,
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
  const stagingDir = getEnv('PMTILES_STAGING_DIR', path.resolve(REPO_ROOT, 'pmtiles', 'staging'));
  const maxAttempts = Number.parseInt(getEnv('PMTILES_BUILD_MAX_ATTEMPTS', '5'), 10);
  const batchCount = Number.parseInt(getEnv('PMTILES_BUILD_BATCH_COUNT', '10'), 10);
  const rebuildInterval = Number.parseInt(getEnv('PMTILES_REBUILD_INTERVAL', '500'), 10);
  const uploadOnRebuild = getEnv('PMTILES_REBUILD_UPLOAD', '') === '1';
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

function parseWorkerArgs() {
  const args = process.argv.slice(2);
  const options = {
    consumer: `worker-${Math.random().toString(36).slice(2, 8)}`,
    blockMs: 1000,
    concurrency: 4,
    rebuildInterval: null,
    uploadOnRebuild: null,
  };
  for (const arg of args) {
    if (arg.startsWith('--consumer=')) {
      options.consumer = arg.substring('--consumer='.length);
    } else if (arg.startsWith('--block=')) {
      options.blockMs = Number.parseInt(arg.substring('--block='.length), 10);
    } else if (arg.startsWith('--concurrency=')) {
      options.concurrency = Number.parseInt(arg.substring('--concurrency='.length), 10);
    } else if (arg.startsWith('--rebuild-interval=')) {
      options.rebuildInterval = Number.parseInt(arg.substring('--rebuild-interval='.length), 10);
    } else if (arg === '--upload-on-rebuild') {
      options.uploadOnRebuild = true;
    }
  }
  if (!Number.isFinite(options.blockMs) || options.blockMs <= 0) {
    options.blockMs = 1000;
  }
  if (!Number.isFinite(options.concurrency) || options.concurrency <= 0) {
    options.concurrency = 4;
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
  throw new Error('Unable to resolve Postgres connection string');
}

async function ensureConsumerGroup(redisClient, stream, group) {
  try {
    await redisClient.xGroupCreate(stream, group, '0', { MKSTREAM: true });
    console.log(`[worker] Created consumer group '${group}' on stream '${stream}'`);
  } catch (error) {
    if (!String(error?.message).includes('BUSYGROUP')) {
      throw error;
    }
  }
}

function resolveShard(dataset, shardId) {
  const datasetConfig = getDatasetConfig();
  const config = datasetConfig[dataset];
  if (!config) {
    throw new Error(`Unknown dataset '${dataset}'`);
  }
  const shard = (config.shards || []).find((entry) => entry.id === shardId);
  if (!shard) {
    throw new Error(`Unknown shard '${dataset}:${shardId}'`);
  }
  return shard;
}

async function fetchTile(pool, dataset, z, x, y) {
  const sql = TILE_SQL[dataset];
  if (!sql) {
    throw new Error(`No SQL defined for dataset '${dataset}'`);
  }
  const result = await pool.query(sql, [z, x, y, dataset]);
  if (result.rows.length === 0 || !result.rows[0].st_asmvt) {
    return null;
  }
  return result.rows[0].st_asmvt;
}

async function writeTile(stagingDir, dataset, shardId, z, x, y, buffer) {
  const tilePath = path.join(stagingDir, dataset, shardId, String(z), String(x));
  await fs.promises.mkdir(tilePath, { recursive: true });
  const filename = path.join(tilePath, `${y}.mvt`);
  await fs.promises.writeFile(filename, buffer);
}

async function spawnRebuild(dataset, shardId, upload) {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(REPO_ROOT, 'scripts', 'pmtiles', 'build_pmtiles.py');
    const outputDir = path.join(REPO_ROOT, 'pmtiles');
    const args = [scriptPath, '--output-dir', outputDir, '--datasets', dataset, '--shards', `${dataset}:${shardId}`];
    if (upload) {
      args.push('--upload');
    }
    const child = spawn('python', args, {
      cwd: REPO_ROOT,
      stdio: 'inherit',
    });
    child.on('error', reject);
    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`build_pmtiles.py exited with code ${code}`));
      }
    });
  });
}

async function main() {
  const workerOptions = parseWorkerArgs();
  const buildConfig = getBuildConfig();
  const stream = buildConfig.stream;
  const group = buildConfig.consumerGroup;
  const progressPrefix = buildConfig.progressPrefix;
  const failureStream = buildConfig.failureStream;
  const stagingDir = buildConfig.stagingDir;
  const maxAttempts = buildConfig.maxAttempts;
  const batchCount = buildConfig.batchCount;
  const rebuildInterval = workerOptions.rebuildInterval || buildConfig.rebuildInterval;
  const uploadOnRebuild = workerOptions.uploadOnRebuild ?? buildConfig.uploadOnRebuild;

  await fs.promises.mkdir(stagingDir, { recursive: true });

  const redisUrl = getEnv('REDIS_URL') || getEnv('REDIS_PUBLIC_URL') || getEnv('REDIS_CONNECTION');
  if (!redisUrl) {
    throw new Error('REDIS_URL (or REDIS_PUBLIC_URL) must be set');
  }

  const redisClient = createClient({ url: redisUrl });
  redisClient.on('error', (error) => {
    console.error('[worker] Redis error:', error);
  });
  await redisClient.connect();
  await ensureConsumerGroup(redisClient, stream, group);

  const dsn = resolveDatabaseDsn();
  const pool = new Pool({ connectionString: dsn, max: Math.max(workerOptions.concurrency, 4) });

  let running = true;
  const rebuildLocks = new Set();

  const shutdown = async () => {
    if (!running) {
      return;
    }
    running = false;
    console.log('[worker] Shutting down...');
    await pool.end().catch(() => {});
    await redisClient.quit().catch(() => {});
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  async function updateProgress(dataset, shardId, z, x, y, written) {
    const progressKey = `${progressPrefix}:${dataset}:${shardId}`;
    const multi = redisClient.multi();
    multi.hIncrBy(progressKey, 'completed_tiles', 1);
    if (written) {
      multi.hIncrBy(progressKey, 'tiles_written', 1);
    }
    multi.hIncrBy(progressKey, 'pending_since_rebuild', 1);
    multi.hSet(progressKey, {
      last_z: z,
      last_x: x,
      last_y: y,
      last_updated_at: new Date().toISOString(),
      status: 'running',
    });
    const results = await multi.exec();
    const pendingIndex = written ? 2 : 1;
    const pendingResult = results?.[pendingIndex]?.[1];
    const pending = Number.parseInt(pendingResult ?? '0', 10);
    const totals = await redisClient.hmGet(progressKey, ['total_tiles', 'completed_tiles']);
    const totalTiles = Number.parseInt(totals[0] || '0', 10);
    const completedTiles = Number.parseInt(totals[1] || '0', 10);
    return { pending, totalTiles, completedTiles, progressKey };
  }

  async function maybeRebuild(dataset, shardId, progressKey, pending, completed, total) {
    const shardKey = `${dataset}:${shardId}`;
    if (rebuildLocks.has(shardKey)) {
      return;
    }
    if (total > 0 && completed >= total) {
      console.log(`[worker] Shard ${shardKey} completed (${completed}/${total}); triggering rebuild`);
    } else if (pending < rebuildInterval) {
      return;
    }
    rebuildLocks.add(shardKey);
    try {
      console.log(`[worker] Rebuilding shard ${shardKey}`);
      await spawnRebuild(dataset, shardId, uploadOnRebuild);
      await redisClient.hSet(progressKey, {
        pending_since_rebuild: 0,
        last_rebuild_at: new Date().toISOString(),
        last_rebuild_status: 'success',
        status: total > 0 && completed >= total ? 'complete' : 'running',
      });
    } catch (error) {
      console.error(`[worker] Rebuild failed for ${shardKey}:`, error.message);
      await redisClient.xAdd(failureStream, '*', {
        type: 'rebuild_failure',
        shard: shardKey,
        error: error.message,
        time: new Date().toISOString(),
      });
      await redisClient.hSet(progressKey, {
        last_rebuild_status: 'failed',
        last_rebuild_error: error.message,
      });
    } finally {
      rebuildLocks.delete(shardKey);
    }
  }

  async function handleMessage(message) {
    const { id, message: fields } = message;
    const dataset = fields.dataset;
    const shardId = fields.shard;
    const z = Number.parseInt(fields.z, 10);
    const x = Number.parseInt(fields.x, 10);
    const y = Number.parseInt(fields.y, 10);
    const attempt = Number.parseInt(fields.attempt || '0', 10);
    const shardKey = `${dataset}:${shardId}`;

    try {
      resolveShard(dataset, shardId);
      const tileBuffer = await fetchTile(pool, dataset, z, x, y);
      if (tileBuffer && tileBuffer.length > 0) {
        await writeTile(stagingDir, dataset, shardId, z, x, y, tileBuffer);
      }
      const { pending, totalTiles, completedTiles, progressKey } = await updateProgress(
        dataset,
        shardId,
        z,
        x,
        y,
        Boolean(tileBuffer && tileBuffer.length > 0),
      );

      await redisClient.xAck(stream, group, id);
      await redisClient.xDel(stream, id);

      if (pending >= rebuildInterval || (totalTiles > 0 && completedTiles >= totalTiles)) {
        await maybeRebuild(dataset, shardId, progressKey, pending, completedTiles, totalTiles);
      }
    } catch (error) {
      console.error(`[worker] Failed to process tile ${shardKey} ${z}/${x}/${y}:`, error.message);
      await redisClient.xAck(stream, group, id);
      const nextAttempt = attempt + 1;
      if (nextAttempt > maxAttempts) {
        await redisClient.xAdd(failureStream, '*', {
          type: 'tile_failure',
          shard: shardKey,
          z: fields.z,
          x: fields.x,
          y: fields.y,
          error: error.message,
          attempts: String(nextAttempt),
          time: new Date().toISOString(),
        });
      } else {
        const delayMs = Math.min(60000, Math.pow(2, attempt) * 1000);
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        await redisClient.xAdd(stream, '*', {
          ...fields,
          attempt: String(nextAttempt),
          error: error.message,
        });
      }
    }
  }

  while (running) {
    const response = await redisClient.xReadGroup(group, workerOptions.consumer, [{ key: stream, id: '>' }], {
      COUNT: batchCount,
      BLOCK: workerOptions.blockMs,
    });
    if (!response) {
      continue;
    }
    const batch = response[0]?.messages || [];
    if (batch.length === 0) {
      continue;
    }
    const tasks = [];
    for (const message of batch) {
      tasks.push(handleMessage(message));
      if (tasks.length >= workerOptions.concurrency) {
        await Promise.allSettled(tasks.splice(0));
      }
      if (!running) {
        break;
      }
    }
    if (tasks.length > 0) {
      await Promise.allSettled(tasks);
    }
  }

  await shutdown();
}

main().catch((error) => {
  console.error('[worker] Fatal error:', error);
  process.exitCode = 1;
});
