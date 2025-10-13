import { Buffer } from 'node:buffer';
import { gunzipSync } from 'node:zlib';
import { Pool } from 'pg';
import { createClient } from 'redis';
import { getTileDbConfig, getRedisConfig } from '../../server/runtimeConfig.js';

function formatNumber(value) {
  return value.toLocaleString('en-CA');
}

async function withPg(callback) {
  const config = getTileDbConfig();
  if (!config.enabled || !config.connectionString) {
    throw new Error('Postgres connection is not configured/enabled');
  }
  const pool = new Pool({
    connectionString: config.connectionString,
    ssl: config.ssl,
    application_name: 'ase-totals-debug',
  });
  try {
    return await callback(pool);
  } finally {
    await pool.end();
  }
}

async function querySingle(pool, sql, params = []) {
  const result = await pool.query(sql, params);
  return result?.rows?.[0] ?? null;
}

async function queryAll(pool, sql, params = []) {
  const result = await pool.query(sql, params);
  return result?.rows ?? [];
}

async function readRedisKey(key) {
  const config = getRedisConfig();
  if (!config.enabled || !config.url) {
    return null;
  }
  const client = createClient({ url: config.url });
  client.on('error', (error) => {
    console.warn('[analyze-ase] Redis error:', error.message);
  });
  try {
    await client.connect();
    const raw = await client.get(key);
    return raw;
  } finally {
    await client.disconnect();
  }
}

function maybeDecompressRedisPayload(raw) {
  if (!raw) {
    return null;
  }
  try {
    const parsed = JSON.parse(raw);
    if (parsed?.encoding === 'gzip+base64' && typeof parsed.data === 'string') {
      const decoded = gunzipSync(Buffer.from(parsed.data, 'base64')).toString('utf-8');
      parsed.data = JSON.parse(decoded);
    }
    return parsed;
  } catch (error) {
    console.warn('[analyze-ase] Failed to parse Redis payload:', error.message);
    return null;
  }
}

async function main() {
  console.log('=== ASE dataset totals analysis ===');

  await withPg(async (pool) => {
    const yearlyTotals = await querySingle(
      pool,
      `
        SELECT
          COUNT(DISTINCT location_code) AS location_count,
          SUM(ticket_count)::BIGINT AS ticket_count,
          SUM(total_revenue)::NUMERIC AS total_revenue,
          MIN(year) AS min_year,
          MAX(year) AS max_year
        FROM ase_yearly_locations
      `,
    );

    const activeTotals = await querySingle(
      pool,
      `
        SELECT
          COUNT(*) AS feature_count,
          SUM(ticket_count)::BIGINT AS ticket_count,
          SUM(total_fine_amount)::NUMERIC AS total_revenue
        FROM ase_camera_locations
      `,
    );

    const statusBreakdown = await queryAll(
      pool,
      `
        SELECT status, COUNT(*) AS location_count
        FROM (
          SELECT DISTINCT location_code, status
          FROM ase_yearly_locations
        ) AS distinct_locations
        GROUP BY status
        ORDER BY status
      `,
    );

    const latestYear = await querySingle(
      pool,
      `
        SELECT year,
               COUNT(DISTINCT location_code) AS location_count,
               SUM(ticket_count)::BIGINT AS ticket_count,
               SUM(total_revenue)::NUMERIC AS total_revenue
        FROM ase_yearly_locations
        GROUP BY year
        ORDER BY year DESC
        LIMIT 1
      `,
    );

    console.log('\nPostgres: ase_yearly_locations (historical rollup)');
    console.log('-----------------------------------------------');
    console.log({
      locationCount: Number(yearlyTotals?.location_count ?? 0),
      ticketCount: Number(yearlyTotals?.ticket_count ?? 0),
      totalRevenue: Number(yearlyTotals?.total_revenue ?? 0),
      yearRange: `${yearlyTotals?.min_year ?? '?'}-${yearlyTotals?.max_year ?? '?'}`,
    });

    console.log('\nPostgres: ase_camera_locations (current inventory)');
    console.log('------------------------------------------------');
    console.log({
      featureCount: Number(activeTotals?.feature_count ?? 0),
      ticketCount: Number(activeTotals?.ticket_count ?? 0),
      totalRevenue: Number(activeTotals?.total_revenue ?? 0),
    });

    console.log('\nStatus breakdown across historical locations');
    for (const row of statusBreakdown) {
      console.log(`  ${row.status || 'UNKNOWN'}: ${formatNumber(Number(row.location_count || 0))} locations`);
    }

    console.log('\nMost recent year in ase_yearly_locations');
    console.log({
      year: latestYear?.year ?? null,
      locationCount: Number(latestYear?.location_count ?? 0),
      ticketCount: Number(latestYear?.ticket_count ?? 0),
      totalRevenue: Number(latestYear?.total_revenue ?? 0),
    });
  });

  const redisKey = 'toronto:map-data:ase_locations:summary:v1';
  const redisRaw = await readRedisKey(redisKey);
  let redisPayload = null;
  if (redisRaw) {
    redisPayload = maybeDecompressRedisPayload(redisRaw);
  }

  if (redisPayload) {
    console.log('\nRedis summary payload');
    console.log('----------------------');
    console.log({
      version: redisPayload.version ?? null,
      updatedAt: redisPayload.updatedAt ?? null,
      totals: redisPayload.data?.totals ?? null,
      notes: redisPayload.data?.notes ?? null,
    });
  } else {
    console.log('\nRedis summary payload: not found or could not parse.');
  }

  console.log('\nDisk snapshot (public/data/ase_summary.json)');
  try {
    const filePath = new URL('../../public/data/ase_summary.json', import.meta.url);
    const contents = await (await import('node:fs/promises')).readFile(filePath, 'utf-8');
    const json = JSON.parse(contents);
    console.log({
      generatedAt: json.generatedAt ?? null,
      totals: json.totals ?? null,
    });
  } catch (error) {
    console.warn('  Failed to read ase_summary.json:', error.message);
  }

  console.log('\nAnalysis summary');
  console.log('----------------');
  console.log('Compare the historical rollup (ase_yearly_locations) against the current summary sources');
  console.log('If Redis/disk totals reflect ~1.04M tickets, they are stale relative to the 2.05M in Postgres.');
}

main().catch((error) => {
  console.error('Analysis failed:', error);
  process.exitCode = 1;
});
