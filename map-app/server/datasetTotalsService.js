import { readFile } from 'fs/promises';
import path from 'path';
import { setTimeout as delay } from 'node:timers/promises';
import { Pool } from 'pg';
import { getPostgresConfig } from './runtimeConfig.js';

const MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 1200;

let pool = null;

function ensurePool() {
  const config = getPostgresConfig();
  if (!config.enabled || !config.connectionString) {
    return null;
  }
  if (!pool) {
    pool = new Pool({
      connectionString: config.connectionString,
      ssl: config.ssl,
      application_name: 'dataset-totals-node',
    });
    pool.on('error', (error) => {
      console.warn('Postgres pool error:', error.message);
    });
  }
  return pool;
}

function isRetryable(error) {
  if (!error) {
    return false;
  }
  const code = error.code || error.errno;
  if (code && ['57P01', '57P03', '53300', 'ETIMEDOUT'].includes(code)) {
    return true;
  }
  const message = String(error.message || '').toLowerCase();
  return (
    message.includes('timeout') ||
    message.includes('terminating connection due to administrator command') ||
    message.includes('connection refused') ||
    message.includes('server closed the connection')
  );
}

async function withPgClient(task) {
  const activePool = ensurePool();
  if (!activePool) {
    return null;
  }
  let lastError = null;
  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
    try {
      const client = await activePool.connect();
      try {
        return await task(client);
      } finally {
        client.release();
      }
    } catch (error) {
      lastError = error;
      if (!isRetryable(error) || attempt === MAX_ATTEMPTS - 1) {
        throw error;
      }
      await delay(RETRY_DELAY_MS * (attempt + 1));
    }
  }
  return Promise.reject(lastError);
}

async function readLocalSummary(dataDir) {
  if (!dataDir) {
    return null;
  }
  try {
    const filePath = path.join(dataDir, 'tickets_summary.json');
    const raw = await readFile(filePath, 'utf-8');
    return JSON.parse(raw);
  } catch (error) {
    console.warn('Failed to read local tickets summary:', error.message);
    return null;
  }
}

async function fetchParkingTicketTotals() {
  return withPgClient(async (client) => {
    const result = await client.query(
      `
        SELECT COUNT(*)::BIGINT AS count,
               COALESCE(SUM(set_fine_amount), 0)::NUMERIC AS revenue
        FROM parking_tickets
      `,
    );
    const row = result?.rows?.[0];
    if (!row) {
      return null;
    }
    return {
      dataset: 'parking_tickets',
      featureCount: Number(row.count) || 0,
      ticketCount: Number(row.count) || 0,
      totalRevenue: Number(row.revenue) || 0,
    };
  });
}

async function fetchRedLightTotals() {
  return withPgClient(async (client) => {
    const result = await client.query(
      `
        SELECT COUNT(*)::BIGINT AS count,
               COALESCE(SUM(ticket_count), 0)::BIGINT AS tickets,
               COALESCE(SUM(total_fine_amount), 0)::NUMERIC AS revenue
        FROM red_light_camera_locations
      `,
    );
    const row = result?.rows?.[0];
    if (!row) {
      return null;
    }
    return {
      dataset: 'red_light_locations',
      featureCount: Number(row.count) || 0,
      ticketCount: Number(row.tickets) || 0,
      totalRevenue: Number(row.revenue) || 0,
    };
  });
}

async function fetchASETotals() {
  return withPgClient(async (client) => {
    const result = await client.query(
      `
        SELECT COUNT(*)::BIGINT AS count,
               COALESCE(SUM(ticket_count), 0)::BIGINT AS tickets,
               COALESCE(SUM(total_fine_amount), 0)::NUMERIC AS revenue
        FROM ase_camera_locations
      `,
    );
    const row = result?.rows?.[0];
    if (!row) {
      return null;
    }
    return {
      dataset: 'ase_locations',
      featureCount: Number(row.count) || 0,
      ticketCount: Number(row.tickets) || 0,
      totalRevenue: Number(row.revenue) || 0,
    };
  });
}

export async function getDatasetTotals(dataset, options = {}) {
  const normalised = dataset || 'parking_tickets';
  const { dataDir } = options;

  try {
    if (normalised === 'parking_tickets') {
      const pgResult = await fetchParkingTicketTotals();
      if (pgResult) {
        return pgResult;
      }
      const summary = await readLocalSummary(dataDir);
      if (summary) {
        return {
          dataset: 'parking_tickets',
          featureCount: Number(summary.featureCount) || 0,
          ticketCount: Number(summary.ticketCount) || 0,
          totalRevenue: Number(summary.totalRevenue) || 0,
          source: 'local-summary',
        };
      }
      return {
        dataset: 'parking_tickets',
        featureCount: 0,
        ticketCount: 0,
        totalRevenue: 0,
        source: 'fallback',
      };
    }

    if (normalised === 'red_light_locations') {
      const pgResult = await fetchRedLightTotals();
      if (pgResult) {
        return pgResult;
      }
      return null;
    }

    if (normalised === 'ase_locations') {
      const pgResult = await fetchASETotals();
      if (pgResult) {
        return pgResult;
      }
      return null;
    }
  } catch (error) {
    if (!isRetryable(error)) {
      throw error;
    }
    return null;
  }

  throw new Error(`Unsupported dataset "${normalised}"`);
}
