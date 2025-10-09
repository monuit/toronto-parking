import { setTimeout as delay } from 'node:timers/promises';
import { Pool } from 'pg';
import { getPostgresConfig } from './runtimeConfig.js';
import { loadTicketsSummary, loadDatasetSummary } from './ticketsDataStore.js';

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

function isMissingRelation(error) {
  return Boolean(error && error.code === '42P01');
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
        SELECT
          COUNT(DISTINCT location_code)::BIGINT AS count,
          COALESCE(SUM(ticket_count), 0)::BIGINT AS tickets,
          COALESCE(SUM(total_revenue), 0)::NUMERIC AS revenue
        FROM ase_yearly_locations
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

export async function getDatasetTotals(dataset) {
  const normalised = dataset || 'parking_tickets';

  try {
    if (normalised === 'parking_tickets') {
      const pgResult = await fetchParkingTicketTotals();
      if (pgResult) {
        return pgResult;
      }
      const summaryWrapper = await loadTicketsSummary();
      if (summaryWrapper?.data) {
        const summary = summaryWrapper.data;
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
      const summaryWrapper = await loadDatasetSummary('red_light_locations');
      const summary = summaryWrapper?.data;
      if (summary?.totals) {
        return {
          dataset: 'red_light_locations',
          featureCount: Number(summary.totals.locationCount) || 0,
          ticketCount: Number(summary.totals.ticketCount) || 0,
          totalRevenue: Number(summary.totals.totalRevenue) || 0,
          source: 'local-summary',
        };
      }
      return null;
    }

    if (normalised === 'ase_locations') {
      const [pgResult, summaryWrapper] = await Promise.all([
        fetchASETotals(),
        loadDatasetSummary('ase_locations').catch(() => null),
      ]);
      const summaryTotals = summaryWrapper?.data?.totals;
      if (pgResult) {
        return {
          dataset: 'ase_locations',
          featureCount: Number(pgResult.featureCount) || Number(summaryTotals?.locationCount) || 0,
          ticketCount: Number(pgResult.ticketCount) || Number(summaryTotals?.ticketCount) || 0,
          totalRevenue: Number(pgResult.totalRevenue) || Number(summaryTotals?.totalRevenue) || 0,
          source: 'postgres',
        };
      }
      if (summaryTotals) {
        return {
          dataset: 'ase_locations',
          featureCount: Number(summaryTotals.locationCount) || 0,
          ticketCount: Number(summaryTotals.ticketCount) || 0,
          totalRevenue: Number(summaryTotals.totalRevenue) || 0,
          source: summaryWrapper?.source || 'local-summary',
        };
      }
      return null;
    }
  } catch (error) {
    if (isMissingRelation(error)) {
      console.warn(`Dataset totals fallback: missing relation for ${normalised}`);
      if (normalised === 'parking_tickets') {
        const summaryWrapper = await loadTicketsSummary();
        if (summaryWrapper?.data) {
          const summary = summaryWrapper.data;
          return {
            dataset: 'parking_tickets',
            featureCount: Number(summary.featureCount) || 0,
            ticketCount: Number(summary.ticketCount) || 0,
            totalRevenue: Number(summary.totalRevenue) || 0,
            source: 'local-summary',
          };
        }
      } else {
        const summaryWrapper = await loadDatasetSummary(normalised);
        const summary = summaryWrapper?.data;
        if (summary?.totals) {
          return {
            dataset: normalised,
            featureCount: Number(summary.totals.locationCount) || 0,
            ticketCount: Number(summary.totals.ticketCount) || 0,
            totalRevenue: Number(summary.totals.totalRevenue) || 0,
            source: 'local-summary',
          };
        }
      }
      return null;
    }
    if (!isRetryable(error)) {
      throw error;
    }
    return null;
  }

  throw new Error(`Unsupported dataset "${normalised}"`);
}
