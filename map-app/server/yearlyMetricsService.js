import { Pool } from 'pg';
import { getPostgresConfig } from './runtimeConfig.js';
import { loadDatasetSummary } from './ticketsDataStore.js';

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
      application_name: 'yearly-metrics-service',
    });
    pool.on('error', (error) => {
      console.warn('[yearlyMetrics] Postgres pool error:', error.message);
    });
  }
  return pool;
}

function normaliseDataset(dataset) {
  if (dataset === 'parking_tickets' || !dataset) {
    return 'parking_tickets';
  }
  if (dataset === 'red_light_locations') {
    return 'red_light_locations';
  }
  if (dataset === 'ase_locations') {
    return 'ase_locations';
  }
  throw new Error(`Unsupported dataset "${dataset}"`);
}

async function query(sqlText, params = []) {
  const activePool = ensurePool();
  if (!activePool) {
    return null;
  }
  const client = await activePool.connect();
  try {
    const result = await client.query(sqlText, params);
    return result.rows;
  } finally {
    client.release();
  }
}

function coerceNumeric(value) {
  if (value === null || value === undefined) {
    return 0;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

export async function getDatasetYears(dataset) {
  const name = normaliseDataset(dataset);
  if (name === 'parking_tickets') {
    const rows = await query(
      `SELECT DISTINCT year FROM parking_ticket_yearly_locations ORDER BY year ASC`,
    );
    return rows ? rows.map((row) => row.year) : [];
  }
  if (name === 'red_light_locations') {
    const rows = await query(
      `SELECT DISTINCT year FROM red_light_yearly_locations ORDER BY year ASC`,
    );
    return rows ? rows.map((row) => row.year) : [];
  }
  const rows = await query(
    `SELECT DISTINCT year FROM ase_yearly_locations ORDER BY year ASC`,
  );
  return rows ? rows.map((row) => row.year) : [];
}

export async function getParkingTotals(year = null) {
  const rows = await query(
    `
      SELECT
        COUNT(DISTINCT location)::BIGINT AS location_count,
        SUM(ticket_count)::BIGINT AS ticket_count,
        SUM(total_revenue)::NUMERIC AS total_revenue
      FROM parking_ticket_yearly_locations
      WHERE ($1::INT IS NULL OR year = $1)
    `,
    [year],
  );
  const row = rows?.[0];
  return {
    locationCount: coerceNumeric(row?.location_count),
    ticketCount: coerceNumeric(row?.ticket_count),
    totalRevenue: coerceNumeric(row?.total_revenue),
  };
}

export async function getParkingTopStreets(year = null, limit = 10) {
  const rows = await query(
    `
      SELECT street, SUM(ticket_count)::BIGINT AS ticket_count,
             SUM(total_revenue)::NUMERIC AS total_revenue
      FROM parking_ticket_yearly_streets
      WHERE ($1::INT IS NULL OR year = $1)
      GROUP BY street
      ORDER BY ticket_count DESC, total_revenue DESC
      LIMIT $2
    `,
    [year, limit],
  );
  return (rows || []).map((row) => ({
    name: row.street,
    ticketCount: coerceNumeric(row.ticket_count),
    totalRevenue: Number(coerceNumeric(row.total_revenue).toFixed(2)),
  }));
}

export async function getParkingTopNeighbourhoods(year = null, limit = 10) {
  const rows = await query(
    `
      SELECT neighbourhood, SUM(ticket_count)::BIGINT AS ticket_count,
             SUM(total_revenue)::NUMERIC AS total_revenue
      FROM parking_ticket_yearly_neighbourhoods
      WHERE ($1::INT IS NULL OR year = $1)
      GROUP BY neighbourhood
      ORDER BY ticket_count DESC, total_revenue DESC
      LIMIT $2
    `,
    [year, limit],
  );
  return (rows || []).map((row) => ({
    name: row.neighbourhood,
    ticketCount: coerceNumeric(row.ticket_count),
    totalRevenue: Number(coerceNumeric(row.total_revenue).toFixed(2)),
  }));
}

export async function getParkingLocationDetail(location, year = null) {
  if (!location) {
    return null;
  }
  const rows = await query(
    `
      SELECT
        location,
        year,
        ticket_count,
        total_revenue,
        top_infraction,
        latitude,
        longitude,
        neighbourhood
      FROM parking_ticket_yearly_locations
      WHERE location_key = md5($1) AND location = $1 AND ($2::INT IS NULL OR year = $2)
      ORDER BY year ASC
    `,
    [location, year],
  );

  if (!rows || rows.length === 0) {
    return null;
  }

  const aggregate = rows.reduce(
    (acc, row) => {
      acc.ticketCount += coerceNumeric(row.ticket_count);
      acc.totalRevenue += coerceNumeric(row.total_revenue);
      acc.years.push(row.year);
      if (row.top_infraction && !acc.topInfraction) {
        acc.topInfraction = row.top_infraction;
      }
      acc.yearly[row.year] = {
        ticketCount: coerceNumeric(row.ticket_count),
        totalRevenue: Number(coerceNumeric(row.total_revenue).toFixed(2)),
      };
      acc.latitude = acc.latitude ?? row.latitude;
      acc.longitude = acc.longitude ?? row.longitude;
      acc.neighbourhood = acc.neighbourhood ?? row.neighbourhood;
      return acc;
    },
    {
      ticketCount: 0,
      totalRevenue: 0,
      years: [],
      topInfraction: null,
      yearly: {},
      latitude: null,
      longitude: null,
      neighbourhood: null,
    },
  );

  return {
    location,
    ticketCount: aggregate.ticketCount,
    totalRevenue: Number(aggregate.totalRevenue.toFixed(2)),
    topInfraction: aggregate.topInfraction,
    years: aggregate.years,
    yearly: aggregate.yearly,
    latitude: aggregate.latitude,
    longitude: aggregate.longitude,
    neighbourhood: aggregate.neighbourhood,
  };
}

export async function getCameraTotals(dataset, year = null) {
  const name = normaliseDataset(dataset);
  const table = name === 'red_light_locations' ? 'red_light_yearly_locations' : 'ase_yearly_locations';
  if (year === null) {
    try {
      const summaryResult = await loadDatasetSummary(name);
      const summaryTotals = summaryResult?.data?.totals;
      if (summaryTotals) {
        return {
          locationCount: coerceNumeric(summaryTotals.locationCount ?? summaryTotals.featureCount),
          ticketCount: coerceNumeric(summaryTotals.ticketCount),
          totalRevenue: coerceNumeric(summaryTotals.totalRevenue),
        };
      }
    } catch (error) {
      console.warn('[yearlyMetrics] Failed to load dataset summary:', error);
    }
  }
  const rows = await query(
    `
      SELECT
        COUNT(DISTINCT CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN location_code END)::BIGINT AS location_count,
        SUM(ticket_count)::BIGINT AS ticket_count,
        SUM(total_revenue)::NUMERIC AS total_revenue
      FROM ${table}
      WHERE ($1::INT IS NULL OR year = $1)
    `,
    [year],
  );
  const row = rows?.[0];
  return {
    locationCount: coerceNumeric(row?.location_count),
    ticketCount: coerceNumeric(row?.ticket_count),
    totalRevenue: coerceNumeric(row?.total_revenue),
  };
}

export async function getCameraTopLocations(dataset, year = null, limit = 10) {
  const name = normaliseDataset(dataset);
  const table = name === 'red_light_locations' ? 'red_light_yearly_locations' : 'ase_yearly_locations';
  const rows = await query(
    `
      SELECT
        location_code,
        location_name,
        SUM(ticket_count)::BIGINT AS ticket_count,
        SUM(total_revenue)::NUMERIC AS total_revenue
      FROM ${table}
      WHERE ($1::INT IS NULL OR year = $1)
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
      GROUP BY location_code, location_name
      ORDER BY ticket_count DESC, total_revenue DESC
      LIMIT $2
    `,
    [year, limit],
  );
  return (rows || []).map((row) => ({
    id: row.location_code,
    name: row.location_name,
    ticketCount: coerceNumeric(row.ticket_count),
    totalRevenue: Number(coerceNumeric(row.total_revenue).toFixed(2)),
  }));
}

export async function getCameraTopGroups(dataset, year = null, limit = 10) {
  const name = normaliseDataset(dataset);
  const table = name === 'red_light_locations' ? 'red_light_yearly_locations' : 'ase_yearly_locations';
  const groupField = 'ward';
  const rows = await query(
    `
      SELECT ${groupField} AS name,
             SUM(ticket_count)::BIGINT AS ticket_count,
             SUM(total_revenue)::NUMERIC AS total_revenue
      FROM ${table}
      WHERE ${groupField} IS NOT NULL AND ${groupField} <> ''
        AND ($1::INT IS NULL OR year = $1)
      GROUP BY ${groupField}
      ORDER BY ticket_count DESC, total_revenue DESC
      LIMIT $2
    `,
    [year, limit],
  );
  return (rows || []).map((row) => ({
    name: row.name,
    ticketCount: coerceNumeric(row.ticket_count),
    totalRevenue: Number(coerceNumeric(row.total_revenue).toFixed(2)),
  }));
}

export async function getCameraLocationDetail(dataset, code, year = null) {
  if (!code) {
    return null;
  }
  const name = normaliseDataset(dataset);
  const table = name === 'red_light_locations' ? 'red_light_yearly_locations' : 'ase_yearly_locations';
  const rows = await query(
    `
      SELECT
        location_code,
        location_name,
        ticket_count,
        total_revenue,
        ward,
        police_division,
        status,
        latitude,
        longitude,
        year
      FROM ${table}
      WHERE location_code = $1 AND ($2::INT IS NULL OR year = $2)
      ORDER BY year ASC
    `,
    [code, year],
  );

  if (!rows || rows.length === 0) {
    return null;
  }

  const aggregate = rows.reduce(
    (acc, row) => {
      acc.ticketCount += coerceNumeric(row.ticket_count);
      acc.totalRevenue += coerceNumeric(row.total_revenue);
      acc.years.push(row.year);
      acc.yearly[row.year] = {
        ticketCount: coerceNumeric(row.ticket_count),
        totalRevenue: Number(coerceNumeric(row.total_revenue).toFixed(2)),
      };
      acc.name = acc.name || row.location_name;
      acc.ward = acc.ward || row.ward;
      acc.policeDivision = acc.policeDivision || row.police_division;
      acc.status = acc.status || row.status;
      acc.latitude = acc.latitude ?? row.latitude;
      acc.longitude = acc.longitude ?? row.longitude;
      return acc;
    },
    {
      ticketCount: 0,
      totalRevenue: 0,
      years: [],
      yearly: {},
      name: null,
      ward: null,
      policeDivision: null,
      status: null,
      latitude: null,
      longitude: null,
    },
  );

  return {
    id: code,
    name: aggregate.name,
    ticketCount: aggregate.ticketCount,
    totalRevenue: Number(aggregate.totalRevenue.toFixed(2)),
    years: aggregate.years,
    yearly: aggregate.yearly,
    ward: aggregate.ward,
    policeDivision: aggregate.policeDivision,
    status: aggregate.status,
    latitude: aggregate.latitude,
    longitude: aggregate.longitude,
  };
}

export async function getParkingLocationYearMap() {
  const rows = await query(
    `
      SELECT location, year, ticket_count, total_revenue
      FROM parking_ticket_yearly_locations
    `,
  );
  if (!rows) {
    return new Map();
  }
  const map = new Map();
  for (const row of rows) {
    const key = row.location;
    if (!map.has(key)) {
      map.set(key, {});
    }
    map.get(key)[row.year] = {
      ticketCount: coerceNumeric(row.ticket_count),
      totalRevenue: Number(coerceNumeric(row.total_revenue).toFixed(2)),
    };
  }
  return map;
}

export async function getCameraLocationYearMap(dataset) {
  const name = normaliseDataset(dataset);
  const table = name === 'red_light_locations' ? 'red_light_yearly_locations' : 'ase_yearly_locations';
  const rows = await query(
    `
      SELECT location_code, year, ticket_count, total_revenue
      FROM ${table}
    `,
  );
  if (!rows) {
    return new Map();
  }
  const map = new Map();
  for (const row of rows) {
    const key = row.location_code;
    if (!map.has(key)) {
      map.set(key, {});
    }
    map.get(key)[row.year] = {
      ticketCount: coerceNumeric(row.ticket_count),
      totalRevenue: Number(coerceNumeric(row.total_revenue).toFixed(2)),
    };
  }
  return map;
}
