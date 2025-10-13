import { Pool } from 'pg';
import { getTileDbConfig } from './runtimeConfig.js';
import { loadDatasetSummary } from './ticketsDataStore.js';

let pool = null;
let poolSignature = null;
const MISSING_RELATION_CODES = new Set(['42P01']);

function ensurePool() {
  const config = getTileDbConfig();
  const connectionString = config.readOnlyConnectionString || config.connectionString;
  if (!config.enabled || !connectionString) {
    return null;
  }
  const signature = `${connectionString}|${config.ssl ? 'ssl' : 'plain'}`;
  if (!pool || poolSignature !== signature) {
    if (pool) {
      pool.end().catch(() => {
        /* ignored */
      });
    }
    pool = new Pool({
      connectionString,
      ssl: config.ssl,
      application_name: 'yearly-metrics-service',
    });
    pool.on('error', (error) => {
      console.warn('[yearlyMetrics] Postgres pool error:', error.message);
    });
    poolSignature = signature;
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
  } catch (error) {
    if (MISSING_RELATION_CODES.has(error?.code)) {
      console.warn('[yearlyMetrics] query skipped due to missing relation:', error.message);
      return null;
    }
    throw error;
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

const WARD_DATASETS = new Set(['red_light_locations', 'ase_locations']);

function extractWardCodeAndName(raw) {
  if (!raw) {
    return { code: null, name: null };
  }
  const text = String(raw).trim();
  if (!text) {
    return { code: null, name: null };
  }
  let code = null;
  const parenMatch = text.match(/\((\d{1,2})\)/);
  if (parenMatch) {
    code = Number.parseInt(parenMatch[1], 10);
  }
  if (!Number.isInteger(code)) {
    const wardMatch = text.match(/\bWard\s*(\d{1,2})\b/i);
    if (wardMatch) {
      code = Number.parseInt(wardMatch[1], 10);
    }
  }
  if (!Number.isInteger(code)) {
    const trailing = text.match(/(\d{1,2})\b(?!.*\d)/);
    if (trailing) {
      code = Number.parseInt(trailing[1], 10);
    }
  }
  if (!Number.isInteger(code) || code <= 0) {
    return { code: null, name: text };
  }
  let name = text;
  if (parenMatch) {
    name = name.replace(parenMatch[0], '').trim();
  }
  name = name.replace(/^Ward\s*\d+\s*[:-]?\s*/i, '').trim();
  if (!name) {
    name = `Ward ${code}`;
  } else if (!/^\d+\s*[-–]/.test(name)) {
    name = `${code} - ${name}`;
  }
  return { code, name };
}

function mergeWardAggregate(bucket, delta, label) {
  bucket.ticketCount += Number(delta.ticketCount || 0);
  bucket.totalRevenue += Number(delta.totalRevenue || 0);
  bucket.locationCount += Number(delta.locationCount || 0);
  if (label === 'ase') {
    bucket.aseTicketCount += Number(delta.ticketCount || 0);
    bucket.aseTotalRevenue += Number(delta.totalRevenue || 0);
  }
  if (label === 'red') {
    bucket.redLightTicketCount += Number(delta.ticketCount || 0);
    bucket.redLightTotalRevenue += Number(delta.totalRevenue || 0);
  }
}

async function fetchWardAggregates(dataset) {
  const name = normaliseDataset(dataset);
  if (!WARD_DATASETS.has(name)) {
    throw new Error(`Unsupported ward dataset: ${dataset}`);
  }
  const table = name === 'red_light_locations' ? 'red_light_yearly_locations' : 'ase_yearly_locations';
  const rows = await query(
    `
      SELECT ward,
             COUNT(DISTINCT location_code)::INT AS location_count,
             SUM(ticket_count)::BIGINT AS ticket_count,
             SUM(total_revenue)::NUMERIC AS total_revenue
      FROM ${table}
      WHERE ward IS NOT NULL AND ward <> ''
      GROUP BY ward
    `,
  );
  const map = new Map();
  for (const row of rows || []) {
    const parsed = extractWardCodeAndName(row.ward);
    if (!parsed.code) {
      continue;
    }
    const key = String(parsed.code);
    const ticketCount = coerceNumeric(row.ticket_count);
    const totalRevenue = coerceNumeric(row.total_revenue);
    const locationCount = coerceNumeric(row.location_count);
    map.set(key, {
      wardCode: parsed.code,
      wardName: parsed.name || `Ward ${parsed.code}`,
      ticketCount,
      totalRevenue,
      locationCount,
      aseTicketCount: name === 'ase_locations' ? ticketCount : 0,
      aseTotalRevenue: name === 'ase_locations' ? totalRevenue : 0,
      redLightTicketCount: name === 'red_light_locations' ? ticketCount : 0,
      redLightTotalRevenue: name === 'red_light_locations' ? totalRevenue : 0,
    });
  }
  return map;
}

function buildRollupFromMap(map, options = {}) {
  const { dataset, sourceLabel } = options;
  const items = Array.from(map.values()).map((entry) => ({
    ...entry,
    totalRevenue: Number(entry.totalRevenue.toFixed(2)),
    aseTotalRevenue: Number(entry.aseTotalRevenue.toFixed(2)),
    redLightTotalRevenue: Number(entry.redLightTotalRevenue.toFixed(2)),
  }));
  items.sort((a, b) => (b.ticketCount || 0) - (a.ticketCount || 0));
  const totals = items.reduce(
    (acc, entry) => {
      acc.ticketCount += entry.ticketCount || 0;
      acc.totalRevenue += entry.totalRevenue || 0;
      acc.locationCount += entry.locationCount || 0;
      return acc;
    },
    { ticketCount: 0, totalRevenue: 0, locationCount: 0 },
  );
  const summary = {
    generatedAt: new Date().toISOString(),
    dataset,
    totals: {
      ticketCount: totals.ticketCount,
      totalRevenue: Number(totals.totalRevenue.toFixed(2)),
      locationCount: totals.locationCount,
    },
    wards: items,
    topWards: items.slice(0, 10),
    meta: {
      source: sourceLabel || 'yearly-rollup',
    },
  };
  if (dataset === 'cameras_combined') {
    summary.meta.componentDatasets = ['ase_locations', 'red_light_locations'];
  }
  summary.version = Date.now();
  return summary;
}

export async function getCameraWardRollup(dataset) {
  if (dataset === 'cameras_combined') {
    const [aseMap, redMap] = await Promise.all([
      fetchWardAggregates('ase_locations'),
      fetchWardAggregates('red_light_locations'),
    ]);
    const combined = new Map();
    const apply = (map, label) => {
      for (const entry of map.values()) {
        const key = String(entry.wardCode);
        let bucket = combined.get(key);
        if (!bucket) {
          bucket = {
            wardCode: entry.wardCode,
            wardName: entry.wardName,
            ticketCount: 0,
            totalRevenue: 0,
            locationCount: 0,
            aseTicketCount: 0,
            aseTotalRevenue: 0,
            redLightTicketCount: 0,
            redLightTotalRevenue: 0,
          };
          combined.set(key, bucket);
        }
        if (!bucket.wardName && entry.wardName) {
          bucket.wardName = entry.wardName;
        }
        mergeWardAggregate(bucket, entry, label);
      }
    };
    apply(aseMap, 'ase');
    apply(redMap, 'red');
    return buildRollupFromMap(combined, { dataset: 'cameras_combined', sourceLabel: 'yearly-rollup' });
  }

  const name = normaliseDataset(dataset);
  if (!WARD_DATASETS.has(name)) {
    throw new Error(`Unsupported ward dataset: ${dataset}`);
  }
  const map = await fetchWardAggregates(name);
  const sourceLabel = name === 'ase_locations' ? 'yearly-rollup-ase' : 'yearly-rollup-red-light';
  return buildRollupFromMap(map, { dataset: name, sourceLabel });
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
