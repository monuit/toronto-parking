import fs from 'node:fs';
import { Client } from 'pg';

const connectionString = 'postgres://postgres:CA3DeGBF23F5C3Aag3Ecg4f2eDGD52Be@interchange.proxy.rlwy.net:57747/railway';
const geojsonPath = 'C:/Users/boredbedouin/Desktop/toronto-parking/map-app/public/data/tickets_aggregated.geojson';

function loadGeojsonCounts() {
  const raw = fs.readFileSync(geojsonPath, 'utf-8');
  const data = JSON.parse(raw);
  const map = new Map();
  for (const feature of data.features || []) {
    const location = feature?.properties?.location;
    const count = feature?.properties?.count;
    if (!location || typeof count !== 'number') {
      continue;
    }
    map.set(location.toUpperCase(), count);
  }
  return map;
}

async function loadDbCounts() {
  const client = new Client({ connectionString, ssl: { rejectUnauthorized: false } });
  await client.connect();
  const { rows } = await client.query(
    `SELECT location, SUM(ticket_count)::BIGINT AS count
     FROM parking_ticket_yearly_locations
     GROUP BY location`
  );
  await client.end();
  const map = new Map();
  for (const row of rows) {
    if (!row.location) continue;
    map.set(row.location.toUpperCase(), Number(row.count) || 0);
  }
  return map;
}

function compare(geojsonMap, dbMap) {
  let missingCount = 0;
  let missingTickets = 0;
  const negativeDiffs = [];
  const positiveDiffs = [];
  let geojsonTotal = 0;
  let dbTotal = 0;

  for (const [location, count] of geojsonMap.entries()) {
    const dbCount = dbMap.get(location) || 0;
    const diff = count - dbCount;
    geojsonTotal += count;
    dbTotal += dbCount;
    if (diff !== 0) {
      missingCount += 1;
      missingTickets += diff;
      if (diff > 0 && positiveDiffs.length < 10) {
        positiveDiffs.push({ location, expected: count, actual: dbCount, diff });
      }
      if (diff < 0 && negativeDiffs.length < 10) {
        negativeDiffs.push({ location, expected: count, actual: dbCount, diff });
      }
    }
  }

  return { missingLocations: missingCount, missingTickets, positiveDiffs, negativeDiffs, geojsonTotal, dbTotal };
}

async function main() {
  const geojsonMap = loadGeojsonCounts();
  const dbMap = await loadDbCounts();
  const comparison = compare(geojsonMap, dbMap);
  console.log({
    geojsonLocations: geojsonMap.size,
    dbLocations: dbMap.size,
    geojsonTotal: comparison.geojsonTotal,
    dbTotal: comparison.dbTotal,
    missingLocations: comparison.missingLocations,
    missingTickets: comparison.missingTickets,
    samplePositive: comparison.positiveDiffs,
    sampleNegative: comparison.negativeDiffs,
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
