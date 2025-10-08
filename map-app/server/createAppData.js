import { readFile, stat } from 'fs/promises';
import path from 'path';
import process from 'node:process';
import { fileURLToPath } from 'url';
import { normalizeStreetName } from '../shared/streetUtils.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// Use DATA_DIR from environment (set by index.js)
const DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../public/data');
const STREET_STATS_FILE = path.join(DATA_DIR, 'street_stats.json');
const NEIGHBOURHOOD_STATS_FILE = path.join(DATA_DIR, 'neighbourhood_stats.json');
const SUMMARY_FILE = path.join(DATA_DIR, 'tickets_summary.json');

let cachedSnapshot = null;

function mapToSerializableList(map, transform, sortKey = 'totalRevenue') {
  return Array.from(map.values())
    .map(transform)
    .sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0));
}

async function readJson(filePath, fallbackValue) {
  try {
    const raw = await readFile(filePath, 'utf-8');
    return JSON.parse(raw);
  } catch (error) {
    if (fallbackValue !== undefined) {
      console.warn(`Failed to read ${filePath}, using fallback:`, error.message);
      return fallbackValue;
    }
    throw error;
  }
}

export async function createAppData() {
  let version = null;
  try {
    const stats = await stat(SUMMARY_FILE);
    version = Math.trunc(stats.mtimeMs);
  } catch (error) {
    console.warn('Unable to stat tickets summary file:', error.message);
  }

  if (cachedSnapshot && version !== null && cachedSnapshot.version === version) {
    return cachedSnapshot.payload;
  }

  const [summary, streetStats, neighbourhoodStats] = await Promise.all([
    readJson(SUMMARY_FILE, { featureCount: 0, ticketCount: 0, totalRevenue: 0 }),
    readJson(STREET_STATS_FILE, {}),
    readJson(NEIGHBOURHOOD_STATS_FILE, {}),
  ]);

  const totals = {
    featureCount: Number(summary.featureCount) || 0,
    ticketCount: Number(summary.ticketCount) || 0,
    totalRevenue: Number(summary.totalRevenue) || 0,
  };

  const streetMap = new Map();
  for (const [location, stats] of Object.entries(streetStats)) {
    const streetKey = normalizeStreetName(location);
    const neighbourhoodSet = new Set();
    if (Array.isArray(stats.neighbourhoods)) {
      for (const value of stats.neighbourhoods) {
        if (value && value !== 'Unknown') {
          neighbourhoodSet.add(value);
        }
      }
    } else if (stats.neighbourhood && stats.neighbourhood !== 'Unknown') {
      neighbourhoodSet.add(stats.neighbourhood);
    }
    streetMap.set(streetKey, {
      name: streetKey,
      ticketCount: Number(stats.ticketCount) || 0,
      totalRevenue: Number(stats.totalRevenue) || 0,
      sampleLocation: location,
      neighbourhoods: neighbourhoodSet,
    });
  }

  const neighbourhoodMap = new Map();
  for (const [name, stats] of Object.entries(neighbourhoodStats)) {
    neighbourhoodMap.set(name, {
      name,
      count: Number(stats.count) || 0,
      totalRevenue: Number(stats.totalFines) || 0,
    });
  }

  const topStreets = mapToSerializableList(streetMap, (entry) => ({
    name: entry.name,
    ticketCount: entry.ticketCount,
    totalRevenue: Number(entry.totalRevenue.toFixed(2)),
    neighbourhoods: Array.from(entry.neighbourhoods),
    sampleLocation: entry.sampleLocation,
  })).slice(0, 10);

  const topNeighbourhoods = mapToSerializableList(
    neighbourhoodMap,
    (entry) => ({
      name: entry.name,
      totalFines: Number(entry.totalRevenue.toFixed(2)),
      count: entry.count,
    }),
    'totalFines',
  )
    .filter((entry) => entry.name && entry.name !== 'Unknown')
    .slice(0, 10);

  const payload = {
    totals: {
      ...totals,
      totalRevenue: Number(totals.totalRevenue.toFixed(2)),
    },
    topStreets,
    topNeighbourhoods,
    generatedAt: new Date().toISOString(),
  };

  cachedSnapshot = {
    version,
    payload,
  };

  return payload;
}
