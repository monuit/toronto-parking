import { readFile, stat } from 'fs/promises';
import path from 'path';
import process from 'node:process';
import { fileURLToPath } from 'url';
import booleanPointInPolygon from '@turf/boolean-point-in-polygon';
import { point as turfPoint } from '@turf/helpers';
import { normalizeStreetName } from '../shared/streetUtils.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// In dev: server/ -> public/data/
// In prod: dist/server/ -> dist/client/data/
const isProd = process.env.NODE_ENV === 'production';
const DATA_DIR = isProd
  ? path.resolve(__dirname, '../client/data')
  : path.resolve(__dirname, '../public/data');
const TICKETS_FILE = path.join(DATA_DIR, 'tickets_aggregated.geojson');
const NEIGHBOURHOODS_FILE = path.join(DATA_DIR, 'neighbourhoods.geojson');

let cachedSnapshot = null;
let neighbourhoodIndexPromise = null;
const coordinateNeighbourhoodCache = new Map();

function flattenCoordinates(coords) {
  const result = [];
  const stack = [coords];
  while (stack.length) {
    const current = stack.pop();
    if (!Array.isArray(current)) {
      continue;
    }
    if (typeof current[0] === 'number') {
      result.push(current);
    } else {
      for (const item of current) {
        stack.push(item);
      }
    }
  }
  return result;
}

function computeBoundingBox(geometry) {
  if (!geometry) {
    return null;
  }
  const positions = flattenCoordinates(geometry.coordinates || []);
  if (positions.length === 0) {
    return null;
  }
  let minLng = Infinity;
  let minLat = Infinity;
  let maxLng = -Infinity;
  let maxLat = -Infinity;

  for (const [lng, lat] of positions) {
    if (lng < minLng) minLng = lng;
    if (lat < minLat) minLat = lat;
    if (lng > maxLng) maxLng = lng;
    if (lat > maxLat) maxLat = lat;
  }

  return [minLng, minLat, maxLng, maxLat];
}

async function loadNeighbourhoodIndex() {
  if (!neighbourhoodIndexPromise) {
    neighbourhoodIndexPromise = (async () => {
      try {
        const raw = await readFile(NEIGHBOURHOODS_FILE, 'utf-8');
        const geojson = JSON.parse(raw);
        const features = Array.isArray(geojson?.features) ? geojson.features : [];
        return features
          .filter((feature) => feature?.geometry)
          .map((feature) => ({
            name: feature?.properties?.name || feature?.properties?.AREA_NAME || 'Unknown',
            feature,
            bbox: computeBoundingBox(feature.geometry),
          }))
          .filter((entry) => Array.isArray(entry.bbox));
      } catch (error) {
        console.warn('Failed to load neighbourhood polygons, continuing without reassignment:', error.message);
        return [];
      }
    })();
  }
  return neighbourhoodIndexPromise;
}

function normaliseCoordinateKey(lng, lat) {
  if (!Number.isFinite(lng) || !Number.isFinite(lat)) {
    return null;
  }
  return `${lng.toFixed(6)}|${lat.toFixed(6)}`;
}

function resolveNeighbourhoodFromPolygons(lng, lat, polygons) {
  const cacheKey = normaliseCoordinateKey(lng, lat);
  if (cacheKey && coordinateNeighbourhoodCache.has(cacheKey)) {
    return coordinateNeighbourhoodCache.get(cacheKey);
  }

  let assigned = 'Unknown';
  if (Array.isArray(polygons) && polygons.length > 0) {
    const candidatePoint = turfPoint([lng, lat]);
    for (const polygon of polygons) {
      const bbox = polygon.bbox;
      if (!bbox) {
        continue;
      }
      const [minLng, minLat, maxLng, maxLat] = bbox;
      if (lng < minLng || lng > maxLng || lat < minLat || lat > maxLat) {
        continue;
      }
      if (booleanPointInPolygon(candidatePoint, polygon.feature)) {
        assigned = polygon.name || 'Unknown';
        break;
      }
    }
  }

  if (cacheKey) {
    coordinateNeighbourhoodCache.set(cacheKey, assigned);
  }
  return assigned;
}

function mapToSerializableList(map, transform, sortKey = 'totalRevenue') {
  return Array.from(map.values())
    .map(transform)
    .sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0));
}

export async function createAppData() {
  const fileInfo = await stat(TICKETS_FILE);
  if (cachedSnapshot && cachedSnapshot.mtimeMs === fileInfo.mtimeMs) {
    return cachedSnapshot.payload;
  }

  const raw = await readFile(TICKETS_FILE, 'utf-8');
  const geojson = JSON.parse(raw);
  const neighbourhoodPolygons = await loadNeighbourhoodIndex();
  const canReassignNeighbourhood = neighbourhoodPolygons.length > 0;

  const totals = {
    featureCount: geojson.features.length,
    ticketCount: 0,
    totalRevenue: 0,
  };

  const streetMap = new Map();
  const neighbourhoodMap = new Map();

  for (const feature of geojson.features) {
    const props = feature.properties || {};
    const count = Number(props.count) || 0;
    const revenue = Number(props.total_revenue) || 0;
    let neighbourhood = props.neighbourhood;
    const coordinates = feature.geometry?.coordinates;

    if ((!neighbourhood || neighbourhood === 'Unknown') && canReassignNeighbourhood && Array.isArray(coordinates) && coordinates.length >= 2) {
      const [lng, lat] = coordinates;
      neighbourhood = resolveNeighbourhoodFromPolygons(lng, lat, neighbourhoodPolygons);
    }

    if (!neighbourhood) {
      neighbourhood = 'Unknown';
    }

    totals.ticketCount += count;
    totals.totalRevenue += revenue;

    const streetKey = normalizeStreetName(props.location);
    const streetEntry = streetMap.get(streetKey) || {
      name: streetKey,
      ticketCount: 0,
      totalRevenue: 0,
      sampleLocation: props.location || streetKey,
      neighbourhoods: new Set(),
    };
    streetEntry.ticketCount += count;
    streetEntry.totalRevenue += revenue;
    if (neighbourhood && neighbourhood !== 'Unknown') {
      streetEntry.neighbourhoods.add(neighbourhood);
    }
    streetMap.set(streetKey, streetEntry);

    const hoodEntry = neighbourhoodMap.get(neighbourhood) || {
      name: neighbourhood,
      count: 0,
      totalRevenue: 0,
    };
    hoodEntry.count += count;
    hoodEntry.totalRevenue += revenue;
    neighbourhoodMap.set(neighbourhood, hoodEntry);
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
    mtimeMs: fileInfo.mtimeMs,
    payload,
  };

  return payload;
}
