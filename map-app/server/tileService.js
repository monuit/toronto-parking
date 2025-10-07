import { readFile, stat } from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import Supercluster from 'supercluster';
import geojsonvt from 'geojson-vt';
import vtpbf from 'vt-pbf';
import Flatbush from 'flatbush';
import { normalizeStreetName } from '../shared/streetUtils.js';
import {
  RAW_POINT_ZOOM_THRESHOLD,
  SUMMARY_ZOOM_THRESHOLD,
  TILE_LAYER_NAME,
} from '../shared/mapConstants.js';

const TILE_CACHE_LIMIT = 512;
const SUMMARY_LIMIT = 5;
const YEARS_LIMIT = 32;
const MONTHS_LIMIT = 24;

const YEAR_OFFSET = 2000;
const YEAR_MASK_WIDTH = 32;
const MONTH_MASK_WIDTH = 12;

function toNumberArray(value, limit) {
  if (!Array.isArray(value)) {
    return [];
  }
  const unique = new Set();
  for (const entry of value) {
    const num = Number(entry);
    if (!Number.isFinite(num)) {
      continue;
    }
    unique.add(num);
    if (unique.size >= limit) {
      break;
    }
  }
  return Array.from(unique).sort((a, b) => a - b).slice(0, limit);
}

function encodeYearMask(values) {
  if (!Array.isArray(values) || values.length === 0) {
    return 0;
  }
  let mask = 0;
  for (const value of values) {
    const offset = Math.trunc(value) - YEAR_OFFSET;
    if (offset < 0 || offset >= YEAR_MASK_WIDTH) {
      continue;
    }
    mask |= 1 << offset;
  }
  return mask >>> 0;
}

function decodeYearMask(mask) {
  if (!Number.isFinite(mask) || mask === 0) {
    return [];
  }
  const years = [];
  for (let offset = 0; offset < YEAR_MASK_WIDTH; offset += 1) {
    if ((mask & (1 << offset)) !== 0) {
      years.push(YEAR_OFFSET + offset);
    }
  }
  return years;
}

function yearMaskIncludes(mask, value) {
  if (!Number.isFinite(mask) || !Number.isFinite(value)) {
    return false;
  }
  const offset = Math.trunc(value) - YEAR_OFFSET;
  if (offset < 0 || offset >= YEAR_MASK_WIDTH) {
    return false;
  }
  return (mask & (1 << offset)) !== 0;
}

function encodeMonthMask(values) {
  if (!Array.isArray(values) || values.length === 0) {
    return 0;
  }
  let mask = 0;
  for (const value of values) {
    const offset = Math.trunc(value) - 1;
    if (offset < 0 || offset >= MONTH_MASK_WIDTH) {
      continue;
    }
    mask |= 1 << offset;
  }
  return mask >>> 0;
}

function decodeMonthMask(mask) {
  if (!Number.isFinite(mask) || mask === 0) {
    return [];
  }
  const months = [];
  for (let offset = 0; offset < MONTH_MASK_WIDTH; offset += 1) {
    if ((mask & (1 << offset)) !== 0) {
      months.push(offset + 1);
    }
  }
  return months;
}

function monthMaskIncludes(mask, value) {
  if (!Number.isFinite(mask) || !Number.isFinite(value)) {
    return false;
  }
  const offset = Math.trunc(value) - 1;
  if (offset < 0 || offset >= MONTH_MASK_WIDTH) {
    return false;
  }
  return (mask & (1 << offset)) !== 0;
}

function buildTileKey(z, x, y) {
  return `${z}/${x}/${y}`;
}

function hydrateTemporalTags(tile) {
  if (!tile || !Array.isArray(tile.features)) {
    return;
  }
  for (const feature of tile.features) {
    const tags = feature?.tags;
    if (!tags) {
      continue;
    }
    if (typeof tags.yearMask === 'number') {
      tags.years = decodeYearMask(tags.yearMask);
      delete tags.yearMask;
    }
    if (typeof tags.monthMask === 'number') {
      tags.months = decodeMonthMask(tags.monthMask);
      delete tags.monthMask;
    }
  }
}

// MARK: TileService
class TileService {
  constructor(dataFile) {
    this.dataFile = dataFile;
    this.lastMtimeMs = null;
    this.loadingPromise = null;
    this.clusterIndex = null;
    this.pointIndex = null;
    this.summaryPoints = [];
    this.summaryTree = null;
    this.tileCache = new Map();
  }

  async ensureLoaded() {
    const fileInfo = await stat(this.dataFile);
    if (this.lastMtimeMs && this.lastMtimeMs === fileInfo.mtimeMs && this.clusterIndex && this.pointIndex) {
      return;
    }
    if (this.loadingPromise) {
      await this.loadingPromise;
      return;
    }
    this.loadingPromise = this.loadFromDisk(fileInfo);
    try {
      await this.loadingPromise;
    } finally {
      this.loadingPromise = null;
    }
  }

  async loadFromDisk(fileInfo) {
    const raw = await readFile(this.dataFile, 'utf-8');
    const geojson = JSON.parse(raw);
    const features = Array.isArray(geojson?.features) ? geojson.features : [];

    const sanitized = [];
    const summaryPoints = [];
    const spatialIndex = new Flatbush(features.length || 1);

    for (let i = 0; i < features.length; i += 1) {
      const feature = features[i];
      const coords = feature?.geometry?.coordinates;
      if (!Array.isArray(coords) || coords.length < 2) {
        continue;
      }
      const [longitude, latitude] = coords;
      if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
        continue;
      }

      const props = feature?.properties || {};
      const count = Number(props.count) || 0;
      const totalRevenue = Number(props.total_revenue) || 0;
      if (count <= 0) {
        continue;
      }

      const years = toNumberArray(props.years, YEARS_LIMIT);
      const months = toNumberArray(props.months, MONTHS_LIMIT);
      const yearMask = encodeYearMask(years);
      const monthMask = encodeMonthMask(months);
      const location = props.location || null;
      const topInfraction = props.top_infraction || null;
      const streetName = normalizeStreetName(location || props.address || '');

      const cleanedProperties = {
        id: i,
        location,
        count,
        total_revenue: Number(totalRevenue.toFixed(2)),
        top_infraction: topInfraction,
        yearMask,
        monthMask,
      };

      if (!cleanedProperties.top_infraction) {
        delete cleanedProperties.top_infraction;
      }

      sanitized.push({
        type: 'Feature',
        geometry: {
          type: 'Point',
          coordinates: [longitude, latitude],
        },
        properties: cleanedProperties,
      });

      summaryPoints.push({
        longitude,
        latitude,
        ticketCount: count,
        totalRevenue,
        location: location || streetName,
        streetName,
        yearMask,
        monthMask,
      });

      spatialIndex.add(longitude, latitude, longitude, latitude);
    }

    spatialIndex.finish();

    const clusterIndex = new Supercluster({
      minZoom: 0,
      maxZoom: 16,
      radius: 60,
      extent: 4096,
      map: (properties) => ({
        ...properties,
        ticketCount: properties.count,
        yearMask: properties.yearMask ?? 0,
        monthMask: properties.monthMask ?? 0,
      }),
      reduce: (acc, props) => {
        acc.total_revenue = (acc.total_revenue || 0) + (props.total_revenue || 0);
        acc.count = (acc.count || 0) + (props.count || props.ticketCount || 0);
        acc.ticketCount = (acc.ticketCount || 0) + (props.ticketCount || props.count || 0);
        acc.yearMask = (acc.yearMask || 0) | (props.yearMask || 0);
        acc.monthMask = (acc.monthMask || 0) | (props.monthMask || 0);
      },
    });

    clusterIndex.load(sanitized);

    const pointIndex = geojsonvt({ type: 'FeatureCollection', features: sanitized }, {
      maxZoom: 16,
      extent: 4096,
      buffer: 64,
      indexMaxZoom: 16,
      indexMaxPoints: 0,
    });

    this.clusterIndex = clusterIndex;
    this.pointIndex = pointIndex;
    this.summaryPoints = summaryPoints;
    this.summaryTree = spatialIndex;
    this.lastMtimeMs = fileInfo.mtimeMs;
    this.tileCache.clear();
  }

  async getTile(z, x, y) {
    await this.ensureLoaded();
    if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y)) {
      return null;
    }
    const key = buildTileKey(z, x, y);
    if (this.tileCache.has(key)) {
      return this.tileCache.get(key);
    }

    const layers = {};

    if (z < RAW_POINT_ZOOM_THRESHOLD) {
      const clusterTile = this.clusterIndex.getTile(z, x, y);
      if (clusterTile) {
        hydrateTemporalTags(clusterTile);
        layers[TILE_LAYER_NAME] = clusterTile;
      }
    } else {
      const pointTile = this.pointIndex.getTile(z, x, y);
      if (pointTile) {
        hydrateTemporalTags(pointTile);
        layers[TILE_LAYER_NAME] = pointTile;
      }
    }

    if (Object.keys(layers).length === 0) {
      return null;
    }

    const buffer = vtpbf.fromGeojsonVt(layers, { extent: 4096 });
    this.tileCache.set(key, buffer);
    if (this.tileCache.size > TILE_CACHE_LIMIT) {
      const oldestKey = this.tileCache.keys().next().value;
      this.tileCache.delete(oldestKey);
    }
    return buffer;
  }

  async summarizeViewport({ west, south, east, north, zoom, filters }) {
    await this.ensureLoaded();
    const numericZoom = Number(zoom);
    if (!Number.isFinite(numericZoom) || numericZoom < SUMMARY_ZOOM_THRESHOLD) {
      return {
        zoomRestricted: true,
        topStreets: [],
      };
    }

    if (!this.summaryTree || !this.summaryPoints.length) {
      return {
        zoomRestricted: false,
        visibleCount: 0,
        visibleRevenue: 0,
        topStreets: [],
      };
    }

    const indices = this.summaryTree.search(west, south, east, north);
    let visibleCount = 0;
    let visibleRevenue = 0;
    const streetMap = new Map();

    for (const index of indices) {
      const point = this.summaryPoints[index];
      if (!point) {
        continue;
      }
      if (filters?.year && !yearMaskIncludes(point.yearMask, filters.year)) {
        continue;
      }
      if (filters?.month && !monthMaskIncludes(point.monthMask, filters.month)) {
        continue;
      }

      visibleCount += point.ticketCount;
      visibleRevenue += point.totalRevenue;

      const streetKey = point.streetName || point.location;
      if (!streetMap.has(streetKey)) {
        streetMap.set(streetKey, {
          name: streetKey,
          ticketCount: 0,
          totalRevenue: 0,
          sampleLocation: point.location,
        });
      }
      const streetEntry = streetMap.get(streetKey);
      streetEntry.ticketCount += point.ticketCount;
      streetEntry.totalRevenue += point.totalRevenue;
    }

    const topStreets = Array.from(streetMap.values())
      .sort((a, b) => (b.totalRevenue || 0) - (a.totalRevenue || 0))
      .slice(0, SUMMARY_LIMIT)
      .map((entry) => ({
        ...entry,
        totalRevenue: Number(entry.totalRevenue.toFixed(2)),
      }));

    return {
      zoomRestricted: false,
      visibleCount,
      visibleRevenue: Number(visibleRevenue.toFixed(2)),
      topStreets,
    };
  }

  async getViewportPoints({ west, south, east, north, limit = 5000, filters }) {
    await this.ensureLoaded();
    if (!this.summaryTree || !this.summaryPoints.length) {
      return [];
    }

    const indices = this.summaryTree.search(west, south, east, north);
    if (!indices || indices.length === 0) {
      return [];
    }

    const maxPoints = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 20000) : 5000;
    const result = [];

    for (let i = 0; i < indices.length; i += 1) {
      const point = this.summaryPoints[indices[i]];
      if (!point) {
        continue;
      }

      if (filters?.year && !yearMaskIncludes(point.yearMask, filters.year)) {
        continue;
      }
      if (filters?.month && !monthMaskIncludes(point.monthMask, filters.month)) {
        continue;
      }

      result.push({
        longitude: point.longitude,
        latitude: point.latitude,
        count: point.ticketCount,
      });

      if (result.length >= maxPoints) {
        break;
      }
    }

    return result;
  }

  async getClusterExpansionZoom(clusterId) {
    await this.ensureLoaded();
    if (!this.clusterIndex || !Number.isFinite(clusterId)) {
      return null;
    }
    try {
      return this.clusterIndex.getClusterExpansionZoom(clusterId);
    } catch (error) {
      console.warn('Failed to resolve cluster expansion zoom', error.message);
      return null;
    }
  }
}

export function createTileService() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const dataFile = path.resolve(__dirname, '../public/data/tickets_aggregated.geojson');
  return new TileService(dataFile);
}
