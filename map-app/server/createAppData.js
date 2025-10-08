import { normalizeStreetName } from '../shared/streetUtils.js';
import {
  loadTicketsSummary,
  loadStreetStats,
  loadNeighbourhoodStats,
} from './ticketsDataStore.js';

let cachedSnapshot = null;

function mapToSerializableList(map, transform, sortKey = 'totalRevenue') {
  return Array.from(map.values())
    .map(transform)
    .sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0));
}

export async function createAppData() {
  const summaryResult = await loadTicketsSummary();
  const streetStatsResult = await loadStreetStats();
  const neighbourhoodStatsResult = await loadNeighbourhoodStats();

  const version = summaryResult?.version ?? null;

  if (cachedSnapshot && version !== null && cachedSnapshot.version === version) {
    return cachedSnapshot.payload;
  }

  const summary = summaryResult?.data || { featureCount: 0, ticketCount: 0, totalRevenue: 0 };
  const streetStats = streetStatsResult?.data || {};
  const neighbourhoodStats = neighbourhoodStatsResult?.data || {};

  const totals = {
    featureCount: Number(summary.featureCount) || 0,
    ticketCount: Number(summary.ticketCount) || 0,
    totalRevenue: Number(summary.totalRevenue) || 0,
  };

  const streetMap = new Map();
  for (const [location, stats] of Object.entries(streetStats)) {
    const streetKey = normalizeStreetName(location);
    if (!streetKey) {
      continue;
    }

    const ticketCount = Number(stats.ticketCount) || 0;
    const totalRevenue = Number(stats.totalRevenue ?? stats.totalFines ?? 0) || 0;
    const neighbourhoodValues = new Set();
    if (Array.isArray(stats.neighbourhoods)) {
      for (const value of stats.neighbourhoods) {
        if (value && value !== 'Unknown') {
          neighbourhoodValues.add(value);
        }
      }
    } else if (stats.neighbourhood && stats.neighbourhood !== 'Unknown') {
      neighbourhoodValues.add(stats.neighbourhood);
    }

    let entry = streetMap.get(streetKey);
    if (!entry) {
      entry = {
        name: streetKey,
        ticketCount: 0,
        totalRevenue: 0,
        neighbourhoods: new Set(),
        sampleLocation: location,
        _sampleTicketCount: 0,
        topInfraction: null,
      };
      streetMap.set(streetKey, entry);
    }

    entry.ticketCount += ticketCount;
    entry.totalRevenue += totalRevenue;
    for (const neighbourhood of neighbourhoodValues) {
      entry.neighbourhoods.add(neighbourhood);
    }

    if (stats.topInfraction && !entry.topInfraction) {
      entry.topInfraction = stats.topInfraction;
    }

    if (ticketCount > entry._sampleTicketCount) {
      entry._sampleTicketCount = ticketCount;
      entry.sampleLocation = location;
      if (stats.topInfraction) {
        entry.topInfraction = stats.topInfraction;
      }
    }
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
    topInfraction: entry.topInfraction || null,
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

  if (version !== null) {
    cachedSnapshot = {
      version,
      payload,
    };
  } else {
    cachedSnapshot = null;
  }

  return payload;
}
