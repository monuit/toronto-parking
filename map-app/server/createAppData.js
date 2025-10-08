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

  const streetEntries = Object.entries(streetStats).map(([name, stats]) => {
    const ticketCount = Number(stats.ticketCount) || 0;
    const totalRevenue = Number(stats.totalRevenue ?? stats.totalFines ?? 0) || 0;
    const neighbourhoods = Array.isArray(stats.neighbourhoods)
      ? stats.neighbourhoods.filter((value) => value && value !== 'Unknown')
      : [];
    return {
      name,
      ticketCount,
      totalRevenue,
      neighbourhoods,
      sampleLocation: stats.sampleLocation || name,
      topInfraction: stats.topInfraction || null,
    };
  });

  const neighbourhoodMap = new Map();
  for (const [name, stats] of Object.entries(neighbourhoodStats)) {
    neighbourhoodMap.set(name, {
      name,
      count: Number(stats.count) || 0,
      totalRevenue: Number(stats.totalFines) || 0,
    });
  }

  const topStreets = streetEntries
    .sort((a, b) => (b.totalRevenue || 0) - (a.totalRevenue || 0))
    .slice(0, 10)
    .map((entry) => ({
      ...entry,
      totalRevenue: Number(entry.totalRevenue.toFixed(2)),
    }));

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
