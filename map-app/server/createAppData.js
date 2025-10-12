import process from 'node:process';

import {
  loadTicketsSummary,
  loadStreetStats,
  loadNeighbourhoodStats,
  loadDatasetSummary,
  loadCameraWardSummary,
} from './ticketsDataStore.js';
import { getDatasetYears } from './yearlyMetricsService.js';
import { getDatasetTotals } from './datasetTotalsService.js';

const CACHE_TTL_MS = Number.isFinite(Number.parseInt(process.env.APP_DATA_CACHE_MS || '', 10))
  ? Number.parseInt(process.env.APP_DATA_CACHE_MS, 10)
  : 600_000;

let cachedSnapshot = null;
let lastSnapshotMeta = {
  fromCache: false,
  refreshedAt: null,
};

function mapToSerializableList(map, transform, sortKey = 'totalRevenue') {
  return Array.from(map.values())
    .map(transform)
    .sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0));
}

export async function createAppData(options = {}) {
  const { bypassCache = false } = options;
  const now = Date.now();

  if (!bypassCache && cachedSnapshot && cachedSnapshot.expiresAt > now && cachedSnapshot.payload) {
    lastSnapshotMeta = {
      fromCache: true,
      refreshedAt: new Date().toISOString(),
    };
    return cachedSnapshot.payload;
  }

  const [
    summaryResult,
    streetStatsResult,
    neighbourhoodStatsResult,
    redLightSummaryResult,
    aseSummaryResult,
    redLightWardSummaryResult,
    aseWardSummaryResult,
    combinedWardSummaryResult,
  ] = await Promise.all([
    loadTicketsSummary(),
    loadStreetStats(),
    loadNeighbourhoodStats(),
    loadDatasetSummary('red_light_locations'),
    loadDatasetSummary('ase_locations'),
    loadCameraWardSummary('red_light_locations'),
    loadCameraWardSummary('ase_locations'),
    loadCameraWardSummary('cameras_combined'),
  ]);

  const [parkingLiveTotals, redLiveTotals, aseLiveTotals] = await Promise.all([
    getDatasetTotals('parking_tickets').catch(() => null),
    getDatasetTotals('red_light_locations').catch(() => null),
    getDatasetTotals('ase_locations').catch(() => null),
  ]);

  const versionComponents = [
    summaryResult?.version ?? null,
    streetStatsResult?.version ?? null,
    neighbourhoodStatsResult?.version ?? null,
    redLightSummaryResult?.version ?? null,
    aseSummaryResult?.version ?? null,
    redLightWardSummaryResult?.version ?? null,
    aseWardSummaryResult?.version ?? null,
    combinedWardSummaryResult?.version ?? null,
  ];

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

  const normalizeTotals = (source) => {
    if (!source) {
      return null;
    }
    const locationCount = Number(source.locationCount ?? source.featureCount ?? 0) || 0;
    const ticketCount = Number(source.ticketCount ?? source.featureCount ?? 0) || 0;
    const totalRevenue = Number(Number(source.totalRevenue ?? 0).toFixed(2));
    return { locationCount, ticketCount, totalRevenue };
  };

  const parkingCurrentTotals = parkingLiveTotals
    ? {
        featureCount: Number(parkingLiveTotals.featureCount ?? parkingLiveTotals.ticketCount ?? 0),
        ticketCount: Number(parkingLiveTotals.ticketCount ?? parkingLiveTotals.featureCount ?? 0),
        totalRevenue: Number(Number(parkingLiveTotals.totalRevenue ?? 0).toFixed(2)),
      }
    : {
        featureCount: totals.featureCount,
        ticketCount: totals.ticketCount,
        totalRevenue: Number(totals.totalRevenue.toFixed(2)),
      };

  const parkingLegacyTotals = {
    featureCount: totals.featureCount,
    ticketCount: totals.ticketCount,
    totalRevenue: Number(Number(totals.totalRevenue ?? 0).toFixed(2)),
  };

  const datasets = {
    parking_tickets: {
      totals: parkingCurrentTotals,
      summary,
      topStreets,
      topNeighbourhoods,
      sources: {
        summary: summaryResult?.source || 'unknown',
        streetStats: streetStatsResult?.source || 'unknown',
        neighbourhoodStats: neighbourhoodStatsResult?.source || 'unknown',
      },
    },
  };

  datasets.parking_tickets.legacyTotals = parkingLegacyTotals;

  if (redLightSummaryResult?.data) {
    const redSummary = redLightSummaryResult.data;
    const redTotals = redSummary?.totals || {};
    const redLegacyTotals = {
      locationCount: Number(redTotals.locationCount) || 0,
      ticketCount: Number(redTotals.ticketCount) || 0,
      totalRevenue: Number(Number(redTotals.totalRevenue ?? 0).toFixed(2)),
    };
    const redLive = redLiveTotals
      ? {
          locationCount: Number(redLiveTotals.locationCount ?? redLiveTotals.featureCount ?? 0) || 0,
          ticketCount: Number(redLiveTotals.ticketCount) || 0,
          totalRevenue: Number(Number(redLiveTotals.totalRevenue ?? 0).toFixed(2)),
        }
      : null;
    const redRevenue = Number((redLive?.totalRevenue ?? redLegacyTotals.totalRevenue) || 0);
    datasets.red_light_locations = {
      totals: {
        locationCount: redLive?.locationCount ?? redLegacyTotals.locationCount,
        ticketCount: redLive?.ticketCount ?? redLegacyTotals.ticketCount,
        totalRevenue: Number(redRevenue.toFixed(2)),
      },
      summary: redSummary,
      topLocations: Array.isArray(redSummary.topLocations)
        ? redSummary.topLocations.slice(0, 10)
        : [],
      topGroups: redSummary.topGroups || {},
      locationsById: redSummary.locationsById || {},
      sources: {
        summary: redLightSummaryResult.source || 'unknown',
      },
    };
    datasets.red_light_locations.legacyTotals = redLegacyTotals;
    if (redLightWardSummaryResult?.data) {
      datasets.red_light_locations.wardSummary = redLightWardSummaryResult.data;
      datasets.red_light_locations.sources.wardSummary = redLightWardSummaryResult.source || 'unknown';
    }
  }

  if (aseSummaryResult?.data) {
    const aseSummary = aseSummaryResult.data;
    const aseTotals = aseSummary?.totals || {};
    const aseLegacyTotals = {
      locationCount: Number(aseTotals.locationCount) || 0,
      ticketCount: Number(aseTotals.ticketCount) || 0,
      totalRevenue: Number(Number(aseTotals.totalRevenue ?? 0).toFixed(2)),
    };
    const aseWardTotals = normalizeTotals(aseWardSummaryResult?.data?.totals || null);
    const aseLive = normalizeTotals(aseLiveTotals);
    const aseDisplayTotals = aseLive && aseLive.ticketCount > 0
      ? aseLive
      : aseWardTotals && aseWardTotals.ticketCount > 0
        ? aseWardTotals
        : aseLegacyTotals;
    datasets.ase_locations = {
      totals: {
        locationCount: aseDisplayTotals.locationCount,
        ticketCount: aseDisplayTotals.ticketCount,
        totalRevenue: aseDisplayTotals.totalRevenue,
      },
      summary: aseSummary,
      topLocations: Array.isArray(aseSummary.topLocations)
        ? aseSummary.topLocations.slice(0, 10)
        : [],
      topGroups: aseSummary.topGroups || {},
      statusBreakdown: Array.isArray(aseSummary.statusBreakdown) ? aseSummary.statusBreakdown : [],
      locationsById: aseSummary.locationsById || {},
      sources: {
        summary: aseSummaryResult.source || 'unknown',
      },
    };
    datasets.ase_locations.legacyTotals = aseLegacyTotals;
    if (aseWardSummaryResult?.data) {
      datasets.ase_locations.wardSummary = aseWardSummaryResult.data;
      datasets.ase_locations.sources.wardSummary = aseWardSummaryResult.source || 'unknown';
    }
  }

  if (combinedWardSummaryResult?.data) {
    const combinedSummary = combinedWardSummaryResult.data;
    const combinedTotals = combinedSummary?.totals || {};
    const combinedRevenue = Number(combinedTotals.totalRevenue ?? 0);
    datasets.cameras_combined = {
      totals: {
        ticketCount: Number(combinedTotals.ticketCount) || 0,
        locationCount: Number(combinedTotals.locationCount) || 0,
        totalRevenue: Number(combinedRevenue.toFixed(2)),
      },
      wardSummary: combinedSummary,
      sources: {
        wardSummary: combinedWardSummaryResult.source || 'unknown',
      },
      breakdown: {
        ase: {
          ticketCount: Number(datasets.ase_locations?.totals?.ticketCount) || 0,
          locationCount: Number(datasets.ase_locations?.totals?.locationCount) || 0,
          totalRevenue: Number(datasets.ase_locations?.totals?.totalRevenue) || 0,
        },
        redLight: {
          ticketCount: Number(datasets.red_light_locations?.totals?.ticketCount) || 0,
          locationCount: Number(datasets.red_light_locations?.totals?.locationCount) || 0,
          totalRevenue: Number(datasets.red_light_locations?.totals?.totalRevenue) || 0,
        },
      },
    };
  }

  const [parkingYears, redLightYears, aseYears] = await Promise.all([
    getDatasetYears('parking_tickets').catch(() => []),
    getDatasetYears('red_light_locations').catch(() => []),
    getDatasetYears('ase_locations').catch(() => []),
  ]);

  versionComponents.push(
    Array.isArray(parkingYears) ? `parking:${parkingYears.join(',')}` : 'parking:null',
    Array.isArray(redLightYears) ? `red:${redLightYears.join(',')}` : 'red:null',
    Array.isArray(aseYears) ? `ase:${aseYears.join(',')}` : 'ase:null',
  );

  const hasVersion = versionComponents.some((value) => value !== null && value !== undefined);
  const version = hasVersion
    ? versionComponents.map((value) => (value === null ? 'null' : String(value))).join('|')
    : null;

  if (!bypassCache && cachedSnapshot && version !== null && cachedSnapshot.version === version && cachedSnapshot.payload) {
    cachedSnapshot.expiresAt = now + CACHE_TTL_MS;
    lastSnapshotMeta = {
      fromCache: true,
      refreshedAt: new Date().toISOString(),
    };
    return cachedSnapshot.payload;
  }

  const payload = {
    totals: {
      featureCount: parkingCurrentTotals.featureCount,
      ticketCount: parkingCurrentTotals.ticketCount,
      totalRevenue: parkingCurrentTotals.totalRevenue,
    },
    topStreets,
    topNeighbourhoods,
    datasets,
    generatedAt: new Date().toISOString(),
    yearlyMeta: {
      parking_tickets: parkingYears,
      red_light_locations: redLightYears,
      ase_locations: aseYears,
    },
  };

  if (payload.datasets?.red_light_locations?.locationsById) {
    payload.datasets.red_light_locations.hasLocationIndex = true;
    payload.datasets.red_light_locations.locationsById = null;
  }
  if (payload.datasets?.ase_locations?.locationsById) {
    payload.datasets.ase_locations.hasLocationIndex = true;
    payload.datasets.ase_locations.locationsById = null;
  }

  if (version !== null) {
    cachedSnapshot = {
      version,
      payload,
      expiresAt: now + CACHE_TTL_MS,
    };
  } else {
    cachedSnapshot = {
      version: null,
      payload,
      expiresAt: now + CACHE_TTL_MS,
    };
  }

  lastSnapshotMeta = {
    fromCache: false,
    refreshedAt: new Date().toISOString(),
  };

  return payload;
}

export function getLatestAppDataMeta() {
  return { ...lastSnapshotMeta };
}
