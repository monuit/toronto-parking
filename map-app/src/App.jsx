/**
 * Main App component
 * Single responsibility: coordinate all map components and state
 */
import { useState, useCallback, useMemo, useEffect, useRef, lazy, Suspense } from 'react';
import { AppDataProvider, useAppData } from './context/AppDataContext.jsx';
import { WardDataProvider, useWardData } from './context/WardDataContext.jsx';
import { useCentrelineLookup } from './context/CentrelineContext.jsx';
import { StatsSummary } from './components/StatsSummary.jsx';
import { StreetLeaderboard } from './components/StreetLeaderboard.jsx';
import { NeighbourhoodLeaderboard } from './components/NeighbourhoodLeaderboard.jsx';
import { Legend } from './components/Legend.jsx';
import { InfoPopup } from './components/InfoPopup.jsx';
import { HowItWorks } from './components/HowItWorks.jsx';
import { ViewportInsights } from './components/ViewportInsights.jsx';
import { DatasetToggle } from './components/DatasetToggle.jsx';
import { YearFilter } from './components/YearFilter.jsx';
import { MobileHeader } from './components/MobileHeader.jsx';
import { MobileDrawer } from './components/MobileDrawer.jsx';
import { MobileAccordion } from './components/MobileAccordion.jsx';
import { MAP_CONFIG } from './lib/mapSources.js';
import { prefetchCameraDatasets } from './lib/cameraDatasetLoader.js';
import { prefetchGlowDatasets } from './lib/glowDatasetLoader.js';
import { useBreakpoint, useTouchDevice } from './hooks/useBreakpoint.js';
import { PmtilesProvider } from './context/PmtilesContext.jsx';
import './App.css';
import './styles/MobileLayout.css';

const WardLeaderboard = lazy(() => import('./components/WardLeaderboard.jsx'));
const WardModeToggle = lazy(() => import('./components/WardModeToggle.jsx'));
const WardHoverPopup = lazy(() => import('./components/WardHoverPopup.jsx'));

const POPUP_SHEET_BREAKPOINT = 640;
const POPUP_SIDE_BREAKPOINT = 1024;

function getPopupVariantForWidth(width) {
  if (!Number.isFinite(width)) {
    return 'floating';
  }
  if (width <= POPUP_SHEET_BREAKPOINT) {
    return 'sheet';
  }
  if (width <= POPUP_SIDE_BREAKPOINT) {
    return 'side';
  }
  return 'floating';
}

const MapExperience = lazy(() => import('./components/MapExperience.jsx'));

function AppContent({
  fallbackTopStreets = [],
  fallbackTopNeighbourhoods = [],
}) {
  const appData = useAppData();
  const contextTotals = appData?.totals || null;
  const datasetsValue = appData?.datasets;
  const datasetSnapshots = useMemo(
    () => (datasetsValue && typeof datasetsValue === 'object' ? datasetsValue : {}),
    [datasetsValue],
  );
  const [dataset, setDataset] = useState('parking_tickets');
  const { getDataset: getWardDataset, preloadDataset: preloadWardDataset } = useWardData();
  const [viewModes, setViewModes] = useState({
    red_light_locations: 'detail',
    ase_locations: 'detail',
  });
  const [wardDatasetSelections, setWardDatasetSelections] = useState({
    red_light_locations: 'red_light_locations',
    ase_locations: 'ase_locations',
  });
  const activeViewMode = viewModes[dataset] || 'detail';
  const activeWardDataset = activeViewMode === 'ward'
    ? (wardDatasetSelections[dataset] || dataset)
    : null;
  const legendDataset = useMemo(
    () => (activeViewMode === 'ward' && activeWardDataset ? activeWardDataset : dataset),
    [activeViewMode, activeWardDataset, dataset],
  );
  const yearlyMeta = appData?.yearlyMeta || {};
  const initialYearsByDataset = useMemo(() => {
    const parkingYears = yearlyMeta?.parking_tickets;
    const redLightYears = yearlyMeta?.red_light_locations;
    const aseYears = yearlyMeta?.ase_locations;
    return {
      parking_tickets: Array.isArray(parkingYears) ? parkingYears : [],
      red_light_locations: Array.isArray(redLightYears) ? redLightYears : [],
      ase_locations: Array.isArray(aseYears) ? aseYears : [],
    };
  }, [yearlyMeta?.parking_tickets, yearlyMeta?.red_light_locations, yearlyMeta?.ase_locations]);
  const [yearsByDataset, setYearsByDataset] = useState(initialYearsByDataset);
  useEffect(() => {
    setYearsByDataset((previous) => ({ ...previous, ...initialYearsByDataset }));
  }, [initialYearsByDataset]);
  const [yearSelections, setYearSelections] = useState({});
  const [yearlySnapshots, setYearlySnapshots] = useState({});
  const [yearLoadingState, setYearLoadingState] = useState({});
  const [legacyTotalsMode, setLegacyTotalsMode] = useState({
    parking_tickets: false,
    red_light_locations: false,
    ase_locations: false,
  });
  const isLegacyToggleAllowed = useCallback((key) => (
    key === 'parking_tickets' || key === 'red_light_locations'
  ), []);

  const handleLegacyTotalsToggle = useCallback((key, nextValue) => {
    if (!isLegacyToggleAllowed(key)) {
      return;
    }
    setLegacyTotalsMode((previous) => {
      if (previous[key] === nextValue) {
        return previous;
      }
      return {
        ...previous,
        [key]: nextValue,
      };
    });
  }, [isLegacyToggleAllowed]);
  const activeYear = yearSelections?.[dataset] ?? null;
  const existingYearSnapshot = activeYear !== null
    ? (yearlySnapshots?.[dataset]?.[activeYear] ?? null)
    : null;

  const setYearSelection = useCallback((targetDataset, value) => {
    setYearSelections((previous) => {
      if (previous[targetDataset] === value) {
        return previous;
      }
      return { ...previous, [targetDataset]: value };
    });
  }, []);

  const handleViewModeChange = useCallback((mode) => {
    if (!['red_light_locations', 'ase_locations'].includes(dataset)) {
      return;
    }
    setViewModes((previous) => {
      if (previous[dataset] === mode) {
        return previous;
      }
      return { ...previous, [dataset]: mode };
    });
  }, [dataset]);

  const handleWardDatasetChange = useCallback((targetDataset) => {
    if (!['red_light_locations', 'ase_locations', 'cameras_combined'].includes(targetDataset)) {
      return;
    }
    setWardDatasetSelections((previous) => {
      if (previous[dataset] === targetDataset) {
        return previous;
      }
      return { ...previous, [dataset]: targetDataset };
    });
  }, [dataset]);

  const handleYearChange = useCallback((value) => {
    setYearSelection(dataset, value);
  }, [dataset, setYearSelection]);

  const updateYearLoadingState = useCallback((targetDataset, yearValue, isLoading) => {
    const key = `${targetDataset}:${yearValue}`;
    setYearLoadingState((previous) => {
      if (previous[key] === isLoading) {
        return previous;
      }
      return { ...previous, [key]: isLoading };
    });
  }, []);

  useEffect(() => {
    if ((yearsByDataset[dataset] || []).length > 0) {
      return;
    }
    let cancelled = false;
    const params = new URLSearchParams({ dataset });
    fetch(`${MAP_CONFIG.API_PATHS.YEARLY_YEARS}?${params.toString()}`)
      .then((response) => (response.ok ? response.json() : null))
      .then((payload) => {
        if (!cancelled && payload?.years) {
          setYearsByDataset((previous) => ({
            ...previous,
            [dataset]: Array.isArray(payload.years) ? payload.years : [],
          }));
        }
      })
      .catch((error) => {
        console.error('Failed to load available years', error);
      });
    return () => {
      cancelled = true;
    };
  }, [dataset, yearsByDataset]);

  useEffect(() => {
    if (activeYear === null || existingYearSnapshot) {
      return;
    }

    let cancelled = false;
    updateYearLoadingState(dataset, activeYear, true);

    const totalsParams = new URLSearchParams({ dataset, year: String(activeYear) });
    const totalsPromise = fetch(`${MAP_CONFIG.API_PATHS.YEARLY_TOTALS}?${totalsParams.toString()}`)
      .then((response) => (response.ok ? response.json() : null))
      .catch((error) => {
        console.error('Failed to load yearly totals', error);
        return null;
      });

    if (dataset === 'parking_tickets') {
      const streetsParams = new URLSearchParams({ year: String(activeYear) });
      const neighbourhoodParams = new URLSearchParams({ year: String(activeYear) });

      Promise.all([
        totalsPromise,
        fetch(`${MAP_CONFIG.API_PATHS.YEARLY_TOP_STREETS}?${streetsParams.toString()}`)
          .then((response) => (response.ok ? response.json() : null))
          .catch((error) => {
            console.error('Failed to load yearly street rankings', error);
            return null;
          }),
        fetch(`${MAP_CONFIG.API_PATHS.YEARLY_TOP_NEIGHBOURHOODS}?${neighbourhoodParams.toString()}`)
          .then((response) => (response.ok ? response.json() : null))
          .catch((error) => {
            console.error('Failed to load yearly neighbourhood rankings', error);
            return null;
          }),
      ]).then(([totalsPayload, streetsPayload, neighbourhoodPayload]) => {
        if (cancelled) {
          return;
        }
        const totals = totalsPayload ? {
          ticketCount: Number(totalsPayload.ticketCount ?? 0),
          totalRevenue: Number(totalsPayload.totalRevenue ?? 0),
          locationCount: Number(totalsPayload.locationCount ?? 0),
        } : null;
        const topStreets = Array.isArray(streetsPayload?.items) ? streetsPayload.items : [];
        const topNeighbourhoods = Array.isArray(neighbourhoodPayload?.items) ? neighbourhoodPayload.items : [];
        setYearlySnapshots((previous) => ({
          ...previous,
          [dataset]: {
            ...(previous[dataset] || {}),
            [activeYear]: {
              totals,
              topStreets,
              topNeighbourhoods,
            },
          },
        }));
      }).finally(() => {
        if (!cancelled) {
          updateYearLoadingState(dataset, activeYear, false);
        }
      });
    } else {
      const aggregatedParams = new URLSearchParams({ dataset, year: String(activeYear) });
      Promise.all([
        totalsPromise,
        fetch(`${MAP_CONFIG.API_PATHS.YEARLY_TOP_LOCATIONS}?${aggregatedParams.toString()}`)
          .then((response) => (response.ok ? response.json() : null))
          .catch((error) => {
            console.error('Failed to load yearly location rankings', error);
            return null;
          }),
        fetch(`${MAP_CONFIG.API_PATHS.YEARLY_TOP_GROUPS}?${aggregatedParams.toString()}`)
          .then((response) => (response.ok ? response.json() : null))
          .catch((error) => {
            console.error('Failed to load yearly group rankings', error);
            return null;
          }),
      ]).then(([totalsPayload, locationsPayload, groupsPayload]) => {
        if (cancelled) {
          return;
        }
        const totals = totalsPayload ? {
          ticketCount: Number(totalsPayload.ticketCount ?? 0),
          totalRevenue: Number(totalsPayload.totalRevenue ?? 0),
          locationCount: Number(totalsPayload.locationCount ?? 0),
        } : null;
        const topLocations = Array.isArray(locationsPayload?.items) ? locationsPayload.items : [];
        const topGroups = Array.isArray(groupsPayload?.items) ? groupsPayload.items : [];
        setYearlySnapshots((previous) => ({
          ...previous,
          [dataset]: {
            ...(previous[dataset] || {}),
            [activeYear]: {
              totals,
              topLocations,
              topGroups,
            },
          },
        }));
      }).finally(() => {
        if (!cancelled) {
          updateYearLoadingState(dataset, activeYear, false);
        }
      });
    }

    return () => {
      cancelled = true;
      updateYearLoadingState(dataset, activeYear, false);
    };
  }, [dataset, activeYear, existingYearSnapshot, updateYearLoadingState]);

  useEffect(() => {
    if (activeViewMode !== 'ward' || !activeWardDataset) {
      return undefined;
    }
    preloadWardDataset(activeWardDataset);
    return undefined;
  }, [activeViewMode, activeWardDataset, preloadWardDataset]);
  const [map, setMap] = useState(null);
  const [activeCentrelineIds, setActiveCentrelineIds] = useState([]);
  const [activeTab, setActiveTab] = useState('streets');
  const [popupData, setPopupData] = useState(null);
  const [popupPosition, setPopupPosition] = useState(null);
  const [wardHoverInfo, setWardHoverInfo] = useState(null);
  const [viewportSummary, setViewportSummary] = useState({ zoomRestricted: true, topStreets: [] });
  const [totalsByDataset, setTotalsByDataset] = useState({});
  const [isClient, setIsClient] = useState(false);
  const [isOverlayCollapsed, setIsOverlayCollapsed] = useState(false);
  const datasetTotalsWarningRef = useRef(false);
  const previousYearTotalsRef = useRef(null);
  const [popupVariant, setPopupVariant] = useState(() => {
    if (typeof window === 'undefined') {
      return 'floating';
    }
    return getPopupVariantForWidth(window.innerWidth);
  });
  const isMobile = useBreakpoint(768);
  const isTouchDevice = useTouchDevice();
  const [isDrawerOpen, setDrawerOpen] = useState(false);
  const [isLegendSheetOpen, setLegendSheetOpen] = useState(false);
  const [isInsightsSheetOpen, setInsightsSheetOpen] = useState(false);
  const { getStreetSummary, getCentrelineDetail } = useCentrelineLookup();
  const datasetEntry = useMemo(
    () => (datasetSnapshots && datasetSnapshots[dataset]) || {},
    [datasetSnapshots, dataset],
  );
  const aseWardWards = datasetSnapshots?.ase_locations?.wardSummary?.wards;
  const redWardWards = datasetSnapshots?.red_light_locations?.wardSummary?.wards;
  const aseWardLookup = useMemo(() => {
    if (!Array.isArray(aseWardWards)) {
      return null;
    }
    const map = new Map();
    for (const ward of aseWardWards) {
      if (!ward) {
        continue;
      }
      const code = ward.wardCode ?? ward.ward_code;
      if (code !== undefined && code !== null) {
        map.set(String(code), ward);
      }
    }
    return map;
  }, [aseWardWards]);
  const redWardLookup = useMemo(() => {
    if (!Array.isArray(redWardWards)) {
      return null;
    }
    const map = new Map();
    for (const ward of redWardWards) {
      if (!ward) {
        continue;
      }
      const code = ward.wardCode ?? ward.ward_code;
      if (code !== undefined && code !== null) {
        map.set(String(code), ward);
      }
    }
    return map;
  }, [redWardWards]);
  const activeWardEntry = activeWardDataset ? getWardDataset(activeWardDataset) : null;
  const activeWardSummary = activeWardEntry?.summary || null;
  const wardTotals = activeWardSummary?.totals || null;
  const wardTopItems = useMemo(
    () => (Array.isArray(activeWardSummary?.topWards) ? activeWardSummary.topWards : []),
    [activeWardSummary?.topWards],
  );
  const isWardLoading = activeWardDataset ? Boolean(activeWardEntry?.loading) : false;
  const wardLeaderboardItems = useMemo(() => {
    if (!Array.isArray(wardTopItems)) {
      return [];
    }
    return wardTopItems.map((item) => item);
  }, [wardTopItems]);
  const datasetYears = yearsByDataset[dataset] || [];
  const yearLoadingKey = activeYear !== null ? `${dataset}:${activeYear}` : null;
  const isYearLoading = Boolean(yearLoadingKey && yearLoadingState[yearLoadingKey]);
  const yearlyDatasetSnapshot = activeYear !== null
    ? (yearlySnapshots?.[dataset]?.[activeYear] ?? null)
    : null;
  const streetInitialItems = useMemo(() => {
    if (dataset === 'parking_tickets') {
      return Array.isArray(fallbackTopStreets) ? fallbackTopStreets : [];
    }
    const items = Array.isArray(datasetEntry.topLocations) ? datasetEntry.topLocations : [];
    return items.slice(0, 10);
  }, [dataset, fallbackTopStreets, datasetEntry]);
  const neighbourhoodInitialItems = useMemo(() => {
    if (dataset === 'parking_tickets') {
      return Array.isArray(fallbackTopNeighbourhoods) ? fallbackTopNeighbourhoods : [];
    }
    const wards = datasetEntry?.topGroups?.wards;
    return Array.isArray(wards) ? wards.slice(0, 10) : [];
  }, [dataset, fallbackTopNeighbourhoods, datasetEntry]);
  const streetOverrideItems = useMemo(() => {
    if (activeYear === null || !yearlyDatasetSnapshot) {
      return null;
    }
    if (dataset === 'parking_tickets') {
      return Array.isArray(yearlyDatasetSnapshot.topStreets)
        ? yearlyDatasetSnapshot.topStreets
        : null;
    }
    return Array.isArray(yearlyDatasetSnapshot.topLocations)
      ? yearlyDatasetSnapshot.topLocations
      : null;
  }, [activeYear, yearlyDatasetSnapshot, dataset]);
  const neighbourhoodOverrideItems = useMemo(() => {
    if (activeYear === null || !yearlyDatasetSnapshot) {
      return null;
    }
    if (dataset === 'parking_tickets') {
      return Array.isArray(yearlyDatasetSnapshot.topNeighbourhoods)
        ? yearlyDatasetSnapshot.topNeighbourhoods
        : null;
    }
    return Array.isArray(yearlyDatasetSnapshot.topGroups)
      ? yearlyDatasetSnapshot.topGroups
      : null;
  }, [activeYear, yearlyDatasetSnapshot, dataset]);
  const statsTotalsOverride = useMemo(() => {
    if (activeYear === null || !yearlyDatasetSnapshot?.totals) {
      return null;
    }
    const totals = yearlyDatasetSnapshot.totals;
    return {
      locationCount: Number(totals.locationCount ?? 0),
      ticketCount: Number(totals.ticketCount ?? 0),
      totalRevenue: Number(totals.totalRevenue ?? 0),
    };
  }, [activeYear, yearlyDatasetSnapshot]);
  useEffect(() => {
    if (activeYear === null) {
      previousYearTotalsRef.current = null;
      return;
    }
    if (statsTotalsOverride) {
      previousYearTotalsRef.current = statsTotalsOverride;
    }
  }, [activeYear, statsTotalsOverride]);
  const streetsTabLabel = dataset === 'parking_tickets' ? 'Top Streets' : 'Top Locations';
  const neighbourhoodsTabLabel = dataset === 'parking_tickets' ? 'Top Neighbourhoods' : 'Top Wards';

  useEffect(() => {
    if (dataset === 'red_light_locations' || dataset === 'ase_locations') {
      preloadWardDataset(dataset);
    }
  }, [dataset, preloadWardDataset]);
  useEffect(() => {
    const updates = {};
    for (const [key, entry] of Object.entries(datasetSnapshots)) {
      if (entry?.totals) {
        updates[key] = { ...entry.totals };
      }
    }
    if (!updates.parking_tickets && contextTotals) {
      updates.parking_tickets = { ...contextTotals };
    }

    const keys = Object.keys(updates);
    if (keys.length === 0) {
      return;
    }

    setTotalsByDataset((previous) => {
      let next = previous;
      let mutated = false;
      for (const key of keys) {
        const payload = { ...updates[key], __source: 'context' };
        const existing = next[key];
        if (existing && existing.__source === 'api') {
          continue;
        }
        if (existing && existing.__source === 'context') {
          const same = Object.keys(payload).every((prop) => existing[prop] === payload[prop]);
          if (same) {
            continue;
          }
        }
        if (!mutated) {
          next = { ...next };
          mutated = true;
        }
        next[key] = payload;
      }
      return mutated ? next : next;
    });
  }, [datasetSnapshots, contextTotals]);

  useEffect(() => {
    let cancelled = false;
    const entry = totalsByDataset[dataset];
    if (entry && entry.__source === 'api') {
      return undefined;
    }

    fetch(`${MAP_CONFIG.API_PATHS.DATASET_TOTALS}?dataset=${dataset}`)
      .then((response) => (response.ok ? response.json() : null))
      .then((payload) => {
        if (!cancelled && payload) {
          setTotalsByDataset((previous) => {
            if (dataset === 'parking_tickets') {
              const contextTicketCount = Number(
                contextTotals?.ticketCount ?? contextTotals?.featureCount ?? 0,
              );
              const payloadTicketCount = Number(
                payload.ticketCount ?? payload.featureCount ?? 0,
              );
              if (contextTicketCount > 0 && payloadTicketCount === 0) {
                return previous;
              }
            }
            const contextEntry = datasetSnapshots[dataset];
            if (contextEntry?.totals) {
              const contextTicketCount = Number(
                contextEntry.totals.ticketCount ?? contextEntry.totals.featureCount ?? contextEntry.totals.locationCount ?? 0,
              );
              const payloadTicketCount = Number(
                payload.ticketCount ?? payload.featureCount ?? payload.locationCount ?? 0,
              );
              if (contextTicketCount > 0 && payloadTicketCount === 0) {
                return previous;
              }
            }
            return {
              ...previous,
              [dataset]: { ...payload, __source: 'api' },
            };
          });
        }
      })
      .catch((error) => {
        if (!datasetTotalsWarningRef.current) {
          datasetTotalsWarningRef.current = true;
          console.warn('Dataset totals API unavailable; using cached snapshot.', error?.message || error);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [dataset, totalsByDataset, contextTotals, datasetSnapshots]);

  const sanitizedTotalsByDataset = useMemo(() => {
    const next = {};
    for (const [key, value] of Object.entries(totalsByDataset)) {
      if (!value || typeof value !== 'object') {
        continue;
      }
      const { __source, ...rest } = value;
      next[key] = rest;
    }

    const wardTotalsOverrides = [
      ['red_light_locations', datasetSnapshots?.red_light_locations?.wardSummary?.totals],
      ['ase_locations', datasetSnapshots?.ase_locations?.wardSummary?.totals],
      ['cameras_combined', datasetSnapshots?.cameras_combined?.wardSummary?.totals],
    ];

    for (const [key, totals] of wardTotalsOverrides) {
      if (!totals || typeof totals !== 'object') {
        continue;
      }
      next[key] = {
        ...(next[key] || {}),
        ...totals,
      };
    }

    return next;
  }, [totalsByDataset, datasetSnapshots]);

  const legacyTotalsByDataset = useMemo(() => {
    const next = {};
    for (const [key, value] of Object.entries(datasetSnapshots || {})) {
      if (value?.legacyTotals) {
        next[key] = value.legacyTotals;
      } else if (value?.totals) {
        next[key] = value.totals;
      }
    }
    if (!next.parking_tickets && contextTotals) {
      next.parking_tickets = contextTotals;
    }
    return next;
  }, [datasetSnapshots, contextTotals]);

  const discrepancyByDataset = useMemo(() => {
    const importantKeys = ['parking_tickets', 'red_light_locations', 'ase_locations'];
    const toNumber = (value) => {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : 0;
    };
    const map = {};
    importantKeys.forEach((key) => {
      const current = sanitizedTotalsByDataset[key];
      const legacy = legacyTotalsByDataset[key];
      if (!current || !legacy) {
        return;
      }
      const currentTicket = toNumber(current.ticketCount ?? current.featureCount);
      const legacyTicket = toNumber(legacy.ticketCount ?? legacy.featureCount);
      const currentRevenue = toNumber(current.totalRevenue);
      const legacyRevenue = toNumber(legacy.totalRevenue);
      const currentLocations = toNumber(current.locationCount ?? current.featureCount);
      const legacyLocations = toNumber(legacy.locationCount ?? legacy.featureCount);
      const datasetSnapshot = datasetSnapshots?.[key] || {};
      const meta = datasetSnapshot?.meta || {};
      const entry = {
        current: {
          ticketCount: currentTicket,
          totalRevenue: currentRevenue,
          locationCount: currentLocations,
        },
        legacy: {
          ticketCount: legacyTicket,
          totalRevenue: legacyRevenue,
          locationCount: legacyLocations,
        },
        delta: {
          ticketCount: legacyTicket - currentTicket,
          totalRevenue: legacyRevenue - currentRevenue,
          locationCount: legacyLocations - currentLocations,
        },
      };
      if (key === 'ase_locations') {
        const unresolvedIds = Array.isArray(meta.historicalLocationsWithoutGeometry)
          ? meta.historicalLocationsWithoutGeometry.length
          : toNumber(meta.historicalLocationCount);
        const unresolvedTickets = toNumber(meta.historicalTicketCount);
        entry.note = {
          type: 'aseHistorical',
          resolvedLocations: currentLocations,
          unresolvedLocations: unresolvedIds,
          unresolvedTicketCount: unresolvedTickets,
          totalLocations: currentLocations + unresolvedIds,
          footnote: 'Legacy rotation codes without published coordinates remain aggregated in the charges spreadsheets provided by the city.',
        };
        entry.forceShow = true;
      } else if (key === 'red_light_locations') {
        const unresolvedCount = toNumber(meta.historicalLocationCount ?? (meta.historicalLocationCodes?.length ?? 0));
        const unresolvedTickets = toNumber(meta.historicalTicketCount);
        entry.note = {
          type: 'rlcHistorical',
          resolvedLocations: currentLocations,
          unresolvedLocations: unresolvedCount,
          unresolvedTicketCount: unresolvedTickets,
          totalLocations: currentLocations + unresolvedCount,
          footnote: 'Historic intersection codes without coordinates remain outstanding in the city releases and are tracked separately until geometry is provided.',
        };
        entry.forceShow = true;
      } else if (key === 'parking_tickets') {
        entry.delta.locationCount = 0;
        entry.note = {
          type: 'parkingLegacy',
          footnote: 'We are reconciling the older exports so both sources align without duplicates.',
        };
        entry.forceShow = true;
      }
      map[key] = entry;
    });
    return map;
  }, [sanitizedTotalsByDataset, legacyTotalsByDataset, datasetSnapshots]);

  const displayTotalsByDataset = useMemo(() => {
    const keys = new Set([
      ...Object.keys(legacyTotalsByDataset),
      ...Object.keys(sanitizedTotalsByDataset),
    ]);
    const result = {};
    keys.forEach((key) => {
      const useLegacy = legacyTotalsMode[key];
      const legacy = legacyTotalsByDataset[key];
      const current = sanitizedTotalsByDataset[key];
      if (useLegacy && legacy) {
        result[key] = legacy;
      } else if (current) {
        result[key] = current;
      } else if (legacy) {
        result[key] = legacy;
      }
    });
    const aseTotalsForCombined = legacyTotalsMode.ase_locations && legacyTotalsByDataset.ase_locations
      ? legacyTotalsByDataset.ase_locations
      : sanitizedTotalsByDataset.ase_locations;
    const redTotalsForCombined = legacyTotalsMode.red_light_locations && legacyTotalsByDataset.red_light_locations
      ? legacyTotalsByDataset.red_light_locations
      : sanitizedTotalsByDataset.red_light_locations;
    const hasCombinedBase = result.cameras_combined
      || sanitizedTotalsByDataset.cameras_combined
      || legacyTotalsByDataset.cameras_combined;
    const hasComponentOverrides = Boolean(aseTotalsForCombined || redTotalsForCombined);
    if (hasCombinedBase && hasComponentOverrides) {
      const base = result.cameras_combined
        || sanitizedTotalsByDataset.cameras_combined
        || legacyTotalsByDataset.cameras_combined
        || {};
      const toNumber = (value) => {
        const numeric = Number(value);
        return Number.isFinite(numeric) ? numeric : 0;
      };
      const aseTickets = toNumber(aseTotalsForCombined?.ticketCount ?? aseTotalsForCombined?.featureCount);
      const redTickets = toNumber(redTotalsForCombined?.ticketCount ?? redTotalsForCombined?.featureCount);
      const aseRevenue = toNumber(aseTotalsForCombined?.totalRevenue);
      const redRevenue = toNumber(redTotalsForCombined?.totalRevenue);
      const aseLocations = toNumber(aseTotalsForCombined?.locationCount ?? aseTotalsForCombined?.featureCount);
      const redLocations = toNumber(redTotalsForCombined?.locationCount ?? redTotalsForCombined?.featureCount);
      result.cameras_combined = {
        ...base,
        ticketCount: aseTickets + redTickets,
        totalRevenue: aseRevenue + redRevenue,
        locationCount: aseLocations + redLocations,
      };
    }
    return result;
  }, [legacyTotalsByDataset, sanitizedTotalsByDataset, legacyTotalsMode]);

  const resolveTotalsForDataset = useCallback(
    (key) => displayTotalsByDataset[key]
      || sanitizedTotalsByDataset[key]
      || legacyTotalsByDataset[key]
      || null,
    [displayTotalsByDataset, sanitizedTotalsByDataset, legacyTotalsByDataset],
  );

  const summaryDatasetKey = activeViewMode === 'ward' && activeWardDataset ? activeWardDataset : dataset;
  const summaryUseLegacy = isLegacyToggleAllowed(summaryDatasetKey)
    ? (legacyTotalsMode[summaryDatasetKey] ?? false)
    : false;
  const summaryLegacyToggleHandler = useMemo(() => {
    if (!isLegacyToggleAllowed(summaryDatasetKey)) {
      return null;
    }
    return (value) => {
      handleLegacyTotalsToggle(summaryDatasetKey, value);
    };
  }, [handleLegacyTotalsToggle, isLegacyToggleAllowed, summaryDatasetKey]);
  const currentTotals = resolveTotalsForDataset(dataset);
  const effectiveYearTotals = useMemo(() => {
    if (statsTotalsOverride) {
      return statsTotalsOverride;
    }
    if (activeYear !== null && isYearLoading && previousYearTotalsRef.current) {
      return previousYearTotalsRef.current;
    }
    return null;
  }, [statsTotalsOverride, activeYear, isYearLoading]);

  const statsTotalsToUse = useMemo(() => {
    if (activeViewMode === 'ward') {
      if (summaryUseLegacy) {
        const legacyTotals = resolveTotalsForDataset(summaryDatasetKey);
        if (legacyTotals) {
          return {
            locationCount: Number(legacyTotals.locationCount ?? legacyTotals.featureCount ?? 0),
            ticketCount: Number(legacyTotals.ticketCount ?? legacyTotals.featureCount ?? 0),
            totalRevenue: Number(legacyTotals.totalRevenue ?? 0),
          };
        }
      }
      if (wardTotals) {
        return {
          locationCount: Number(wardTotals.locationCount ?? 0),
          ticketCount: Number(wardTotals.ticketCount ?? 0),
          totalRevenue: Number(wardTotals.totalRevenue ?? 0),
        };
      }
      if (activeWardDataset) {
        const wardDatasetTotals = resolveTotalsForDataset(activeWardDataset);
        if (wardDatasetTotals) {
          return wardDatasetTotals;
        }
      }
      return currentTotals;
    }
    if (effectiveYearTotals) {
      return effectiveYearTotals;
    }
    return currentTotals;
  }, [activeViewMode, summaryUseLegacy, summaryDatasetKey, resolveTotalsForDataset, wardTotals, activeWardDataset, currentTotals, effectiveYearTotals]);

  const summaryDiscrepancy = discrepancyByDataset[summaryDatasetKey] || null;
  const datasetTotals = resolveTotalsForDataset(dataset);
  const primaryDisplayTotals = effectiveYearTotals || datasetTotals;
  const combinedBreakdownOverride = useMemo(() => {
    const aseTotals = resolveTotalsForDataset('ase_locations');
    const redTotals = resolveTotalsForDataset('red_light_locations');
    if (!aseTotals && !redTotals) {
      return null;
    }
    const normalize = (source) => {
      if (!source) {
        return {
          ticketCount: 0,
          totalRevenue: 0,
          locationCount: 0,
        };
      }
      return {
        ticketCount: Number(source.ticketCount ?? source.featureCount ?? 0),
        totalRevenue: Number(source.totalRevenue ?? 0),
        locationCount: Number(source.locationCount ?? source.featureCount ?? 0),
      };
    };
    return {
      ase: normalize(aseTotals),
      redLight: normalize(redTotals),
    };
  }, [resolveTotalsForDataset]);

  useEffect(() => {
    if (typeof window !== 'undefined') {
      setIsClient(true);
    }
  }, []);

  useEffect(() => {
    if (!isClient) {
      return undefined;
    }
    Promise.all([prefetchCameraDatasets(), prefetchGlowDatasets()]).catch(() => {
      /* prefetch failures are non-fatal */
    });
    return undefined;
  }, [isClient]);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return undefined;
    }
    const updateVariant = () => {
      setPopupVariant((current) => {
        const next = getPopupVariantForWidth(window.innerWidth);
        return current === next ? current : next;
      });
    };
    updateVariant();
    window.addEventListener('resize', updateVariant);
    return () => window.removeEventListener('resize', updateVariant);
  }, []);

  const handleMapLoad = useCallback((mapInstance) => {
    setMap(mapInstance);
  }, []);

  const focusOnPoint = useCallback((longitude, latitude, options = {}) => {
    if (!map) {
      return;
    }
    if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
      return;
    }
    map.flyTo({
      center: [longitude, latitude],
      zoom: options.zoom ?? 14,
      duration: options.duration ?? 850,
      essential: true,
    });
  }, [map]);

  const focusOnBounds = useCallback((bbox, options = {}) => {
    if (!map || !Array.isArray(bbox) || bbox.length !== 4) {
      return;
    }
    const [minLng, minLat, maxLng, maxLat] = bbox;
    if (![minLng, minLat, maxLng, maxLat].every((value) => Number.isFinite(value))) {
      return;
    }
    map.fitBounds(
      [
        [minLng, minLat],
        [maxLng, maxLat],
      ],
      {
        padding: 64,
        maxZoom: 16,
        duration: 700,
        ...options,
      },
    );
  }, [map]);

  const computePopupPosition = useCallback((event) => {
    if (popupVariant !== 'floating') {
      return null;
    }
    if (typeof window === 'undefined') {
      return { x: 0, y: 0 };
    }

    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;

    const sidebarWidth = 320;
    const overlayWidth = viewportWidth <= 1024 ? 280 : 320;
    const rightMargin = viewportWidth <= 1024 ? 16 : 20;
    const popupMaxWidth = 420;
    const popupHalfWidth = popupMaxWidth / 2;
    const baseX = event?.point?.x ?? viewportWidth / 2;
    const baseY = event?.point?.y ?? viewportHeight / 2;

    const minCenterX = sidebarWidth + popupHalfWidth + 24;
    const preferredCenterX = viewportWidth - overlayWidth - rightMargin - popupHalfWidth;
    const maxCenterX = viewportWidth - popupHalfWidth - rightMargin;

    let x;
    if (preferredCenterX >= minCenterX) {
      x = preferredCenterX;
    } else {
      if (maxCenterX <= minCenterX) {
        const fallback = Math.min(Math.max(baseX, popupHalfWidth + rightMargin), viewportWidth - popupHalfWidth - rightMargin);
        x = Number.isFinite(fallback) ? fallback : viewportWidth / 2;
      } else {
        const clamped = Math.min(Math.max(baseX, minCenterX), maxCenterX);
        x = Number.isFinite(clamped) ? clamped : minCenterX;
      }
    }

    const minY = 120;
    const maxY = viewportHeight - 200;
    const y = Math.min(Math.max(baseY, minY), maxY);

    return { x, y };
  }, [popupVariant]);

  useEffect(() => {
    if (!popupData) {
      return;
    }
    if (popupVariant === 'floating') {
      setPopupPosition((current) => current ?? computePopupPosition());
    } else {
      setPopupPosition(null);
    }
  }, [popupVariant, popupData, computePopupPosition]);

  useEffect(() => {
    if (!isMobile) {
      setDrawerOpen(false);
      setLegendSheetOpen(false);
      setInsightsSheetOpen(false);
    }
  }, [isMobile]);

  const handleNeighbourhoodClick = useCallback((properties, event) => {
    setActiveCentrelineIds([]);
    setPopupData(properties);
    setPopupPosition(computePopupPosition(event));
  }, [computePopupPosition]);

  const handlePointClick = useCallback((properties, event) => {
    if (!properties) {
      return;
    }

    if (dataset !== 'parking_tickets') {
      const rawId = properties.locationId
        ?? properties.location_code
        ?? properties.locationCode
        ?? properties.intersection_id
        ?? properties.intersectionId
        ?? properties.location;
      const locationId = rawId !== undefined && rawId !== null ? String(rawId) : null;
      const lookup = locationId && datasetEntry?.locationsById
        ? datasetEntry.locationsById[locationId]
        : null;

      const yearlyCounts = properties.yearlyCounts
        ?? properties.yearly_counts
        ?? lookup?.yearlyCounts
        ?? {};
      let ticketCountValue = Number(
        properties.count
          ?? properties.ticketCount
          ?? properties.ticket_count
          ?? lookup?.ticketCount
          ?? 0,
      );
      let totalRevenueValue = Number(
        properties.total_revenue
          ?? properties.totalRevenue
          ?? properties.total_fine_amount
          ?? lookup?.totalRevenue
          ?? 0,
      );
      if (activeYear !== null && yearlyCounts) {
        const entry = yearlyCounts[activeYear] ?? yearlyCounts[String(activeYear)];
        if (entry && typeof entry === 'object') {
          const entryCount = Number(entry.ticketCount ?? entry.count ?? entry);
          if (Number.isFinite(entryCount)) {
            ticketCountValue = entryCount;
          }
          const entryRevenue = Number(entry.totalRevenue ?? entry.total_revenue);
          if (Number.isFinite(entryRevenue)) {
            totalRevenueValue = entryRevenue;
          }
        }
      }
      const years = Array.isArray(properties.years) && properties.years.length > 0
        ? properties.years
        : lookup?.years || [];
      const displayName = properties.location
        || properties.name
        || lookup?.name
        || properties.street
        || 'Camera location';

      const longitude = Number(
        properties.longitude
          ?? lookup?.longitude
          ?? event?.lngLat?.lng,
      );
      const latitude = Number(
        properties.latitude
          ?? lookup?.latitude
          ?? event?.lngLat?.lat,
      );

      if (Number.isFinite(longitude) && Number.isFinite(latitude)) {
        focusOnPoint(longitude, latitude, { zoom: 15 });
      }

      setActiveCentrelineIds([]);
      setPopupData({
        location: displayName,
        locationId,
        ticketCount: Number.isFinite(ticketCountValue) ? ticketCountValue : 0,
        count: Number.isFinite(ticketCountValue) ? ticketCountValue : 0,
        totalRevenue: Number.isFinite(totalRevenueValue) ? totalRevenueValue : 0,
        total_revenue: Number.isFinite(totalRevenueValue) ? totalRevenueValue : 0,
        ward: properties.ward ?? lookup?.ward,
        policeDivision: properties.policeDivision ?? properties.police_division_1 ?? lookup?.policeDivision,
        status: properties.status ?? lookup?.status,
        activationDate: properties.activationDate ?? lookup?.activationDate,
        years,
        yearlyCounts,
        yearFilter: activeYear,
      });
      setPopupPosition(computePopupPosition(event));
      return;
    }

    setActiveCentrelineIds([]);
    setPopupData(activeYear !== null ? { ...properties, yearFilter: activeYear } : properties);
    setPopupPosition(computePopupPosition(event));
  }, [dataset, datasetEntry, focusOnPoint, computePopupPosition, activeYear]);

  const handleNeighbourhoodFocus = useCallback((name) => {
    if (!map || !name) return;
    const features = map.querySourceFeatures('toronto-neighbourhoods');
    const match = features.find((feature) => {
      const props = feature.properties || {};
      return props.name === name || props.AREA_NAME === name;
    });
    if (match) {
      const [minLng, minLat, maxLng, maxLat] = match.geometry?.bbox || [];
      if (minLng !== undefined) {
        map.fitBounds([[minLng, minLat], [maxLng, maxLat]], { padding: 40, duration: 800 });
      }
    }
  }, [map]);

  const closePopup = useCallback(() => {
    setPopupData(null);
    setPopupPosition(null);
    setActiveCentrelineIds([]);
  }, []);

  const toggleOverlay = useCallback(() => {
    if (isMobile) {
      setInsightsSheetOpen((current) => !current);
      return;
    }
    setIsOverlayCollapsed((current) => !current);
  }, [isMobile]);

  const handleDrawerToggle = useCallback(() => {
    setDrawerOpen((current) => !current);
  }, []);

  const closeDrawer = useCallback(() => {
    setDrawerOpen(false);
  }, []);

  const openLegendSheet = useCallback(() => {
    setLegendSheetOpen(true);
  }, []);

  const closeLegendSheet = useCallback(() => {
    setLegendSheetOpen(false);
  }, []);

  const closeInsightsSheet = useCallback(() => {
    setInsightsSheetOpen(false);
  }, []);

  useEffect(() => {
    setViewportSummary({ zoomRestricted: true, topStreets: [] });
    setPopupData(null);
    setPopupPosition(null);
    setActiveCentrelineIds([]);
    setActiveTab('streets');
    setWardHoverInfo(null);
  }, [dataset, activeYear]);

  useEffect(() => {
    if (isMobile) {
      setDrawerOpen(false);
    }
  }, [dataset, isMobile]);

  useEffect(() => {
    if (activeViewMode === 'ward' && activeYear !== null) {
      setYearSelection(dataset, null);
    }
  }, [activeViewMode, activeYear, dataset, setYearSelection]);
  useEffect(() => {
    if (activeViewMode !== 'ward') {
      setWardHoverInfo(null);
    }
  }, [activeViewMode]);

  const handleStreetSelect = useCallback((streetEntry) => {
    if (!streetEntry) {
      return;
    }
    if (dataset !== 'parking_tickets') {
      const locationId = streetEntry.id ?? streetEntry.locationCode ?? streetEntry.intersectionId;
      const lookup = locationId !== undefined && locationId !== null
        ? datasetEntry?.locationsById?.[String(locationId)]
        : null;
      const longitude = Number(streetEntry.longitude ?? lookup?.longitude);
      const latitude = Number(streetEntry.latitude ?? lookup?.latitude);
      if (Number.isFinite(longitude) && Number.isFinite(latitude)) {
        focusOnPoint(longitude, latitude, { zoom: 14.5 });
      }

      setActiveCentrelineIds([]);
      setPopupData({
        location: streetEntry.name || lookup?.name || streetEntry.address,
        count: Number(streetEntry.ticketCount ?? streetEntry.count ?? lookup?.ticketCount ?? 0),
        ticketCount: Number(streetEntry.ticketCount ?? streetEntry.count ?? lookup?.ticketCount ?? 0),
        total_revenue: Number(streetEntry.totalRevenue ?? lookup?.totalRevenue ?? 0),
        totalRevenue: Number(streetEntry.totalRevenue ?? lookup?.totalRevenue ?? 0),
        ward: streetEntry.ward ?? lookup?.ward,
        policeDivision: streetEntry.policeDivision ?? lookup?.policeDivision,
        status: streetEntry.status ?? lookup?.status,
        years: lookup?.years ?? [],
        locationId: locationId ?? null,
        longitude,
        latitude,
        yearFilter: activeYear,
      });
      setPopupPosition(computePopupPosition());
      return;
    }

    const streetName = streetEntry.name || streetEntry.street || streetEntry.sampleLocation;
    if (!streetName) {
      return;
    }

    const summary = getStreetSummary?.(streetName);
    let payload;
    if (summary) {
      payload = { ...summary, source: 'street-leaderboard' };
      if (activeYear !== null) {
        payload = {
          ...payload,
          ticketCount: Number(streetEntry.ticketCount ?? 0),
          totalRevenue: Number(streetEntry.totalRevenue ?? 0),
          yearFilter: activeYear,
        };
      }
      if (summary.centrelineIds?.length) {
        setActiveCentrelineIds(summary.centrelineIds);
      } else {
        setActiveCentrelineIds([]);
      }
      if (summary.bbox) {
        focusOnBounds(summary.bbox, { duration: 760 });
      }
    } else {
      payload = {
        street: streetName,
        ticketCount: streetEntry.ticketCount ?? 0,
        totalRevenue: streetEntry.totalRevenue ?? 0,
        topLocations: streetEntry.topLocations || [],
        yearFilter: activeYear,
      };
      setActiveCentrelineIds([]);
    }

    setPopupData(payload);
    setPopupPosition(computePopupPosition());
  }, [dataset, datasetEntry, getStreetSummary, computePopupPosition, focusOnBounds, focusOnPoint, activeYear]);

  const handleStreetSegmentClick = useCallback((centrelineId, feature, event) => {
    const detail = getCentrelineDetail?.(centrelineId) || null;
    const streetName = detail?.street || feature?.properties?.street || feature?.properties?.street_name || null;
    const summary = streetName ? getStreetSummary?.(streetName) || null : null;

    let payload = null;
    if (summary) {
      payload = { ...summary };
      if (detail) {
        payload.centrelineDetail = detail;
      }
    } else if (detail) {
      payload = { ...detail };
    }

    if (!payload) {
      if (!streetName) {
        return;
      }
      payload = {
        street: streetName,
        centrelineId,
      };
    }

    if (detail && !payload.centrelineDetail) {
      payload = { ...payload, centrelineDetail: detail };
    }

    setPopupData(payload);
    setPopupPosition(computePopupPosition(event));

    if (summary?.centrelineIds?.length) {
      setActiveCentrelineIds(summary.centrelineIds);
    } else if (detail?.centrelineId) {
      setActiveCentrelineIds([detail.centrelineId]);
    } else if (centrelineId !== null && centrelineId !== undefined) {
      setActiveCentrelineIds([centrelineId]);
    } else {
      setActiveCentrelineIds([]);
    }

    const bbox = summary?.bbox || detail?.bbox;
    if (bbox) {
      focusOnBounds(bbox, { duration: 640, padding: 72, maxZoom: 15.5 });
    }
  }, [getCentrelineDetail, getStreetSummary, computePopupPosition, focusOnBounds]);

  const normalizeWardMetrics = useCallback((properties, targetDataset) => {
    if (!properties || typeof properties !== 'object') {
      return null;
    }
    const rawWardCode = properties.wardCode
      ?? properties.ward_code
      ?? properties.code
      ?? properties.ward
      ?? null;
    const wardName = properties.wardName
      ?? properties.ward_name
      ?? properties.name
      ?? (rawWardCode ? `Ward ${rawWardCode}` : null);
    const rawTickets = Number(
      properties.ticketCount
        ?? properties.ticket_count
        ?? properties.totalTickets
        ?? properties.count,
    );
    const rawRevenue = Number(properties.totalRevenue ?? properties.total_revenue);
    const rawLocations = Number(
      properties.locationCount
        ?? properties.location_count
        ?? properties.cameraCount
        ?? properties.siteCount
        ?? properties.locations,
    );
    const aseTickets = Number(
      properties.aseTicketCount
        ?? properties.ase_ticket_count
        ?? properties.aseTickets,
    );
    const redTickets = Number(
      properties.rlcTicketCount
        ?? properties.redLightTicketCount
        ?? properties.red_light_ticket_count
        ?? properties.redTickets,
    );
    const wardKey = rawWardCode !== null && rawWardCode !== undefined ? String(rawWardCode) : null;
    const datasetKey = targetDataset || null;
    let aseWardTotals = null;
    let redWardTotals = null;
    if (datasetKey === 'cameras_combined' && wardKey) {
      aseWardTotals = aseWardLookup?.get?.(wardKey) || null;
      redWardTotals = redWardLookup?.get?.(wardKey) || null;
    }
    const resolvedAseTickets = Number.isFinite(aseTickets)
      ? aseTickets
      : Number(aseWardTotals?.ticketCount);
    const resolvedRedTickets = Number.isFinite(redTickets)
      ? redTickets
      : Number(redWardTotals?.ticketCount);
    const resolvedAseRevenue = Number(aseWardTotals?.totalRevenue);
    const resolvedRedRevenue = Number(redWardTotals?.totalRevenue);

    return {
      wardCode: Number.isFinite(Number(rawWardCode)) ? Number(rawWardCode) : rawWardCode,
      wardName,
      ticketCount: Number.isFinite(rawTickets) ? rawTickets : null,
      totalRevenue: Number.isFinite(rawRevenue) ? rawRevenue : null,
      locationCount: Number.isFinite(rawLocations) ? rawLocations : null,
      aseTicketCount: Number.isFinite(resolvedAseTickets) ? resolvedAseTickets : null,
      redLightTicketCount: Number.isFinite(resolvedRedTickets) ? resolvedRedTickets : null,
      aseTotalRevenue: Number.isFinite(resolvedAseRevenue) ? resolvedAseRevenue : null,
      redLightTotalRevenue: Number.isFinite(resolvedRedRevenue) ? resolvedRedRevenue : null,
    };
  }, [aseWardLookup, redWardLookup]);

  const handleWardHover = useCallback((properties) => {
    if (activeViewMode !== 'ward') {
      setWardHoverInfo(null);
      return;
    }
    if (!properties) {
      setWardHoverInfo(null);
      return;
    }
    const datasetKey = activeWardDataset || dataset;
    const normalized = normalizeWardMetrics(properties, datasetKey);
    if (!normalized) {
      setWardHoverInfo(null);
      return;
    }
    setWardHoverInfo({ ...normalized, dataset: datasetKey });
  }, [activeViewMode, normalizeWardMetrics, activeWardDataset, dataset]);

  const handleWardClick = useCallback((properties, event) => {
    if (!properties) {
      return;
    }
    const datasetKey = activeWardDataset || dataset;
    const normalized = normalizeWardMetrics(properties, datasetKey);
    if (!normalized) {
      return;
    }
    setWardHoverInfo(null);
    setPopupData({
      name: normalized.wardName || (normalized.wardCode ? `Ward ${normalized.wardCode}` : 'Ward'),
      ticketCount: normalized.ticketCount ?? undefined,
      totalRevenue: normalized.totalRevenue ?? undefined,
      locationCount: normalized.locationCount ?? undefined,
      ward: normalized.wardName || (normalized.wardCode ? `Ward ${normalized.wardCode}` : null),
      dataset: datasetKey,
      aseTicketCount: normalized.aseTicketCount ?? undefined,
      redLightTicketCount: normalized.redLightTicketCount ?? undefined,
      aseTotalRevenue: normalized.aseTotalRevenue ?? undefined,
      redLightTotalRevenue: normalized.redLightTotalRevenue ?? undefined,
    });
    setPopupPosition(computePopupPosition(event));
  }, [normalizeWardMetrics, activeWardDataset, dataset, computePopupPosition]);

  const handleWardSelect = useCallback((item) => {
    if (!item) {
      return;
    }
    const datasetKey = activeWardDataset || dataset;
    const normalized = normalizeWardMetrics(item, datasetKey);
    if (!normalized) {
      return;
    }
    setWardHoverInfo(null);
    setPopupData({
      name: normalized.wardName || (normalized.wardCode ? `Ward ${normalized.wardCode}` : 'Ward'),
      ticketCount: normalized.ticketCount ?? undefined,
      totalRevenue: normalized.totalRevenue ?? undefined,
      locationCount: normalized.locationCount ?? undefined,
      ward: normalized.wardName || (normalized.wardCode ? `Ward ${normalized.wardCode}` : null),
      dataset: datasetKey,
      aseTicketCount: normalized.aseTicketCount ?? undefined,
      redLightTicketCount: normalized.redLightTicketCount ?? undefined,
      aseTotalRevenue: normalized.aseTotalRevenue ?? undefined,
      redLightTotalRevenue: normalized.redLightTotalRevenue ?? undefined,
    });
    setPopupPosition(null);
  }, [normalizeWardMetrics, activeWardDataset, dataset]);

  const sidebarContent = (
    <div className="sidebar-content">
      <MobileAccordion
        title="Totals"
        isMobile={isMobile}
        defaultOpen
      >
        <StatsSummary
          viewportSummary={viewportSummary}
          showViewport={dataset === 'parking_tickets'}
          dataset={activeViewMode === 'ward' && activeWardDataset ? activeWardDataset : dataset}
          totalsOverride={statsTotalsToUse}
          yearFilter={activeViewMode === 'ward' ? null : activeYear}
          discrepancyInfo={summaryDiscrepancy}
          onToggleLegacy={summaryLegacyToggleHandler}
          useLegacyTotals={summaryUseLegacy}
          combinedBreakdownOverride={combinedBreakdownOverride}
        />
      </MobileAccordion>

      <MobileAccordion
        title="Year filter"
        isMobile={isMobile}
        defaultOpen={false}
      >
        <YearFilter
          years={datasetYears}
          value={activeYear}
          onChange={handleYearChange}
          disabled={datasetYears.length === 0 || activeViewMode === 'ward'}
        />
      </MobileAccordion>

      {activeViewMode === 'ward' ? (
        <MobileAccordion
          title="Top wards"
          isMobile={isMobile}
          defaultOpen
        >
          <Suspense fallback={<div className="panel-placeholder">Loading wards…</div>}>
            <WardLeaderboard
              items={wardLeaderboardItems}
              loading={isWardLoading}
              dataset={activeWardDataset || dataset}
              onWardSelect={handleWardSelect}
            />
          </Suspense>
        </MobileAccordion>
      ) : (
        <>
          {isMobile ? (
            <>
              <MobileAccordion
                title={streetsTabLabel}
                isMobile
                defaultOpen
              >
                <StreetLeaderboard
                  visible
                  dataset={dataset}
                  initialItems={streetInitialItems}
                  onStreetSelect={handleStreetSelect}
                  overrideItems={streetOverrideItems}
                  overrideLoading={isYearLoading}
                  totalsOverride={primaryDisplayTotals}
                />
              </MobileAccordion>
              <MobileAccordion
                title={neighbourhoodsTabLabel}
                isMobile
                defaultOpen={false}
              >
                <NeighbourhoodLeaderboard
                  visible
                  dataset={dataset}
                  onNeighbourhoodClick={dataset === 'parking_tickets' ? handleNeighbourhoodFocus : undefined}
                  initialItems={neighbourhoodInitialItems}
                  overrideItems={neighbourhoodOverrideItems}
                  overrideLoading={isYearLoading}
                  totalsOverride={primaryDisplayTotals}
                />
              </MobileAccordion>
            </>
          ) : (
            <>
              <div className="tab-switcher">
                <button
                  className={`tab-btn ${activeTab === 'streets' ? 'active' : ''}`}
                  onClick={() => setActiveTab('streets')}
                >
                  {streetsTabLabel}
                </button>
                <button
                  className={`tab-btn ${activeTab === 'neighbourhoods' ? 'active' : ''}`}
                  onClick={() => setActiveTab('neighbourhoods')}
                >
                  {neighbourhoodsTabLabel}
                </button>
              </div>
              <StreetLeaderboard
                visible={activeTab === 'streets'}
                dataset={dataset}
                initialItems={streetInitialItems}
                onStreetSelect={handleStreetSelect}
                overrideItems={streetOverrideItems}
                overrideLoading={isYearLoading}
                totalsOverride={primaryDisplayTotals}
              />
              <NeighbourhoodLeaderboard
                visible={activeTab === 'neighbourhoods'}
                dataset={dataset}
                onNeighbourhoodClick={dataset === 'parking_tickets' ? handleNeighbourhoodFocus : undefined}
                initialItems={neighbourhoodInitialItems}
                overrideItems={neighbourhoodOverrideItems}
                overrideLoading={isYearLoading}
                totalsOverride={primaryDisplayTotals}
              />
            </>
          )}
        </>
      )}

      <MobileAccordion
        title="About"
        isMobile={isMobile}
        defaultOpen={false}
      >
        <HowItWorks />
      </MobileAccordion>
    </div>
  );

  const mainClassName = [
    'App',
    isMobile ? 'App--mobile' : null,
    isTouchDevice ? 'App--touch' : null,
  ].filter(Boolean).join(' ');

  return (
    <div className={mainClassName}>
      {isMobile ? (
        <>
          <MobileHeader
            dataset={dataset}
            onDatasetChange={setDataset}
            onDrawerToggle={handleDrawerToggle}
            isDrawerOpen={isDrawerOpen}
            onLegendToggle={openLegendSheet}
            onInsightsToggle={() => setInsightsSheetOpen(true)}
          >
            {(dataset === 'red_light_locations' || dataset === 'ase_locations') ? (
              <Suspense fallback={<div className="mobile-header__placeholder" />}
              >
                <WardModeToggle
                  dataset={dataset}
                  viewMode={activeViewMode}
                  wardDataset={activeWardDataset || dataset}
                  onViewModeChange={handleViewModeChange}
                  onWardDatasetChange={handleWardDatasetChange}
                />
              </Suspense>
            ) : null}
          </MobileHeader>

          {isClient ? (
            <Suspense fallback={<div className="map-container map-container__placeholder">Loading map…</div>}>
              <MapExperience
                onMapLoad={handleMapLoad}
                onPointClick={handlePointClick}
                onNeighbourhoodClick={handleNeighbourhoodClick}
                onViewportSummaryChange={setViewportSummary}
                onStreetSegmentClick={handleStreetSegmentClick}
                highlightCentrelineIds={activeCentrelineIds}
                dataset={dataset}
                filter={activeYear !== null && activeViewMode !== 'ward' ? { year: activeYear } : null}
                viewMode={activeViewMode}
                wardDataset={activeWardDataset || dataset}
                onWardHover={handleWardHover}
                onWardClick={handleWardClick}
                isTouchDevice={isTouchDevice}
              />
            </Suspense>
          ) : (
            <div className="map-container map-container__placeholder">Preparing map…</div>
          )}

          <MobileDrawer open={isDrawerOpen} onClose={closeDrawer}>
            {sidebarContent}
          </MobileDrawer>

          <div
            className={`mobile-modal ${isLegendSheetOpen ? 'mobile-modal--open' : ''}`}
            onClick={closeLegendSheet}
            role="dialog"
            aria-modal="true"
            aria-hidden={!isLegendSheetOpen}
          >
            <div
              className="mobile-modal__sheet"
              onClick={(event) => event.stopPropagation()}
            >
              <div className="mobile-modal__header">
                <h2>Legend</h2>
                <button type="button" className="mobile-modal__close" onClick={closeLegendSheet}>
                  Close
                </button>
              </div>
              <Legend visible dataset={legendDataset} />
            </div>
          </div>

          <div
            className={`mobile-modal ${isInsightsSheetOpen ? 'mobile-modal--open' : ''}`}
            onClick={closeInsightsSheet}
            role="dialog"
            aria-modal="true"
            aria-hidden={!isInsightsSheetOpen}
          >
            <div
              className="mobile-modal__sheet"
              onClick={(event) => event.stopPropagation()}
            >
              <div className="mobile-modal__header">
                <h2>Viewport insights</h2>
                <button type="button" className="mobile-modal__close" onClick={closeInsightsSheet}>
                  Close
                </button>
              </div>
              <ViewportInsights
                summary={viewportSummary}
                fallbackTopStreets={fallbackTopStreets}
                variant="compact"
              />
            </div>
          </div>

          {activeViewMode === 'ward' && wardHoverInfo ? (
            <Suspense fallback={null}>
              <WardHoverPopup data={wardHoverInfo} dataset={wardHoverInfo.dataset} />
            </Suspense>
          ) : null}

          {popupData && (
            <InfoPopup
              data={popupData}
              position={popupPosition}
              variant={popupVariant}
              yearFilter={activeYear}
              onClose={closePopup}
            />
          )}
        </>
      ) : (
        <>
          <div className="left-sidebar">
            {sidebarContent}
          </div>

          {dataset === 'parking_tickets' ? (
            <div className={`insights-overlay ${isOverlayCollapsed ? 'insights-overlay--collapsed' : ''}`}>
              <div className="overlay-header">
                <button
                  type="button"
                  className="overlay-toggle"
                  onClick={toggleOverlay}
                  aria-expanded={!isOverlayCollapsed}
                >
                  {isOverlayCollapsed ? 'Show insights' : 'Hide insights'}
                </button>
              </div>
              {!isOverlayCollapsed ? (
                <div className="overlay-stack">
                  <>
                    <div className="overlay-panel summary-panel">
                      <StatsSummary
                        viewportSummary={viewportSummary}
                        variant="compact"
                        showTotals={false}
                        showViewport
                        dataset={dataset}
                        totalsOverride={statsTotalsOverride || primaryDisplayTotals}
                        viewportTitle="Current view"
                        combinedBreakdownOverride={combinedBreakdownOverride}
                      />
                    </div>
                    <div className="overlay-panel insights-panel">
                      <ViewportInsights
                        summary={viewportSummary}
                        fallbackTopStreets={fallbackTopStreets}
                        variant="compact"
                      />
                    </div>
                  </>
                </div>
              ) : null}
            </div>
          ) : null}

          <div className="legend-floating">
            <Legend visible dataset={legendDataset} />
          </div>

          <div className="dataset-toggle-floating">
            <DatasetToggle value={dataset} onChange={setDataset} />
            {(dataset === 'red_light_locations' || dataset === 'ase_locations') ? (
              <div className="dataset-toggle-floating__secondary">
                <Suspense fallback={null}>
                  <WardModeToggle
                    dataset={dataset}
                    viewMode={activeViewMode}
                    wardDataset={activeWardDataset || dataset}
                    onViewModeChange={handleViewModeChange}
                    onWardDatasetChange={handleWardDatasetChange}
                  />
                </Suspense>
              </div>
            ) : null}
          </div>

          {isClient ? (
            <Suspense fallback={<div className="map-container">Loading map…</div>}>
              <MapExperience
                onMapLoad={handleMapLoad}
                onPointClick={handlePointClick}
                onNeighbourhoodClick={handleNeighbourhoodClick}
                onViewportSummaryChange={setViewportSummary}
                onStreetSegmentClick={handleStreetSegmentClick}
                highlightCentrelineIds={activeCentrelineIds}
                dataset={dataset}
                filter={activeYear !== null && activeViewMode !== 'ward' ? { year: activeYear } : null}
                viewMode={activeViewMode}
                wardDataset={activeWardDataset || dataset}
                onWardHover={handleWardHover}
                onWardClick={handleWardClick}
                isTouchDevice={isTouchDevice}
              />
            </Suspense>
          ) : (
            <div className="map-container">Preparing map…</div>
          )}

          {activeViewMode === 'ward' && wardHoverInfo ? (
            <Suspense fallback={null}>
              <WardHoverPopup data={wardHoverInfo} dataset={wardHoverInfo.dataset} />
            </Suspense>
          ) : null}

          {popupData && (
            <InfoPopup
              data={popupData}
              position={popupPosition}
              variant={popupVariant}
              yearFilter={activeYear}
              onClose={closePopup}
            />
          )}
        </>
      )}
    </div>
  );
}

function AppProviders({ initialData, children }) {
  const providerValue = initialData
    ? {
        ...(initialData.totals ? { totals: initialData.totals } : {}),
        topStreets: Array.isArray(initialData.topStreets) ? initialData.topStreets : [],
        topNeighbourhoods: Array.isArray(initialData.topNeighbourhoods) ? initialData.topNeighbourhoods : [],
        datasets: initialData.datasets && typeof initialData.datasets === 'object' ? initialData.datasets : {},
        generatedAt: initialData.generatedAt ?? null,
        yearlyMeta:
          initialData.yearlyMeta && typeof initialData.yearlyMeta === 'object'
            ? initialData.yearlyMeta
            : {},
      }
    : null;

  return (
    <AppDataProvider value={providerValue}>
      <WardDataProvider>
        {children}
      </WardDataProvider>
    </AppDataProvider>
  );
}

export default function App({ initialData = null }) {
  const fallbackTopStreets = useMemo(() => (
    initialData?.topStreets && Array.isArray(initialData.topStreets)
      ? initialData.topStreets
      : []
  ), [initialData]);

  const fallbackTopNeighbourhoods = useMemo(() => (
    initialData?.topNeighbourhoods && Array.isArray(initialData.topNeighbourhoods)
      ? initialData.topNeighbourhoods
      : []
  ), [initialData]);

  return (
    <PmtilesProvider>
      <AppProviders initialData={initialData}>
        <AppContent
          fallbackTopStreets={fallbackTopStreets}
          fallbackTopNeighbourhoods={fallbackTopNeighbourhoods}
        />
      </AppProviders>
    </PmtilesProvider>
  );
}
