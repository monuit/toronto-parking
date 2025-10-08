/**
 * Main App component
 * Single responsibility: coordinate all map components and state
 */
import { useState, useCallback, useMemo, useEffect, lazy, Suspense } from 'react';
import { AppDataProvider, useAppData } from './context/AppDataContext.jsx';
import { useCentrelineLookup } from './context/CentrelineContext.jsx';
import { StatsSummary } from './components/StatsSummary.jsx';
import { StreetLeaderboard } from './components/StreetLeaderboard.jsx';
import { NeighbourhoodLeaderboard } from './components/NeighbourhoodLeaderboard.jsx';
import { Legend } from './components/Legend.jsx';
import { InfoPopup } from './components/InfoPopup.jsx';
import { HowItWorks } from './components/HowItWorks.jsx';
import { ViewportInsights } from './components/ViewportInsights.jsx';
import { DatasetToggle } from './components/DatasetToggle.jsx';
import { MAP_CONFIG } from './lib/mapSources.js';
import './App.css';

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
  isServer = false,
  fallbackTopStreets = [],
  fallbackTopNeighbourhoods = [],
}) {
  const { totals: contextTotals } = useAppData();
  const [map, setMap] = useState(null);
  const [activeCentrelineIds, setActiveCentrelineIds] = useState([]);
  const [activeTab, setActiveTab] = useState('streets');
  const [popupData, setPopupData] = useState(null);
  const [popupPosition, setPopupPosition] = useState(null);
  const [viewportSummary, setViewportSummary] = useState({ zoomRestricted: true, topStreets: [] });
  const [dataset, setDataset] = useState('parking_tickets');
  const [totalsByDataset, setTotalsByDataset] = useState({});
  const [isClient, setIsClient] = useState(() => !isServer && typeof window !== 'undefined');
  const [isOverlayCollapsed, setIsOverlayCollapsed] = useState(false);
  const [popupVariant, setPopupVariant] = useState(() => {
    if (typeof window === 'undefined') {
      return 'floating';
    }
    return getPopupVariantForWidth(window.innerWidth);
  });
  const { getStreetSummary, getCentrelineDetail } = useCentrelineLookup();
  useEffect(() => {
    if (!contextTotals || typeof contextTotals !== 'object') {
      return;
    }

    const nextPayload = { ...contextTotals, __source: 'context' };

    setTotalsByDataset((previous) => {
      const existing = previous.parking_tickets;
      if (existing && existing.__source === 'api') {
        return previous;
      }
      if (existing && existing.__source === 'context') {
        const same = Object.keys(contextTotals).every((key) => existing[key] === contextTotals[key]);
        if (same) {
          return previous;
        }
      }
      return { ...previous, parking_tickets: nextPayload };
    });

  }, [contextTotals]);

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
          setTotalsByDataset((previous) => ({
            ...previous,
            [dataset]: { ...payload, __source: 'api' },
          }));
        }
      })
      .catch((error) => {
        console.error('Failed to load dataset totals', error);
      });

    return () => {
      cancelled = true;
    };
  }, [dataset, totalsByDataset]);

  const currentTotalsEntry = totalsByDataset[dataset] || null;
  const currentTotals = useMemo(() => {
    if (!currentTotalsEntry || typeof currentTotalsEntry !== 'object') {
      return null;
    }
    const { __source, ...rest } = currentTotalsEntry;
    return rest;
  }, [currentTotalsEntry]);

  useEffect(() => {
    if (typeof window !== 'undefined') {
      setIsClient(true);
    }
  }, []);

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

  const handleNeighbourhoodClick = useCallback((properties, event) => {
    setActiveCentrelineIds([]);
    setPopupData(properties);
    setPopupPosition(computePopupPosition(event));
  }, [computePopupPosition]);

  const handlePointClick = useCallback((properties, event) => {
    setActiveCentrelineIds([]);
    setPopupData(properties);
    setPopupPosition(computePopupPosition(event));
  }, [computePopupPosition]);

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
    setIsOverlayCollapsed((current) => !current);
  }, []);

  useEffect(() => {
    setViewportSummary({ zoomRestricted: true, topStreets: [] });
    setPopupData(null);
    setPopupPosition(null);
  }, [dataset]);

  const handleStreetSelect = useCallback((streetEntry) => {
    if (!streetEntry) {
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
      };
      setActiveCentrelineIds([]);
    }

    setPopupData(payload);
    setPopupPosition(computePopupPosition());
  }, [getStreetSummary, computePopupPosition, focusOnBounds]);

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

  return (
    <div className="App">
      <div className="left-sidebar">
        <div className="sidebar-content">
          <StatsSummary
            viewportSummary={viewportSummary}
            showViewport={false}
            dataset={dataset}
            totalsOverride={currentTotals}
          />

          <div className="tab-switcher">
            <button
              className={`tab-btn ${activeTab === 'streets' ? 'active' : ''}`}
              onClick={() => setActiveTab('streets')}
            >
              Top Streets
            </button>
            <button
              className={`tab-btn ${activeTab === 'neighbourhoods' ? 'active' : ''}`}
              onClick={() => setActiveTab('neighbourhoods')}
            >
              Top Neighbourhoods
            </button>
          </div>

          {dataset === 'parking_tickets' ? (
            <StreetLeaderboard
              visible={activeTab === 'streets'}
              initialStreets={fallbackTopStreets}
              onStreetSelect={handleStreetSelect}
            />
          ) : (
            activeTab === 'streets' ? (
              <div className="leaderboard street-leaderboard">
                <p className="subtitle" style={{ marginBottom: 0 }}>
                  Citywide street rankings are currently available for parking ticket data. Switch datasets to view the latest parking insights.
                </p>
              </div>
            ) : null
          )}
          {dataset === 'parking_tickets' ? (
            <NeighbourhoodLeaderboard
              visible={activeTab === 'neighbourhoods'}
              onNeighbourhoodClick={handleNeighbourhoodFocus}
              initialNeighbourhoods={fallbackTopNeighbourhoods}
            />
          ) : (
            activeTab === 'neighbourhoods' ? (
              <div className="leaderboard neighbourhood-leaderboard">
                <p className="subtitle" style={{ marginBottom: 0 }}>
                  Neighbourhood trends are currently limited to parking ticket data. Switch datasets to explore parking activity by area.
                </p>
              </div>
            ) : null
          )}
          <HowItWorks />
        </div>
      </div>

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
                  dataset={dataset}
                  totalsOverride={currentTotals}
                  viewportTitle="Current view"
                />
              </div>
              <div className="overlay-panel insights-panel">
                <ViewportInsights
                  summary={viewportSummary}
                  fallbackTopStreets={dataset === 'parking_tickets' ? fallbackTopStreets : []}
                  variant="compact"
                />
              </div>
            </>
          </div>
        ) : null}
      </div>

      <div className="legend-floating">
        <Legend visible={true} />
      </div>

      <div className="dataset-toggle-floating">
        <DatasetToggle value={dataset} onChange={setDataset} />
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
          />
        </Suspense>
      ) : (
        <div className="map-container">Preparing map…</div>
      )}

      {popupData && (
        <InfoPopup
          data={popupData}
          position={popupPosition}
          variant={popupVariant}
          onClose={closePopup}
        />
      )}
    </div>
  );
}

function AppProviders({ initialData, children }) {
  const providerValue = useMemo(() => {
    if (!initialData) {
      return null;
    }

    const {
      totals = null,
      topStreets = [],
      topNeighbourhoods = [],
      generatedAt = null,
    } = initialData;

    return {
      ...(totals ? { totals } : {}),
      topStreets: Array.isArray(topStreets) ? topStreets : [],
      topNeighbourhoods: Array.isArray(topNeighbourhoods) ? topNeighbourhoods : [],
      generatedAt,
    };
  }, [initialData]);

  return (
    <AppDataProvider value={providerValue}>
      {children}
    </AppDataProvider>
  );
}

export default function App({ initialData = null, isServer = false }) {
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
    <AppProviders initialData={initialData}>
      <AppContent
        isServer={isServer}
        fallbackTopStreets={fallbackTopStreets}
        fallbackTopNeighbourhoods={fallbackTopNeighbourhoods}
      />
    </AppProviders>
  );
}
