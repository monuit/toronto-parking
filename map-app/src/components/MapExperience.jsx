import { useCallback, useEffect, useMemo, useState } from 'react';
import { MapContainer } from './MapContainer.jsx';
import { CityGlowLayer } from './CityGlowLayer.jsx';
import { NeighbourhoodLayer } from './NeighbourhoodLayer.jsx';
import { PointsLayer } from './PointsLayer.jsx';
import { WardChoroplethLayer } from './WardChoroplethLayer.jsx';
import { MAP_CONFIG } from '../lib/mapSources.js';
import { usePmtiles } from '../context/PmtilesContext.jsx';

const SUPPORTED_WARD_DATASETS = new Set(['red_light_locations', 'ase_locations', 'cameras_combined']);

function lonLatToTileIndices(longitude, latitude, zoom) {
  const tileZoom = Math.max(0, Math.floor(zoom));
  const scale = 2 ** tileZoom;
  const x = Math.floor(((longitude + 180) / 360) * scale);
  const latRad = latitude * (Math.PI / 180);
  const y = Math.floor(
    ((1 - Math.log(Math.tan(latRad) + (1 / Math.cos(latRad))) / Math.PI) / 2) * scale,
  );
  return { z: tileZoom, x, y };
}

function buildTileURL(template, z, x, y) {
  return template
    .replace('{z}', String(z))
    .replace('{x}', String(x))
    .replace('{y}', String(y));
}

function MapExperience({
  onMapLoad,
  onPointClick,
  onNeighbourhoodClick,
  onViewportSummaryChange,
  onStreetSegmentClick,
  highlightCentrelineIds = [],
  dataset = 'parking_tickets',
  filter = null,
  viewMode = 'detail',
  wardDataset = null,
  onWardClick,
  onWardHover,
  isTouchDevice = false,
}) {
  const { manifest: pmtilesManifest, ready: pmtilesReady } = usePmtiles();
  const [mapInstance, setMapInstance] = useState(null);
  const [pointsVisible, setPointsVisible] = useState(true);
  const [basemapReady, setBasemapReady] = useState(false);
  const wardDatasetId = useMemo(() => {
    if (wardDataset && SUPPORTED_WARD_DATASETS.has(wardDataset)) {
      return wardDataset;
    }
    if (SUPPORTED_WARD_DATASETS.has(dataset)) {
      return dataset;
    }
    return null;
  }, [wardDataset, dataset]);
  const pointsMinZoom = useMemo(
    () => (dataset === 'parking_tickets'
      ? MAP_CONFIG.TILE_MIN_ZOOM
      : 7.5),
    [dataset],
  );

  const handleLoad = useCallback((instance) => {
    if (instance?.setPrefetchZoomDelta) {
      instance.setPrefetchZoomDelta(1);
    }
    if (instance?.setSourceTileCacheSize) {
      try {
        instance.setSourceTileCacheSize(MAP_CONFIG.SOURCE_IDS.TICKETS, 256);
      } catch {
        // Older MapLibre versions may not support this API.
      }
    }
    setMapInstance(instance);
    if (onMapLoad) {
      onMapLoad(instance);
    }
  }, [onMapLoad]);

  useEffect(() => {
    if (!mapInstance) {
      setBasemapReady(false);
      return undefined;
    }

    if (typeof mapInstance.isStyleLoaded === 'function' && mapInstance.isStyleLoaded()) {
      setBasemapReady(true);
      return undefined;
    }

    let cancelled = false;
    const markReady = () => {
      if (!cancelled) {
        setBasemapReady(true);
      }
    };

    mapInstance.once('idle', markReady);
    mapInstance.once('styledata', markReady);
    const timeoutId = setTimeout(() => {
      if (typeof mapInstance.isStyleLoaded === 'function' && mapInstance.isStyleLoaded()) {
        markReady();
      }
    }, 4000);

    return () => {
      cancelled = true;
      mapInstance.off('idle', markReady);
      mapInstance.off('styledata', markReady);
      clearTimeout(timeoutId);
    };
  }, [mapInstance]);

  useEffect(() => {
    if (!mapInstance) {
      return undefined;
    }

    const syncVisibility = () => {
      const zoom = mapInstance.getZoom();
      const shouldShowPoints = viewMode !== 'ward' && zoom >= pointsMinZoom;

      setPointsVisible(shouldShowPoints);

      if (!shouldShowPoints && typeof onViewportSummaryChange === 'function') {
        onViewportSummaryChange({ zoomRestricted: true, topStreets: [] });
      }
    };

    syncVisibility();
    mapInstance.on('zoomend', syncVisibility);
    mapInstance.on('moveend', syncVisibility);

    return () => {
      mapInstance.off('zoomend', syncVisibility);
      mapInstance.off('moveend', syncVisibility);
    };
  }, [mapInstance, onViewportSummaryChange, pointsMinZoom, viewMode]);

  const datasetUsesPmtiles = useMemo(() => {
    if (!pmtilesReady || !pmtilesManifest?.datasets) {
      return false;
    }
    return Boolean(pmtilesManifest.datasets[dataset]);
  }, [dataset, pmtilesManifest, pmtilesReady]);

  useEffect(() => {
    if (!mapInstance || dataset !== 'parking_tickets' || isTouchDevice || datasetUsesPmtiles) {
      return undefined;
    }

    const controller = new AbortController();
    const { signal } = controller;

    const prefetchTiles = async () => {
      try {
        const center = mapInstance.getCenter();
        const origin = typeof window !== 'undefined' && window.location?.origin
          ? window.location.origin
          : '';
        const template = MAP_CONFIG.TILE_SOURCE.TICKETS.replace('{dataset}', dataset);
        const absoluteTemplate = template.startsWith('http') ? template : `${origin}${template}`;
        const zoomLevels = [
          MAP_CONFIG.TILE_MIN_ZOOM,
          MAP_CONFIG.TILE_MIN_ZOOM + 1,
        ];

        const requests = [];
        for (const zoom of zoomLevels) {
          const { z, x, y } = lonLatToTileIndices(center.lng, center.lat, zoom);
          const neighbours = [
            [x, y],
            [x + 1, y],
            [x - 1, y],
            [x, y + 1],
            [x, y - 1],
          ];
          for (const [tileX, tileY] of neighbours) {
            if (tileX < 0 || tileY < 0) {
              continue;
            }
            const url = buildTileURL(absoluteTemplate, z, tileX, tileY);
            requests.push(
              fetch(url, {
                method: 'GET',
                cache: 'force-cache',
                signal,
              }).then((response) => {
                if (!response.ok) {
                  return null;
                }
                return response.arrayBuffer().catch(() => null);
              }).catch(() => null),
            );
          }
        }
        await Promise.all(requests);
      } catch (error) {
        if (error.name !== 'AbortError') {
          console.warn('Tile prefetch failed:', error.message);
        }
      }
    };

    prefetchTiles();

    return () => {
      controller.abort();
    };
  }, [mapInstance, dataset, isTouchDevice, datasetUsesPmtiles]);

  return (
    <MapContainer onMapLoad={handleLoad}>
      <div
        className={`map-loading-overlay ${basemapReady ? 'map-loading-overlay--hidden' : ''}`}
        aria-hidden="true"
      >
        <div className="map-loading-overlay__gradient" />
      </div>
      {mapInstance && (
        <>
          <CityGlowLayer
            map={mapInstance}
            visible={viewMode !== 'ward'}
            onStreetClick={dataset === 'parking_tickets' ? onStreetSegmentClick : undefined}
            highlightCentrelineIds={dataset === 'parking_tickets' ? highlightCentrelineIds : []}
            dataset={dataset}
          />
          <NeighbourhoodLayer
            map={mapInstance}
            visible={false}
            onClick={onNeighbourhoodClick}
          />
          <PointsLayer
            map={mapInstance}
            visible={pointsVisible}
            onPointClick={onPointClick}
            onViewportSummaryChange={onViewportSummaryChange}
            dataset={dataset}
            filter={filter}
            isTouchDevice={isTouchDevice}
          />
          {wardDatasetId ? (
            <WardChoroplethLayer
              map={mapInstance}
              visible={viewMode === 'ward'}
              dataset={wardDatasetId}
              onWardClick={onWardClick}
              onWardHover={onWardHover}
            />
          ) : null}
        </>
      )}
    </MapContainer>
  );
}

export default MapExperience;
