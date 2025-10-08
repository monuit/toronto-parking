import { useCallback, useEffect, useState } from 'react';
import { MapContainer } from './MapContainer.jsx';
import { CityGlowLayer } from './CityGlowLayer.jsx';
import { NeighbourhoodLayer } from './NeighbourhoodLayer.jsx';
import { PointsLayer } from './PointsLayer.jsx';
import { MAP_CONFIG } from '../lib/mapSources.js';

const POINTS_MIN_ZOOM = Math.max(MAP_CONFIG.ZOOM_THRESHOLDS.SHOW_CLUSTERS + 2, 10);

function MapExperience({
  onMapLoad,
  onPointClick,
  onNeighbourhoodClick,
  onViewportSummaryChange,
  onStreetSegmentClick,
  highlightCentrelineIds = [],
  dataset = 'parking_tickets',
}) {
  const [mapInstance, setMapInstance] = useState(null);
  const [pointsVisible, setPointsVisible] = useState(true);

  const handleLoad = useCallback((instance) => {
    setMapInstance(instance);
    if (onMapLoad) {
      onMapLoad(instance);
    }
  }, [onMapLoad]);

  useEffect(() => {
    if (!mapInstance) {
      return undefined;
    }

    const syncVisibility = () => {
      const zoom = mapInstance.getZoom();
      const shouldShowPoints = zoom >= POINTS_MIN_ZOOM;

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
  }, [mapInstance, onViewportSummaryChange]);

  return (
    <MapContainer onMapLoad={handleLoad}>
      {mapInstance && (
        <>
          {dataset === 'parking_tickets' ? (
            <CityGlowLayer
              map={mapInstance}
              visible
              onStreetClick={onStreetSegmentClick}
              highlightCentrelineIds={highlightCentrelineIds}
            />
          ) : null}
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
          />
        </>
      )}
    </MapContainer>
  );
}

export default MapExperience;
