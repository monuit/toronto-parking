/**
 * MapContainer - Base map initialization and rendering
 * Single responsibility: initialize MapLibre GL instance with base style
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import Map from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { MAP_CONFIG } from '../lib/mapSources';

export function MapContainer({ children, onMapLoad }) {
  const mapRef = useRef(null);
  const [viewState, setViewState] = useState(MAP_CONFIG.DEFAULT_VIEW);
  const [isTouchDevice, setIsTouchDevice] = useState(false);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return undefined;
    }
    const mediaQuery = window.matchMedia('(pointer: coarse)');
    const update = () => setIsTouchDevice(mediaQuery.matches);
    update();
    if (typeof mediaQuery.addEventListener === 'function') {
      mediaQuery.addEventListener('change', update);
      return () => mediaQuery.removeEventListener('change', update);
    }
    if (typeof mediaQuery.addListener === 'function') {
      mediaQuery.addListener(update);
      return () => mediaQuery.removeListener(update);
    }
    return undefined;
  }, []);

  const interactionOptions = useMemo(() => ({
    dragRotate: !isTouchDevice,
    doubleClickZoom: !isTouchDevice,
    scrollZoom: true,
    touchZoomRotate: true,
  }), [isTouchDevice]);
  
  const handleLoad = () => {
    if (onMapLoad && mapRef.current) {
      onMapLoad(mapRef.current.getMap());
    }
  };
  
  return (
    <div className="map-container">
      <Map
        ref={mapRef}
        {...viewState}
        onMove={evt => setViewState(evt.viewState)}
        onLoad={handleLoad}
        mapStyle={MAP_CONFIG.STYLE_URL}
        style={{ width: '100%', height: '100%' }}
        maxZoom={18}
        minZoom={9}
        attributionControl={false}
        glOptions={{
          premultipliedAlpha: false,
          antialias: true,
          preserveDrawingBuffer: false,
        }}
        dragRotate={interactionOptions.dragRotate}
        doubleClickZoom={interactionOptions.doubleClickZoom}
        scrollZoom={interactionOptions.scrollZoom}
        touchZoomRotate={interactionOptions.touchZoomRotate}
      >
        {children}
      </Map>
    </div>
  );
}
