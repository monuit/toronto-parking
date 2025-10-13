/**
 * MapContainer - Base map initialization and rendering
 * Single responsibility: initialize MapLibre GL instance with base style
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Map from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { MAP_CONFIG } from '../lib/mapSources';

export function MapContainer({ children, onMapLoad }) {
  const mapRef = useRef(null);
  const [viewState, setViewState] = useState(MAP_CONFIG.DEFAULT_VIEW);
  const [isTouchDevice, setIsTouchDevice] = useState(false);
  const [shouldMountMap, setShouldMountMap] = useState(() => typeof window === 'undefined');

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

  useEffect(() => {
    if (typeof window === 'undefined') {
      return undefined;
    }
    let cancelled = false;

    const activate = () => {
      if (cancelled) {
        return;
      }
      window.requestAnimationFrame(() => {
        if (!cancelled) {
          setShouldMountMap(true);
        }
      });
    };

    const waitForFonts = () => {
      if (typeof document !== 'undefined' && document.fonts && typeof document.fonts.ready === 'object') {
        document.fonts.ready.then(activate).catch(activate);
      } else {
        activate();
      }
    };

    if (document.readyState === 'complete') {
      waitForFonts();
    } else {
      const handleWindowLoad = () => {
        window.removeEventListener('load', handleWindowLoad);
        waitForFonts();
      };
      window.addEventListener('load', handleWindowLoad);
      return () => {
        cancelled = true;
        window.removeEventListener('load', handleWindowLoad);
      };
    }

    return () => {
      cancelled = true;
    };
  }, []);

  const containerStyle = useMemo(() => ({
    width: '100%',
    height: 'calc(100vh - var(--map-header-offset, 0px))',
    minHeight: '520px',
  }), []);

  const transformRequest = useCallback((url) => {
    if (typeof url !== 'string' || typeof window === 'undefined') {
      return { url };
    }
    if (url.startsWith('http://') || url.startsWith('https://')) {
      return { url };
    }
    if (url.startsWith('/proxy/') || url.startsWith('/tiles/') || url.startsWith('/styles/')) {
      const origin = window.location?.origin || '';
      if (origin) {
        return { url: `${origin}${url}` };
      }
    }
    if (url.startsWith('./')) {
      const origin = window.location?.origin || '';
      if (origin) {
        return { url: `${origin}${url.slice(1)}` };
      }
    }
    return { url };
  }, []);
  
  return (
    <div className="map-container" style={containerStyle}>
      {shouldMountMap ? (
        <Map
          ref={mapRef}
          {...viewState}
          onMove={(evt) => setViewState(evt.viewState)}
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
          transformRequest={transformRequest}
        >
          {children}
        </Map>
      ) : null}
    </div>
  );
}
