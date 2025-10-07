/**
 * MapContainer - Base map initialization and rendering
 * Single responsibility: initialize MapLibre GL instance with base style
 */
import { useRef, useState } from 'react';
import Map from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { MAP_CONFIG } from '../lib/mapSources';

export function MapContainer({ children, onMapLoad }) {
  const mapRef = useRef(null);
  const [viewState, setViewState] = useState(MAP_CONFIG.DEFAULT_VIEW);
  
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
      >
        {children}
      </Map>
    </div>
  );
}
