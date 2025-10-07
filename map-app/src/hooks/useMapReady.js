/**
 * Custom hook for map readiness
 * Single responsibility: manage map load state
 */
import { useState, useEffect } from 'react';

export function useMapReady(mapRef) {
  const [isReady, setIsReady] = useState(false);
  
  useEffect(() => {
    if (!mapRef.current) return;
    
    const map = mapRef.current.getMap();
    
    if (map.loaded()) {
      setIsReady(true);
    } else {
      const handleLoad = () => setIsReady(true);
      map.on('load', handleLoad);
      
      return () => {
        map.off('load', handleLoad);
      };
    }
  }, [mapRef]);
  
  return isReady;
}
