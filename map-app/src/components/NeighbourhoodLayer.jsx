/**
 * NeighbourhoodLayer - Polygon layer for Toronto neighbourhoods
 * Single responsibility: render and manage neighbourhood overlay with interactivity
 */
import { useEffect, useState } from 'react';
import { Source, Layer } from 'react-map-gl/maplibre';
import { MAP_CONFIG, STYLE_CONSTANTS } from '../lib/mapSources';

export function NeighbourhoodLayer({ map, visible = true, onHover, onClick }) {
  const [neighbourhoods, setNeighbourhoods] = useState(null);
  const [hoveredId, setHoveredId] = useState(null);
  
  useEffect(() => {
    // Load neighbourhood data
    fetch(MAP_CONFIG.DATA_PATHS.NEIGHBOURHOODS)
      .then(res => res.json())
      .then(data => setNeighbourhoods(data))
      .catch(err => console.error('Failed to load neighbourhoods:', err));
  }, []);
  
  useEffect(() => {
    if (!map || !neighbourhoods) return;
    
    const handleMouseMove = (e) => {
      if (e.features && e.features.length > 0) {
        const feature = e.features[0];
        setHoveredId(feature.id);
        if (onHover) {
          onHover(feature.properties);
        }
        map.getCanvas().style.cursor = 'pointer';
      }
    };
    
    const handleMouseLeave = () => {
      setHoveredId(null);
      if (onHover) {
        onHover(null);
      }
      map.getCanvas().style.cursor = '';
    };
    
    const handleClick = (e) => {
      if (e.features && e.features.length > 0 && onClick) {
        onClick(e.features[0].properties);
      }
    };
    
    map.on('mousemove', MAP_CONFIG.LAYER_IDS.NEIGHBOURHOODS_FILL, handleMouseMove);
    map.on('mouseleave', MAP_CONFIG.LAYER_IDS.NEIGHBOURHOODS_FILL, handleMouseLeave);
    map.on('click', MAP_CONFIG.LAYER_IDS.NEIGHBOURHOODS_FILL, handleClick);
    
    return () => {
      map.off('mousemove', MAP_CONFIG.LAYER_IDS.NEIGHBOURHOODS_FILL, handleMouseMove);
      map.off('mouseleave', MAP_CONFIG.LAYER_IDS.NEIGHBOURHOODS_FILL, handleMouseLeave);
      map.off('click', MAP_CONFIG.LAYER_IDS.NEIGHBOURHOODS_FILL, handleClick);
    };
  }, [map, neighbourhoods, onHover, onClick]);
  
  if (!neighbourhoods || !visible) return null;
  
  const fillLayer = {
    id: MAP_CONFIG.LAYER_IDS.NEIGHBOURHOODS_FILL,
    type: 'fill',
    paint: {
      'fill-color': [
        'case',
        ['==', ['id'], hoveredId],
        STYLE_CONSTANTS.COLORS.NEIGHBOURHOOD_HOVER,
        STYLE_CONSTANTS.COLORS.NEIGHBOURHOOD_FILL
      ],
      'fill-opacity': 0.6
    }
  };
  
  const outlineLayer = {
    id: MAP_CONFIG.LAYER_IDS.NEIGHBOURHOODS_OUTLINE,
    type: 'line',
    paint: {
      'line-color': STYLE_CONSTANTS.COLORS.NEIGHBOURHOOD_OUTLINE,
      'line-width': 1.5
    }
  };
  
  return (
    <Source
      id={MAP_CONFIG.SOURCE_IDS.NEIGHBOURHOODS}
      type="geojson"
      data={neighbourhoods}
      generateId={true}
    >
      <Layer {...fillLayer} />
      <Layer {...outlineLayer} />
    </Source>
  );
}
