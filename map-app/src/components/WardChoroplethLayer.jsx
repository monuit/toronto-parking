import { useEffect, useMemo } from 'react';
import { Source, Layer } from 'react-map-gl/maplibre';
import { MAP_CONFIG, STYLE_CONSTANTS } from '../lib/mapSources.js';
import { usePmtiles } from '../context/PmtilesContext.jsx';
import { getPmtilesDataset, getPmtilesShardUrl } from '../lib/pmtilesProtocol.js';

const SUPPORTED_DATASETS = new Set(['red_light_locations', 'ase_locations', 'cameras_combined']);

function buildColorStops(stops) {
  const expression = ['interpolate', ['linear'], ['coalesce', ['get', 'ticketCount'], 0]];
  stops.forEach(([value, color]) => {
    expression.push(value);
    expression.push(color);
  });
  return expression;
}

export function WardChoroplethLayer({
  map,
  dataset = 'red_light_locations',
  visible = true,
  onWardClick,
  onWardHover,
}) {
  const { manifest: pmtilesManifest, ready: pmtilesReady } = usePmtiles();
  const pmtilesDataset = useMemo(
    () => (pmtilesReady ? getPmtilesDataset(pmtilesManifest, dataset, 'wardDatasets') : null),
    [pmtilesManifest, pmtilesReady, dataset],
  );
  const pmtilesUrl = useMemo(() => {
    const shardUrl = getPmtilesShardUrl(pmtilesDataset);
    return shardUrl ? `pmtiles://${shardUrl}` : null;
  }, [pmtilesDataset]);
  const sourceId = useMemo(() => `${dataset}-ward-source`, [dataset]);
  const fillLayerId = useMemo(() => `${dataset}-ward-fill`, [dataset]);
  const outlineLayerId = useMemo(() => `${dataset}-ward-outline`, [dataset]);
  const tileUrl = useMemo(
    () => {
      if (pmtilesUrl) {
        return pmtilesUrl;
      }
      if (!SUPPORTED_DATASETS.has(dataset)) {
        return null;
      }
      const template = MAP_CONFIG.TILE_SOURCE.WARD.replace('{dataset}', dataset);
      if (/^https?:\/\//i.test(template)) {
        return template;
      }
      if (typeof window !== 'undefined' && window.location?.origin) {
        return `${window.location.origin}${template}`;
      }
      return template;
    },
    [dataset, pmtilesUrl],
  );

  const layerSourceProps = useMemo(
    () => ({ 'source-layer': pmtilesDataset?.vectorLayer || STYLE_CONSTANTS.WARD_TILE_SOURCE_LAYER }),
    [pmtilesDataset],
  );

  const fillPaint = useMemo(
    () => ({
      'fill-color': buildColorStops(STYLE_CONSTANTS.WARD_CHOROPLETH_STOPS),
      'fill-opacity': visible ? 0.7 : 0,
    }),
    [visible],
  );

  const outlinePaint = useMemo(
    () => ({
      'line-color': '#1f2933',
      'line-width': 1,
      'line-opacity': visible ? 0.4 : 0,
    }),
    [visible],
  );

  const isSupported = SUPPORTED_DATASETS.has(dataset);

  useEffect(() => {
    if (!map || !isSupported) {
      return undefined;
    }

    let detached = false;

    const handleClick = (event) => {
      if (!onWardClick) {
        return;
      }
      const feature = event.features && event.features[0];
      if (feature) {
        onWardClick(feature.properties || null, event);
      }
    };

    const handleHover = (event) => {
      if (!onWardHover) {
        map.getCanvas().style.cursor = 'default';
        return;
      }
      const feature = event.features && event.features[0];
      if (feature) {
        map.getCanvas().style.cursor = 'pointer';
        onWardHover(feature.properties || null, event);
      } else {
        map.getCanvas().style.cursor = 'default';
        onWardHover(null, null);
      }
    };

    const handleMouseLeave = () => {
      if (onWardHover) {
        onWardHover(null, null);
      }
      map.getCanvas().style.cursor = 'default';
    };

    const detachHandlers = () => {
      if (detached) {
        return;
      }
      detached = true;
      try {
        map.off('click', fillLayerId, handleClick);
        map.off('mousemove', fillLayerId, handleHover);
        map.off('mouseleave', fillLayerId, handleMouseLeave);
      } catch (error) {
        console.warn('Failed to unbind ward layer events:', error);
      }
      map.getCanvas().style.cursor = 'default';
    };

    const attachHandlers = () => {
      if (detached) {
        return;
      }
      const layer = map.getLayer(fillLayerId);
      if (!layer) {
        return;
      }
      map.on('click', fillLayerId, handleClick);
      map.on('mousemove', fillLayerId, handleHover);
      map.on('mouseleave', fillLayerId, handleMouseLeave);
      detached = false;
    };

    if (map.getLayer(fillLayerId)) {
      attachHandlers();
    } else {
      const handleStyleData = () => {
        if (map.getLayer(fillLayerId)) {
          attachHandlers();
          map.off('styledata', handleStyleData);
        }
      };
      map.on('styledata', handleStyleData);
      return () => {
        map.off('styledata', handleStyleData);
        detachHandlers();
      };
    }

    return () => {
      detachHandlers();
    };
  }, [map, fillLayerId, onWardClick, onWardHover, isSupported]);

  if (!isSupported || !map || !tileUrl) {
    return null;
  }

  return (
    <Source
      key={sourceId}
      id={sourceId}
      type="vector"
  tiles={[tileUrl]}
  minzoom={pmtilesDataset?.minZoom ?? 0}
  maxzoom={pmtilesDataset?.maxZoom ?? 14}
    >
      <Layer
        id={fillLayerId}
        type="fill"
        paint={fillPaint}
        layout={{ visibility: visible ? 'visible' : 'none' }}
        {...layerSourceProps}
      />
      <Layer
        id={outlineLayerId}
        type="line"
        paint={outlinePaint}
        layout={{ visibility: visible ? 'visible' : 'none' }}
        {...layerSourceProps}
      />
    </Source>
  );
}

export default WardChoroplethLayer;
