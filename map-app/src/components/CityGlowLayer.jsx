import { useEffect, useMemo, useState } from 'react';
import { Source, Layer } from 'react-map-gl/maplibre';
import { MAP_CONFIG, STYLE_CONSTANTS } from '../lib/mapSources.js';

const EMPTY_FEATURE_COLLECTION = { type: 'FeatureCollection', features: [] };

const GLOW_STOPS = STYLE_CONSTANTS.CITY_GLOW_STOPS;

function hexToRgba(hex, alpha) {
  const normalized = hex.replace('#', '');
  const r = parseInt(normalized.slice(0, 2), 16);
  const g = parseInt(normalized.slice(2, 4), 16);
  const b = parseInt(normalized.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function buildColorExpression(countExpression, alphaStart, alphaEnd) {
  const expression = ['interpolate', ['linear'], countExpression];
  expression.push(0);
  expression.push('rgba(0, 0, 0, 0)');

  const lastIndex = GLOW_STOPS.length - 1;
  GLOW_STOPS.forEach((stop, index) => {
    const progress = lastIndex === 0 ? 1 : index / lastIndex;
    const alpha = alphaStart + (alphaEnd - alphaStart) * progress;
    expression.push(stop.value);
    expression.push(hexToRgba(stop.color, Number(alpha.toFixed(2))));
  });

  return expression;
}

function buildFilterExpression(filter) {
  if (!filter) {
    return null;
  }

  const clauses = ['all'];

  if (filter.year) {
    clauses.push(['in', filter.year, ['get', 'years']]);
  }

  if (filter.month) {
    clauses.push(['in', filter.month, ['get', 'months']]);
  }

  return clauses.length > 1 ? clauses : null;
}

export function CityGlowLayer({
  map,
  visible = true,
  filter = null,
  onStreetClick,
  highlightCentrelineIds = [],
  dataset = 'parking_tickets',
}) {
  const [glowData, setGlowData] = useState(EMPTY_FEATURE_COLLECTION);
  const dataPath = useMemo(() => {
    if (dataset === 'red_light_locations') {
      return MAP_CONFIG.DATA_PATHS.RED_LIGHT_GLOW_LINES;
    }
    if (dataset === 'ase_locations') {
      return MAP_CONFIG.DATA_PATHS.ASE_GLOW_LINES;
    }
    return MAP_CONFIG.DATA_PATHS.CITY_GLOW_LINES;
  }, [dataset]);

  useEffect(() => {
    let isCancelled = false;

    fetch(dataPath)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Failed to load glow dataset: ${response.status}`);
        }
        return response.json();
      })
      .then((payload) => {
        if (!isCancelled) {
          setGlowData(payload);
        }
      })
      .catch((error) => {
        if (!isCancelled) {
          console.error('Failed to load city glow data', error);
        }
      });

    return () => {
      isCancelled = true;
    };
  }, [dataPath]);

  const filterExpression = useMemo(() => buildFilterExpression(filter), [filter]);

  const layerFilter = useMemo(() => {
    const baseFilter = ['>', ['coalesce', ['get', 'count'], 0], 0];
    if (!filterExpression) {
      return baseFilter;
    }
    if (Array.isArray(filterExpression) && filterExpression[0] === 'all') {
      return ['all', baseFilter, ...filterExpression.slice(1)];
    }
    return ['all', baseFilter, filterExpression];
  }, [filterExpression]);

  const lineLayout = useMemo(
    () => ({
      visibility: visible ? 'visible' : 'none',
      'line-cap': 'round',
      'line-join': 'round'
    }),
    [visible]
  );

  const softGlowPaint = useMemo(() => {
    const countExpression = ['coalesce', ['get', 'count'], 0];
    const weightExpression = [
      'interpolate',
      ['linear'],
      countExpression,
      0, 0,
      10, 0.55,
      80, 1.08,
      260, 1.95,
      800, 3.05
    ];
    const lineColor = buildColorExpression(countExpression, 0.18, 0.68);

    return {
      'line-color': lineColor,
      'line-width': [
        'interpolate',
        ['linear'],
        ['zoom'],
        8, ['+', 0.48, ['*', 0.24, weightExpression]],
        11, ['+', 0.98, ['*', 0.38, weightExpression]],
        13.5, ['+', 1.6, ['*', 0.54, weightExpression]],
        15.5, ['+', 2.1, ['*', 0.7, weightExpression]]
      ],
      'line-opacity': [
        'interpolate',
        ['linear'],
        ['zoom'],
        8, 0.44,
        11, 0.38,
        14, 0.26,
        16, 0.14
      ],
      'line-blur': [
        'interpolate',
        ['linear'],
        ['zoom'],
        8, 0.9,
        11, 0.76,
        14, 0.54,
        16, 0.32
      ]
    };
  }, []);

  const coreGlowPaint = useMemo(() => {
    const countExpression = ['coalesce', ['get', 'count'], 0];
    const weightExpression = [
      'interpolate',
      ['linear'],
      countExpression,
      0, 0,
      30, 0.52,
      150, 1.12,
      400, 1.95,
      900, 3.2
    ];
    const lineColor = buildColorExpression(countExpression, 0.42, 0.98);

    return {
      'line-color': lineColor,
      'line-width': [
        'interpolate',
        ['linear'],
        ['zoom'],
        9, ['+', 0.3, ['*', 0.36, weightExpression]],
        12, ['+', 0.54, ['*', 0.56, weightExpression]],
        14.5, ['+', 0.9, ['*', 0.84, weightExpression]],
        16, ['+', 1.12, ['*', 0.98, weightExpression]]
      ],
      'line-opacity': [
        'interpolate',
        ['linear'],
        ['zoom'],
        9, 0.66,
        12, 0.6,
        14.5, 0.51,
        16, 0.42
      ],
      'line-blur': [
        'interpolate',
        ['linear'],
        ['zoom'],
        9, 0.48,
        13, 0.34,
        16, 0.25
      ]
    };
  }, []);

  const highlightValues = useMemo(() => {
    if (dataset !== 'parking_tickets' || !Array.isArray(highlightCentrelineIds)) {
      return [];
    }
    return highlightCentrelineIds
      .map((value) => (value === null || value === undefined ? null : String(value)))
      .filter((value) => Boolean(value));
  }, [highlightCentrelineIds, dataset]);

  const highlightFilter = useMemo(() => {
    if (highlightValues.length === 0) {
      return ['==', ['get', 'centreline_id'], -1];
    }
    if (highlightValues.length === 1) {
      return ['==', ['to-string', ['get', 'centreline_id']], highlightValues[0]];
    }
    return ['in', ['to-string', ['get', 'centreline_id']], ['literal', highlightValues]];
  }, [highlightValues]);

  const highlightLayout = useMemo(() => ({
    visibility: visible && dataset === 'parking_tickets' && highlightValues.length > 0 ? 'visible' : 'none',
    'line-cap': 'round',
    'line-join': 'round',
  }), [visible, dataset, highlightValues]);

  const highlightPaint = useMemo(() => ({
    'line-color': 'rgba(255, 255, 255, 0.95)',
    'line-width': [
      'interpolate',
      ['linear'],
      ['zoom'],
      9, 2.6,
      12, 4.4,
      14.5, 6.6,
      16.5, 9.2,
    ],
    'line-opacity': 0.96,
    'line-blur': 0.18,
  }), []);

  useEffect(() => {
    if (!map || !visible || dataset !== 'parking_tickets' || typeof onStreetClick !== 'function') {
      return undefined;
    }

    const layerIds = [
      MAP_CONFIG.LAYER_IDS.CITY_GLOW_SOFT,
      MAP_CONFIG.LAYER_IDS.CITY_GLOW_CORE,
    ];

    const handleClick = (event) => {
      if (!event?.features?.length) {
        return;
      }
      const feature = event.features[0];
      const properties = feature?.properties || {};
      const centrelineId = properties.centreline_id ?? properties.centrelineId;
      if (centrelineId === null || centrelineId === undefined) {
        return;
      }
      onStreetClick(
        Number.isFinite(Number(centrelineId)) ? Number(centrelineId) : centrelineId,
        feature,
        event,
      );
    };

    const handleMouseEnter = () => {
      map.getCanvas().style.cursor = 'pointer';
    };
    const handleMouseLeave = () => {
      map.getCanvas().style.cursor = '';
    };

    layerIds.forEach((layerId) => {
      map.on('click', layerId, handleClick);
      map.on('mouseenter', layerId, handleMouseEnter);
      map.on('mouseleave', layerId, handleMouseLeave);
    });

    return () => {
      layerIds.forEach((layerId) => {
        map.off('click', layerId, handleClick);
        map.off('mouseenter', layerId, handleMouseEnter);
        map.off('mouseleave', layerId, handleMouseLeave);
      });
    };
  }, [map, visible, onStreetClick, dataset]);

  if (!map) {
    return null;
  }

  return (
    <Source
      id={MAP_CONFIG.SOURCE_IDS.CITY_GLOW}
      type="geojson"
      lineMetrics
      data={glowData}
    >
      <Layer
        id={MAP_CONFIG.LAYER_IDS.CITY_GLOW_SOFT}
        type="line"
        layout={lineLayout}
        paint={softGlowPaint}
        filter={layerFilter}
        minzoom={7}
        maxzoom={18}
      />
      <Layer
        id={MAP_CONFIG.LAYER_IDS.CITY_GLOW_CORE}
        type="line"
        layout={lineLayout}
        paint={coreGlowPaint}
        filter={layerFilter}
        minzoom={8}
        maxzoom={18}
      />
      <Layer
        id={`${MAP_CONFIG.LAYER_IDS.CITY_GLOW_CORE}-highlight`}
        type="line"
        layout={highlightLayout}
        paint={highlightPaint}
        filter={highlightFilter}
        minzoom={7}
        maxzoom={18}
      />
    </Source>
  );
}
