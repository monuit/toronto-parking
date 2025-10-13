import { useEffect, useMemo, useRef, useState } from 'react';
import { Source, Layer } from 'react-map-gl/maplibre';
import { MAP_CONFIG, STYLE_CONSTANTS } from '../lib/mapSources.js';
import { loadGlowDataset } from '../lib/glowDatasetLoader.js';
import { usePmtiles } from '../context/PmtilesContext.jsx';
import { getPmtilesDataset } from '../lib/pmtilesProtocol.js';

const GLOW_STOPS = STYLE_CONSTANTS.CITY_GLOW_STOPS;

const EMPTY_FEATURE_COLLECTION = { type: 'FeatureCollection', features: [] };

const GLOW_DATASET_META = {
  parking_tickets: { minZoom: 9, maxZoom: 16, yearBase: 2008 },
  red_light_locations: { minZoom: 9, maxZoom: 15, yearBase: 2010 },
  ase_locations: { minZoom: 9, maxZoom: 15, yearBase: 2010 },
};

const GLOW_TILE_TEMPLATE = MAP_CONFIG.TILE_SOURCE?.CITY_GLOW || '/tiles/glow/{dataset}/{z}/{x}/{y}.mvt';

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

function buildMaskClause(property, mask) {
  return ['>', ['bitwise-and', ['coalesce', ['get', property], 0], mask], 0];
}

function buildFilterExpression(filter, options = {}) {
  if (!filter) {
    return null;
  }

  const clauses = ['all'];
  const {
    useMasks = false,
    yearBase = 2008,
    yearMaskProperty = 'years_mask',
    monthMaskProperty = 'months_mask',
  } = options;

  if (filter.year) {
    if (useMasks) {
      const offset = Number(filter.year) - Number(yearBase);
      if (Number.isFinite(offset) && offset >= 0 && offset < 53) {
        const mask = Math.pow(2, offset);
        clauses.push(buildMaskClause(yearMaskProperty, mask));
      }
    } else {
      clauses.push(['in', filter.year, ['get', 'years']]);
    }
  }

  if (filter.month) {
    if (useMasks) {
      const monthIndex = Number(filter.month) - 1;
      if (Number.isFinite(monthIndex) && monthIndex >= 0 && monthIndex < 12) {
        const mask = Math.pow(2, monthIndex);
        clauses.push(buildMaskClause(monthMaskProperty, mask));
      }
    } else {
      clauses.push(['in', filter.month, ['get', 'months']]);
    }
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
  const datasetMeta = GLOW_DATASET_META[dataset] || GLOW_DATASET_META.parking_tickets;

  const { manifest, ready: pmtilesReady } = usePmtiles();
  const pmtilesDataset = useMemo(
    () => (pmtilesReady ? getPmtilesDataset(manifest, dataset, 'glowDatasets') : null),
    [pmtilesReady, manifest, dataset],
  );

  const pmtilesSource = useMemo(() => {
    if (!pmtilesDataset) {
      return null;
    }
    const shards = Array.isArray(pmtilesDataset.shards) && pmtilesDataset.shards.length > 0
      ? pmtilesDataset.shards
      : [pmtilesDataset];
    const [primaryShard] = shards;
    if (!primaryShard) {
      return null;
    }
    const urlCandidates = [];
    if (primaryShard.originUrl) {
      urlCandidates.push(primaryShard.originUrl);
    }
    if (primaryShard.url && primaryShard.url !== primaryShard.originUrl) {
      urlCandidates.push(primaryShard.url);
    }
    const selectedUrl = urlCandidates.find((candidate) => typeof candidate === 'string' && candidate.length > 0);
    if (!selectedUrl) {
      return null;
    }
    const toProtocolUrl = (value) => (value.startsWith('pmtiles://') ? value : `pmtiles://${value}`);
    return {
      tilesUrl: toProtocolUrl(selectedUrl),
      minZoom: Number.isFinite(primaryShard.minZoom) ? primaryShard.minZoom : pmtilesDataset.minZoom || 9,
      maxZoom: Number.isFinite(primaryShard.maxZoom) ? primaryShard.maxZoom : pmtilesDataset.maxZoom || 16,
      vectorLayer: pmtilesDataset.vectorLayer || 'glow_lines',
      yearBase: Number.isFinite(pmtilesDataset.yearBase) ? pmtilesDataset.yearBase : null,
    };
  }, [pmtilesDataset]);

  const usingPmtiles = Boolean(pmtilesSource?.tilesUrl);
  const fallbackVectorTileUrl = useMemo(() => {
    if (usingPmtiles) {
      return null;
    }
    const template = (MAP_CONFIG.TILE_SOURCE?.CITY_GLOW || '').replace('{dataset}', dataset);
    if (!template) {
      return null;
    }
    if (/^https?:\/\//iu.test(template)) {
      return template;
    }
    if (typeof window !== 'undefined' && window.location?.origin) {
      return `${window.location.origin}${template}`;
    }
    return template;
  }, [dataset, usingPmtiles]);

  const usingVectorTiles = usingPmtiles || Boolean(fallbackVectorTileUrl);
  const [glowData, setGlowData] = useState(EMPTY_FEATURE_COLLECTION);
  const loadedDatasetRef = useRef(null);

  useEffect(() => {
    if (!visible) {
      return undefined;
    }
    if (usingVectorTiles) {
      loadedDatasetRef.current = dataset;
      setGlowData(EMPTY_FEATURE_COLLECTION);
      return undefined;
    }
    if (loadedDatasetRef.current === dataset) {
      return undefined;
    }
    let cancelled = false;
    loadedDatasetRef.current = dataset;
    setGlowData(EMPTY_FEATURE_COLLECTION);

    loadGlowDataset(dataset)
      .then((payload) => {
        if (!cancelled && payload) {
          setGlowData(payload);
        }
      })
      .catch((error) => {
        if (!cancelled && error?.name !== 'AbortError') {
          console.error(`Failed to load ${dataset} glow data`, error);
          loadedDatasetRef.current = null;
        }
      });

    return () => {
      cancelled = true;
    };
  }, [dataset, visible, usingVectorTiles]);

  const filterExpression = useMemo(() => buildFilterExpression(filter, {
    useMasks: true,
    yearBase: datasetMeta.yearBase,
    yearMaskProperty: 'years_mask',
    monthMaskProperty: 'months_mask',
  }), [filter, datasetMeta.yearBase]);

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
    const unique = new Set();
    for (const value of highlightCentrelineIds) {
      if (value === null || value === undefined) {
        continue;
      }
      if (unique.size >= 100) {
        break;
      }
      unique.add(String(value));
    }
    return Array.from(unique);
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

  const sourceId = MAP_CONFIG.SOURCE_IDS.CITY_GLOW;
  const vectorLayerName = pmtilesSource?.vectorLayer || 'glow_lines';
  const sourceProps = usingVectorTiles
    ? {
        type: 'vector',
        tiles: [pmtilesSource?.tilesUrl || fallbackVectorTileUrl],
        minzoom: pmtilesSource?.minZoom ?? 9,
        maxzoom: pmtilesSource?.maxZoom ?? 16,
        promoteId: 'centreline_id',
      }
    : {
        type: 'geojson',
        data: glowData,
        lineMetrics: true,
        promoteId: 'centreline_id',
      };

  const sourceLayerProps = usingVectorTiles
    ? { 'source-layer': vectorLayerName }
    : {};

  const softLayerMinZoom = usingVectorTiles
    ? Math.max(9, pmtilesSource?.minZoom ?? 9)
    : 9;
  const coreLayerMinZoom = usingVectorTiles
    ? Math.max(10, pmtilesSource?.minZoom ?? 10)
    : 10;
  const layerMaxZoom = usingVectorTiles
    ? Math.min(18, pmtilesSource?.maxZoom ?? 16)
    : 18;

  return (
    <Source id={sourceId} {...sourceProps}>
      <Layer
        id={MAP_CONFIG.LAYER_IDS.CITY_GLOW_SOFT}
        type="line"
        layout={lineLayout}
        paint={softGlowPaint}
        filter={layerFilter}
        minzoom={softLayerMinZoom}
        maxzoom={layerMaxZoom}
        {...sourceLayerProps}
      />
      <Layer
        id={MAP_CONFIG.LAYER_IDS.CITY_GLOW_CORE}
        type="line"
        layout={lineLayout}
        paint={coreGlowPaint}
        filter={layerFilter}
        minzoom={coreLayerMinZoom}
        maxzoom={layerMaxZoom}
        {...sourceLayerProps}
      />
      <Layer
        id={`${MAP_CONFIG.LAYER_IDS.CITY_GLOW_CORE}-highlight`}
        type="line"
        layout={highlightLayout}
        paint={highlightPaint}
        filter={highlightFilter}
        minzoom={softLayerMinZoom}
        maxzoom={layerMaxZoom}
        {...sourceLayerProps}
      />
    </Source>
  );
}
