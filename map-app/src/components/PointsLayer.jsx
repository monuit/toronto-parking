/**
 * PointsLayer - Ticket points with server-driven clustering
 * Single responsibility: render ticket data from vector tiles and handle interactions
 */
import { useCallback, useEffect, useMemo, useRef } from 'react';
import { Source, Layer } from 'react-map-gl/maplibre';
import { MAP_CONFIG, STYLE_CONSTANTS } from '../lib/mapSources';

function buildFilterExpression(isCluster, filter) {
  const expression = ['all'];
  expression.push(isCluster ? ['==', ['get', 'cluster'], true] : ['!=', ['get', 'cluster'], true]);

  if (filter?.year) {
    expression.push(['in', filter.year, ['get', 'years']]);
  }

  if (filter?.month) {
    expression.push(['in', filter.month, ['get', 'months']]);
  }

  return expression;
}

const SUMMARY_ZOOM_THRESHOLD = MAP_CONFIG.ZOOM_THRESHOLDS.SUMMARY_MIN;
const TILE_LAYER_NAME = MAP_CONFIG.SOURCE_LAYERS.TICKETS;

function parsePgArray(raw) {
  if (Array.isArray(raw)) {
    return raw;
  }
  if (typeof raw !== 'string') {
    return [];
  }
  const trimmed = raw.trim();
  if (!trimmed.startsWith('{') || !trimmed.endsWith('}')) {
    return [];
  }
  const inner = trimmed.slice(1, -1).trim();
  if (!inner) {
    return [];
  }
  return inner.split(',').map((segment) => segment.trim().replace(/^"|"$/g, ''));
}

function parseIntArray(raw) {
  return parsePgArray(raw)
    .map((segment) => {
      const value = Number(segment);
      return Number.isFinite(value) ? value : null;
    })
    .filter((value) => value !== null);
}

function parseJsonProperty(raw) {
  if (!raw) {
    return null;
  }
  if (typeof raw === 'object') {
    return raw;
  }
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw);
    } catch (error) {
      console.warn('Failed to parse JSON property from tile', error);
      return null;
    }
  }
  return null;
}

export function PointsLayer({
  map,
  visible = true,
  onPointClick,
  filter,
  onViewportSummaryChange,
  dataset = 'parking_tickets',
}) {
  const summaryAbortRef = useRef(null);
  const summaryTimeoutRef = useRef(null);

  const clusterFilter = useMemo(() => buildFilterExpression(true, filter), [filter]);
  const pointFilter = useMemo(() => buildFilterExpression(false, filter), [filter]);

  const scheduleSummaryFetch = useCallback(() => {
    if (!map || !onViewportSummaryChange) {
      return;
    }

    const zoom = map.getZoom();
    if (zoom < SUMMARY_ZOOM_THRESHOLD) {
      onViewportSummaryChange({ zoomRestricted: true, topStreets: [] });
      return;
    }

    const bounds = map.getBounds();
    const params = new URLSearchParams({
      west: bounds.getWest().toFixed(6),
      south: bounds.getSouth().toFixed(6),
      east: bounds.getEast().toFixed(6),
      north: bounds.getNorth().toFixed(6),
      zoom: zoom.toFixed(2),
      dataset,
    });

    if (filter?.year) {
      params.append('year', String(filter.year));
    }
    if (filter?.month) {
      params.append('month', String(filter.month));
    }

    summaryAbortRef.current?.abort();
    const controller = new AbortController();
    summaryAbortRef.current = controller;

    fetch(`${MAP_CONFIG.API_PATHS.SUMMARY}?${params.toString()}`, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Summary request failed with status ${response.status}`);
        }
        return response.json();
      })
      .then((payload) => {
        onViewportSummaryChange?.(payload);
      })
      .catch((error) => {
        if (error.name !== 'AbortError') {
          console.error('Failed to load viewport summary', error);
        }
      });
  }, [filter, map, onViewportSummaryChange, dataset]);

  useEffect(() => {
    if (!map || !visible || !onViewportSummaryChange) {
      return undefined;
    }

    const handleMove = () => {
      if (summaryTimeoutRef.current) {
        clearTimeout(summaryTimeoutRef.current);
      }
      summaryTimeoutRef.current = setTimeout(scheduleSummaryFetch, 120);
    };

    scheduleSummaryFetch();
    map.on('moveend', handleMove);
    map.on('zoomend', handleMove);

    return () => {
      summaryAbortRef.current?.abort();
      if (summaryTimeoutRef.current) {
        clearTimeout(summaryTimeoutRef.current);
      }
      map.off('moveend', handleMove);
      map.off('zoomend', handleMove);
    };
  }, [map, visible, scheduleSummaryFetch, onViewportSummaryChange, dataset]);

  const handleFeatureInteraction = useCallback((event) => {
    if (!event?.features?.length || !map) {
      return;
    }
    const feature = event.features[0];
    const properties = { ...(feature.properties || {}) };
    const toNumeric = (value) => {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : null;
    };

    if (dataset !== 'parking_tickets') {
      const normalizedCount = toNumeric(
        properties.count ?? properties.ticket_count ?? properties.ticketCount,
      );
      if (normalizedCount !== null) {
        properties.count = normalizedCount;
      }

      const normalizedRevenue = toNumeric(
        properties.total_revenue ?? properties.total_fine_amount,
      );
      if (normalizedRevenue !== null) {
        properties.total_revenue = normalizedRevenue;
      }

      properties.years = parseIntArray(properties.years);
      properties.months = parseIntArray(properties.months);

      const yearlyCounts = parseJsonProperty(properties.yearly_counts);
      if (yearlyCounts) {
        properties.yearly_counts = yearlyCounts;
      }

      const monthlyCounts = parseJsonProperty(properties.monthly_counts);
      if (monthlyCounts) {
        properties.monthly_counts = monthlyCounts;
      }

      if (!properties.location) {
        const fallback = [
          properties.location_name,
          [properties.linear_name_full_1, properties.linear_name_full_2].filter(Boolean).join(' & '),
          properties.location_code,
        ].find((value) => typeof value === 'string' && value.trim().length > 0);
        if (fallback) {
          properties.location = fallback;
        }
      }
    } else if (properties.set_fine_amount !== undefined) {
      const normalizedSetFine = toNumeric(properties.set_fine_amount);
      if (normalizedSetFine !== null) {
        properties.set_fine_amount = normalizedSetFine;
      }
    }

    if (properties.count === undefined) {
      const fallbackCount = toNumeric(properties.ticket_count ?? properties.ticketCount);
      properties.count = fallbackCount !== null ? fallbackCount : 0;
    }

    if (properties.total_revenue === undefined) {
      const fallbackRevenue = toNumeric(
        properties.total_fine_amount ?? properties.totalFine ?? properties.set_fine_amount,
      );
      if (fallbackRevenue !== null) {
        properties.total_revenue = fallbackRevenue;
      }
    }

    if (properties.cluster === 1) {
      const clusterId = Number(properties.cluster_id);
      if (!Number.isFinite(clusterId)) {
        return;
      }

      const params = new URLSearchParams({ clusterId: String(clusterId) });
      fetch(`${MAP_CONFIG.API_PATHS.CLUSTER_EXPANSION}?${params.toString()}`)
        .then((response) => (response.ok ? response.json() : null))
        .then((payload) => {
          if (!payload || !Number.isFinite(payload.zoom)) {
            return;
          }
          const target = feature?.geometry?.coordinates || [event.lngLat.lng, event.lngLat.lat];
          map.easeTo({
            center: target,
            zoom: payload.zoom + 1,
            duration: 400,
          });
        })
        .catch((error) => {
          console.error('Failed to resolve cluster expansion', error);
        });
      return;
    }

    onPointClick?.(properties, event);
  }, [map, onPointClick, dataset]);

  useEffect(() => {
    if (!map || !visible) {
      return undefined;
    }

    const layerIds = [
      MAP_CONFIG.LAYER_IDS.TICKETS_CLUSTER,
      MAP_CONFIG.LAYER_IDS.TICKETS_CLUSTER_COUNT,
      MAP_CONFIG.LAYER_IDS.TICKETS_POINTS,
    ];

    const handleMouseEnter = () => {
      map.getCanvas().style.cursor = 'pointer';
    };
    const handleMouseLeave = () => {
      map.getCanvas().style.cursor = '';
    };

    for (const layerId of layerIds) {
      map.on('click', layerId, handleFeatureInteraction);
      map.on('mouseenter', layerId, handleMouseEnter);
      map.on('mouseleave', layerId, handleMouseLeave);
    }

    return () => {
      for (const layerId of layerIds) {
        map.off('click', layerId, handleFeatureInteraction);
        map.off('mouseenter', layerId, handleMouseEnter);
        map.off('mouseleave', layerId, handleMouseLeave);
      }
    };
  }, [map, visible, handleFeatureInteraction]);

  const tileUrl = useMemo(() => (
    MAP_CONFIG.TILE_SOURCE.TICKETS.replace('{dataset}', dataset)
  ), [dataset]);

  if (!visible) {
    return null;
  }

  const clusterLayer = {
    id: MAP_CONFIG.LAYER_IDS.TICKETS_CLUSTER,
    type: 'circle',
    source: MAP_CONFIG.SOURCE_IDS.TICKETS,
    'source-layer': dataset === 'parking_tickets' ? TILE_LAYER_NAME : dataset,
    minzoom: 0,
    maxzoom: MAP_CONFIG.ZOOM_THRESHOLDS.SHOW_INDIVIDUAL_TICKETS,
    paint: {
      'circle-color': STYLE_CONSTANTS.COLORS.TICKET_CLUSTER,
      'circle-radius': [
        'step',
        ['get', 'point_count'],
        14,
        50, 18,
        250, 24,
        1000, 30
      ],
      'circle-opacity': 0.7,
      'circle-stroke-color': '#133337',
      'circle-stroke-width': 1.2
    },
  };

  const clusterCountLayer = {
    id: MAP_CONFIG.LAYER_IDS.TICKETS_CLUSTER_COUNT,
    type: 'symbol',
    source: MAP_CONFIG.SOURCE_IDS.TICKETS,
    'source-layer': dataset === 'parking_tickets' ? TILE_LAYER_NAME : dataset,
    minzoom: 0,
    maxzoom: MAP_CONFIG.ZOOM_THRESHOLDS.SHOW_INDIVIDUAL_TICKETS,
    layout: {
      'text-field': '{point_count_abbreviated}',
      'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
      'text-size': 12
    },
    paint: {
      'text-color': '#ffffff'
    }
  };

  const pointLayer = {
    id: MAP_CONFIG.LAYER_IDS.TICKETS_POINTS,
    type: 'circle',
    source: MAP_CONFIG.SOURCE_IDS.TICKETS,
    'source-layer': dataset === 'parking_tickets' ? TILE_LAYER_NAME : dataset,
    minzoom: MAP_CONFIG.ZOOM_THRESHOLDS.SHOW_INDIVIDUAL_TICKETS - 0.01,
    paint: {
      'circle-color': STYLE_CONSTANTS.COLORS.TICKET_POINT,
      'circle-radius': [
        'interpolate',
        ['linear'],
        ['coalesce', ['get', 'count'], ['get', 'ticketCount'], ['get', 'ticket_count'], 1],
        1, 4,
        25, 6,
        75, 9,
        150, 12
      ],
      'circle-stroke-width': 1,
      'circle-stroke-color': '#fff',
      'circle-opacity': 0.85
    }
  };

  return (
    <Source
      key={dataset}
      id={MAP_CONFIG.SOURCE_IDS.TICKETS}
      type="vector"
      tiles={[tileUrl]}
      minzoom={0}
      maxzoom={18}
    >
      {dataset === 'parking_tickets' ? (
        <>
          <Layer {...clusterLayer} filter={clusterFilter} />
          <Layer {...clusterCountLayer} filter={clusterFilter} />
        </>
      ) : null}
      <Layer {...pointLayer} filter={pointFilter} />
    </Source>
  );
}
