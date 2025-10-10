/**
 * PointsLayer - Ticket points with server-driven clustering
 * Single responsibility: render ticket data from vector tiles and handle interactions
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Source, Layer } from 'react-map-gl/maplibre';
import { MAP_CONFIG, STYLE_CONSTANTS } from '../lib/mapSources';
import { loadCameraDataset } from '../lib/cameraDatasetLoader.js';

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
const RAW_POINT_ZOOM = MAP_CONFIG.ZOOM_THRESHOLDS.SHOW_INDIVIDUAL_TICKETS;

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
  isTouchDevice = false,
}) {
  const summaryTimeoutRef = useRef(null);
  const lastSummaryKeyRef = useRef(null);
  const inFlightSummaryKeyRef = useRef(null);
  const summaryRequestIdRef = useRef(0);
  const [geojsonData, setGeojsonData] = useState(null);

  useEffect(() => {
    inFlightSummaryKeyRef.current = null;
    lastSummaryKeyRef.current = null;
  }, [dataset, filter?.year, filter?.month]);

  const pointFilter = useMemo(() => buildFilterExpression(false, filter), [filter]);

  const scheduleSummaryFetch = useCallback(() => {
    if (!map || !onViewportSummaryChange) {
      return;
    }

    if (dataset !== 'parking_tickets') {
      summaryRequestIdRef.current += 1;
      inFlightSummaryKeyRef.current = null;
      lastSummaryKeyRef.current = null;
      onViewportSummaryChange({ zoomRestricted: true, topStreets: [] });
      return;
    }

    const zoom = map.getZoom();
    if (zoom < SUMMARY_ZOOM_THRESHOLD) {
      summaryRequestIdRef.current += 1;
      inFlightSummaryKeyRef.current = null;
      lastSummaryKeyRef.current = null;
      onViewportSummaryChange({ zoomRestricted: true, topStreets: [] });
      return;
    }

    const bounds = map.getBounds();
    const precision = zoom >= 13 ? 4 : 3;
    const roundToPrecision = (value) => Number.parseFloat(value).toFixed(precision);
    const key = [
      roundToPrecision(bounds.getWest()),
      roundToPrecision(bounds.getSouth()),
      roundToPrecision(bounds.getEast()),
      roundToPrecision(bounds.getNorth()),
      zoom.toFixed(2),
      filter?.year ?? 'all',
      filter?.month ?? 'all',
    ].join('|');

    if (lastSummaryKeyRef.current === key || inFlightSummaryKeyRef.current === key) {
      return;
    }

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

    const requestId = summaryRequestIdRef.current + 1;
    summaryRequestIdRef.current = requestId;
    inFlightSummaryKeyRef.current = key;

    fetch(`${MAP_CONFIG.API_PATHS.SUMMARY}?${params.toString()}`)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Summary request failed with status ${response.status}`);
        }
        return response.json();
      })
      .then((payload) => {
        if (summaryRequestIdRef.current !== requestId) {
          return;
        }
        lastSummaryKeyRef.current = key;
        onViewportSummaryChange?.(payload);
      })
      .catch((error) => {
        if (summaryRequestIdRef.current === requestId) {
          console.error('Failed to load viewport summary', error);
          lastSummaryKeyRef.current = null;
        }
      })
      .finally(() => {
        if (summaryRequestIdRef.current === requestId) {
          if (inFlightSummaryKeyRef.current === key) {
            inFlightSummaryKeyRef.current = null;
          }
        }
      });
  }, [filter, map, onViewportSummaryChange, dataset]);

  useEffect(() => {
    if (!map || !visible || !onViewportSummaryChange) {
      return undefined;
    }

    if (dataset !== 'parking_tickets') {
      onViewportSummaryChange({ zoomRestricted: true, topStreets: [] });
      return undefined;
    }

    const handleMove = () => {
      if (summaryTimeoutRef.current) {
        clearTimeout(summaryTimeoutRef.current);
      }
      const delay = isTouchDevice ? 260 : 140;
      summaryTimeoutRef.current = setTimeout(scheduleSummaryFetch, delay);
    };

    scheduleSummaryFetch();
    map.on('moveend', handleMove);
    map.on('zoomend', handleMove);

    return () => {
      inFlightSummaryKeyRef.current = null;
      if (summaryTimeoutRef.current) {
        clearTimeout(summaryTimeoutRef.current);
      }
      map.off('moveend', handleMove);
      map.off('zoomend', handleMove);
    };
  }, [map, visible, scheduleSummaryFetch, onViewportSummaryChange, dataset, isTouchDevice]);

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

      const monthlyCounts = parseJsonProperty(properties.monthly_counts ?? properties.monthlyCounts);
      let yearlyCounts = parseJsonProperty(properties.yearly_counts ?? properties.yearlyCounts);

      if (!yearlyCounts && monthlyCounts && typeof monthlyCounts === 'object') {
        yearlyCounts = Object.entries(monthlyCounts).reduce((acc, [key, value]) => {
          if (typeof key !== 'string') {
            return acc;
          }
          const yearKey = key.slice(0, 4);
          const numericValue = Number(value);
          if (!Number.isFinite(numericValue) || yearKey.length !== 4) {
            return acc;
          }
          acc[yearKey] = (acc[yearKey] || 0) + numericValue;
          return acc;
        }, {});
      }

      if (yearlyCounts) {
        properties.yearly_counts = yearlyCounts;
        properties.yearlyCounts = yearlyCounts;
      }

      if (monthlyCounts) {
        delete properties.monthly_counts;
        delete properties.monthlyCounts;
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
    } else {
      if (properties.set_fine_amount !== undefined) {
        const normalizedSetFine = toNumeric(properties.set_fine_amount);
        if (normalizedSetFine !== null) {
          properties.set_fine_amount = normalizedSetFine;
        }
      }

      const yearCounts = parseJsonProperty(properties.year_counts);
      if (yearCounts) {
        properties.year_counts = yearCounts;
        const yearKey = filter?.year ?? null;
        if (yearKey !== null) {
          const entry = yearCounts[yearKey] || yearCounts[String(yearKey)];
          if (entry && typeof entry === 'object') {
            const entryCount = toNumeric(entry.ticketCount ?? entry.count);
            if (entryCount !== null) {
              properties.count = entryCount;
              properties.ticketCount = entryCount;
            } else {
              properties.count = 0;
              properties.ticketCount = 0;
            }
            const entryRevenue = toNumeric(entry.totalRevenue ?? entry.total_revenue);
            if (entryRevenue !== null) {
              properties.total_revenue = entryRevenue;
            }
          } else {
            properties.count = 0;
            properties.ticketCount = 0;
            properties.total_revenue = 0;
          }
        }
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

    if (dataset !== 'parking_tickets') {
      const rawId = properties.location_id
        ?? properties.locationId
        ?? properties.location_code
        ?? properties.locationCode
        ?? properties.intersection_id
        ?? properties.intersectionId
        ?? null;
      if (rawId !== null && rawId !== undefined) {
        properties.locationId = String(rawId);
      }
      if (typeof properties.location !== 'string' || properties.location.trim().length === 0) {
        const fallbackLocation = properties.name
          ?? properties.location_name
          ?? [properties.streetA, properties.streetB].filter(Boolean).join(' & ');
        if (fallbackLocation) {
          properties.location = fallbackLocation;
        }
      }
      if (!Number.isFinite(properties.longitude) || !Number.isFinite(properties.latitude)) {
        const coords = feature?.geometry?.coordinates;
        if (Array.isArray(coords) && coords.length >= 2) {
          properties.longitude = Number(coords[0]);
          properties.latitude = Number(coords[1]);
        } else if (event?.lngLat) {
          properties.longitude = event.lngLat.lng;
          properties.latitude = event.lngLat.lat;
        }
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
  }, [map, onPointClick, dataset, filter]);

  useEffect(() => {
    if (!map || !visible) {
      return undefined;
    }

    const layerIds = dataset === 'parking_tickets'
      ? [
        MAP_CONFIG.LAYER_IDS.TICKETS_CLUSTER,
        MAP_CONFIG.LAYER_IDS.TICKETS_CLUSTER_COUNT,
        MAP_CONFIG.LAYER_IDS.TICKETS_POINTS,
      ]
      : [MAP_CONFIG.LAYER_IDS.TICKETS_POINTS];

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
  }, [map, visible, handleFeatureInteraction, dataset]);

  const tileUrl = useMemo(() => {
    const template = MAP_CONFIG.TILE_SOURCE.TICKETS.replace('{dataset}', dataset);
    if (/^https?:\/\//i.test(template)) {
      return template;
    }
    if (typeof window !== 'undefined' && window.location?.origin) {
      return `${window.location.origin}${template}`;
    }
    return template;
  }, [dataset]);

  const isParkingDataset = dataset === 'parking_tickets';
  useEffect(() => {
    if (isParkingDataset) {
      setGeojsonData(null);
      return undefined;
    }

    let cancelled = false;
    setGeojsonData(null);

    loadCameraDataset(dataset)
      .then((data) => {
        if (!cancelled && data) {
          setGeojsonData(data);
        }
      })
      .catch((error) => {
        if (!cancelled) {
          console.error(`Failed to load ${dataset} camera geojson`, error);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [dataset, isParkingDataset]);

  const datasetStyle = useMemo(() => {
    if (dataset === 'red_light_locations') {
      return {
        pointColor: STYLE_CONSTANTS.COLORS.RED_LIGHT_POINT,
        strokeColor: STYLE_CONSTANTS.COLORS.RED_LIGHT_STROKE,
        strokeWidth: 1.6,
        opacity: 0.95,
        minZoom: 7.5,
        radiusExpression: [
          'interpolate',
          ['linear'],
          ['zoom'],
          7.5, 6.5,
          10, 8.5,
          13, 11.5,
          16, 14.5
        ],
      };
    }
    if (dataset === 'ase_locations') {
      return {
        pointColor: STYLE_CONSTANTS.COLORS.ASE_POINT,
        strokeColor: STYLE_CONSTANTS.COLORS.ASE_STROKE,
        strokeWidth: 1.6,
        opacity: 0.96,
        minZoom: 7.5,
        radiusExpression: [
          'interpolate',
          ['linear'],
          ['zoom'],
          7.5, 6,
          10, 8,
          13, 11,
          16, 14
        ],
      };
    }
    return {
      pointColor: STYLE_CONSTANTS.COLORS.TICKET_POINT,
      strokeColor: '#fff',
      strokeWidth: 1,
      opacity: 0.85,
      minZoom: MAP_CONFIG.TILE_MIN_ZOOM,
      radiusExpression: [
        'interpolate',
        ['linear'],
        ['coalesce', ['get', 'count'], ['get', 'ticketCount'], ['get', 'ticket_count'], 1],
        1, 4,
        25, 6,
        75, 9,
        150, 12
      ],
    };
  }, [dataset]);
  const pointLayer = {
    id: MAP_CONFIG.LAYER_IDS.TICKETS_POINTS,
    type: 'circle',
    ...(isParkingDataset ? { 'source-layer': TILE_LAYER_NAME } : {}),
    minzoom: datasetStyle.minZoom,
    paint: {
      'circle-color': datasetStyle.pointColor,
      'circle-radius': datasetStyle.radiusExpression,
      'circle-stroke-width': datasetStyle.strokeWidth,
      'circle-stroke-color': datasetStyle.strokeColor,
      'circle-opacity': datasetStyle.opacity
    }
  };

  if (!isParkingDataset && !geojsonData) {
    return null;
  }

  if (!visible) {
    return null;
  }

  return isParkingDataset ? (
    <Source
      key={dataset}
      id={MAP_CONFIG.SOURCE_IDS.TICKETS}
      type="vector"
      tiles={[tileUrl]}
      minzoom={datasetStyle.minZoom}
      maxzoom={18}
    >
      <Layer {...pointLayer} filter={pointFilter} />
    </Source>
  ) : (
    <Source
      key={dataset}
      id={MAP_CONFIG.SOURCE_IDS.TICKETS}
      type="geojson"
      data={geojsonData}
      generateId
    >
      <Layer {...pointLayer} filter={pointFilter} />
    </Source>
  );
}
