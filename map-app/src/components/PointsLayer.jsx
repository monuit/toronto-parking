/**
 * PointsLayer - Ticket points with server-driven clustering
 * Single responsibility: render ticket data from vector tiles and handle interactions
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Source, Layer } from 'react-map-gl/maplibre';
import { MAP_CONFIG, STYLE_CONSTANTS } from '../lib/mapSources';
import { usePmtiles } from '../context/PmtilesContext.jsx';
import { recordTicketsPaint } from '../lib/clientMetrics.js';

const rawTilesBaseUrl = (import.meta.env?.VITE_TILES_BASE_URL || '').trim();
const tilesBaseUrl = rawTilesBaseUrl.replace(/\/+$/, '');

const prefetchedShardKeys = new Set();
const failedShardUrls = new Set();
const loggedShardFailures = new Set();
const inFlightSummaries = new Map();

function buildFilterExpression(targetKinds, filter, { fallbackToRawPoints = false } = {}) {
  const expression = ['all'];
  const kinds = Array.isArray(targetKinds) ? targetKinds : [targetKinds];
  const predicate = ['any', ['in', ['get', 'kind'], ['literal', kinds]]];
  if (fallbackToRawPoints) {
    predicate.push(['all', ['!', ['has', 'kind']], ['literal', true]]);
  }
  expression.push(predicate);

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

function isCoordinateWithinBounds(longitude, latitude, bounds) {
  if (!Array.isArray(bounds) || bounds.length !== 4) {
    return false;
  }
  const [west, south, east, north] = bounds;
  return longitude >= west && longitude <= east && latitude >= south && latitude <= north;
}

function matchesZoom(shard, zoom) {
  if (!Number.isFinite(zoom)) {
    return true;
  }
  const min = Number.isFinite(shard?.minZoom) ? shard.minZoom : Number.NEGATIVE_INFINITY;
  const max = Number.isFinite(shard?.maxZoom) ? shard.maxZoom : Number.POSITIVE_INFINITY;
  return zoom >= min && zoom <= max + 0.0001;
}

function selectShardForView(datasetManifest, longitude, latitude, zoom) {
  if (!datasetManifest || !Array.isArray(datasetManifest.shards) || datasetManifest.shards.length === 0) {
    return null;
  }
  const ordered = [...datasetManifest.shards].sort((a, b) => (Number(a.minZoom || 0) - Number(b.minZoom || 0)));
  const zoomMatches = Number.isFinite(zoom)
    ? ordered.filter((entry) => matchesZoom(entry, zoom))
    : ordered;
  const candidates = zoomMatches.length > 0 ? zoomMatches : ordered;
  const locationMatch = candidates.find((shard) => isCoordinateWithinBounds(longitude, latitude, shard.bounds));
  if (locationMatch) {
    return locationMatch;
  }
  if (zoomMatches.length > 0) {
    return zoomMatches[0];
  }
  return ordered[0];
}

function fetchViewportSummary(key, url) {
  const cacheKey = key;
  if (inFlightSummaries.has(cacheKey)) {
    return inFlightSummaries.get(cacheKey);
  }
  const promise = fetch(url)
    .then((response) => {
      if (!response.ok) {
        throw new Error(`Summary request failed with status ${response.status}`);
      }
      return response.json();
    })
    .finally(() => {
      inFlightSummaries.delete(cacheKey);
    });
  inFlightSummaries.set(cacheKey, promise);
  return promise;
}

function resolvePmtilesUrl(shard) {
  if (!shard) {
    return null;
  }
  const candidates = [shard.originUrl, shard.url];
  for (const candidate of candidates) {
    if (typeof candidate !== 'string' || candidate.length === 0) {
      continue;
    }
    try {
      const base = typeof window !== 'undefined' ? window.location?.origin : undefined;
      const absolute = new URL(candidate, base).toString();
      return absolute;
    } catch {
      // Ignore malformed URLs and continue to next candidate
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
  onDataStatusChange,
}) {
  const summaryTimeoutRef = useRef(null);
  const lastSummaryKeyRef = useRef(null);
  const inFlightSummaryKeyRef = useRef(null);
  const summaryRequestIdRef = useRef(0);
  const summaryRetryRef = useRef(0);
  const { manifest: pmtilesManifest, ready: pmtilesReady } = usePmtiles();
  const [pmtilesSource, setPmtilesSource] = useState(null);
  const vectorSourceKey = pmtilesSource?.shardId ? `${dataset}-${pmtilesSource.shardId}` : dataset;
  const datasetManifest = useMemo(
    () => (pmtilesReady ? pmtilesManifest?.datasets?.[dataset] : null),
    [dataset, pmtilesManifest, pmtilesReady],
  );
  const paintRecordedRef = useRef(false);
  const dataStatusRef = useRef(dataset === 'parking_tickets' ? 'loading' : 'ready');

  useEffect(() => {
    inFlightSummaryKeyRef.current = null;
    lastSummaryKeyRef.current = null;
    paintRecordedRef.current = false;
    dataStatusRef.current = 'loading';
    summaryRetryRef.current = 0;
    if (typeof onDataStatusChange === 'function') {
      onDataStatusChange('loading');
    }
  }, [dataset, filter?.year, filter?.month, onDataStatusChange]);

  useEffect(() => {
    if (!map || !pmtilesReady || !datasetManifest || !pmtilesSource?.rawUrl) {
      return undefined;
    }
    if (!Array.isArray(datasetManifest.shards) || datasetManifest.shards.length <= 1) {
      return undefined;
    }

    const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
    let idleId = null;
    let cancelled = false;

    const runPrefetch = async () => {
      for (const shard of datasetManifest.shards) {
        const resolved = resolvePmtilesUrl(shard);
        if (!resolved || resolved === pmtilesSource.rawUrl) {
          continue;
        }
        const key = `${dataset}:${resolved}`;
        if (prefetchedShardKeys.has(key)) {
          continue;
        }
        prefetchedShardKeys.add(key);
        try {
          const response = await fetch(resolved, {
            method: 'GET',
            headers: { Range: 'bytes=0-511' },
            cache: 'force-cache',
            signal: controller?.signal,
            mode: 'cors',
          });
          if (!response.ok) {
            failedShardUrls.add(resolved);
            prefetchedShardKeys.delete(key);
            if (!cancelled) {
              setPmtilesSource((previous) => {
                if (previous?.rawUrl === resolved) {
                  return null;
                }
                return previous;
              });
              if (!loggedShardFailures.has(resolved)) {
                console.warn(`PMTiles shard prefetch failed (${response.status}) for ${resolved}; falling back to legacy tiles.`);
                loggedShardFailures.add(resolved);
              }
            }
            continue;
          }
          failedShardUrls.delete(resolved);
          loggedShardFailures.delete(resolved);
        } catch (error) {
          if (error?.name === 'AbortError') {
            return;
          }
          prefetchedShardKeys.delete(key);
          failedShardUrls.add(resolved);
          if (!cancelled) {
            setPmtilesSource((previous) => {
              if (previous?.rawUrl === resolved) {
                return null;
              }
              return previous;
            });
            if (!loggedShardFailures.has(resolved)) {
              console.warn(`PMTiles shard prefetch encountered error for ${resolved}:`, error);
              loggedShardFailures.add(resolved);
            }
          }
        }
      }
    };

    const schedule = () => {
      if (typeof window !== 'undefined' && typeof window.requestIdleCallback === 'function') {
        idleId = window.requestIdleCallback(runPrefetch, { timeout: 2000 });
      } else {
        idleId = setTimeout(runPrefetch, 600);
      }
    };

    schedule();

    return () => {
      cancelled = true;
      if (controller) {
        controller.abort();
      }
      if (typeof window !== 'undefined' && typeof window.cancelIdleCallback === 'function' && idleId !== null) {
        window.cancelIdleCallback(idleId);
      } else if (idleId !== null) {
        clearTimeout(idleId);
      }
    };
  }, [dataset, datasetManifest, map, pmtilesReady, pmtilesSource]);
  useEffect(() => {
    if (!map || !pmtilesReady || !datasetManifest) {
      setPmtilesSource(null);
      return undefined;
    }
    if (!Array.isArray(datasetManifest.shards) || datasetManifest.shards.length === 0) {
      setPmtilesSource(null);
      return undefined;
    }

    const updateShard = () => {
      const center = map.getCenter();
      if (!center) {
        return;
      }
      const zoom = map.getZoom();
      const shard = selectShardForView(datasetManifest, center.lng, center.lat, zoom);
      if (!shard) {
        setPmtilesSource(null);
        return;
      }
      const resolvedUrl = resolvePmtilesUrl(shard);
      if (!resolvedUrl) {
        setPmtilesSource(null);
        return;
      }
      if (failedShardUrls.has(resolvedUrl)) {
        if (!loggedShardFailures.has(resolvedUrl)) {
          console.warn(`PMTiles shard ${resolvedUrl} marked unavailable; using legacy tiles.`);
          loggedShardFailures.add(resolvedUrl);
        }
        setPmtilesSource((previous) => (previous?.rawUrl === resolvedUrl ? null : previous));
        return;
      }
      const nextUrl = `pmtiles://${resolvedUrl}`;
      setPmtilesSource((previous) => {
        if (previous && previous.url === nextUrl) {
          return previous;
        }
        return {
          url: nextUrl,
          shardId: shard.id || shard.filename || nextUrl,
          minZoom: Number.isFinite(shard.minZoom) ? shard.minZoom : undefined,
          maxZoom: Number.isFinite(shard.maxZoom) ? shard.maxZoom : undefined,
          rawUrl: resolvedUrl,
        };
      });
    };

    let rafToken = null;
    const scheduleUpdate = () => {
      if (rafToken !== null) {
        return;
      }
      rafToken = typeof window !== 'undefined'
        ? window.requestAnimationFrame(() => {
          rafToken = null;
          updateShard();
        })
        : null;
      if (rafToken === null) {
        updateShard();
      }
    };

    updateShard();
    const handleMove = () => scheduleUpdate();
    map.on('moveend', handleMove);
    map.on('zoomend', handleMove);

    return () => {
      if (rafToken !== null && typeof window !== 'undefined') {
        window.cancelAnimationFrame(rafToken);
        rafToken = null;
      }
      map.off('moveend', handleMove);
      map.off('zoomend', handleMove);
    };
  }, [map, datasetManifest, pmtilesReady]);

  const pointFilter = useMemo(
    () => buildFilterExpression(['sample', 'point', 'cluster'], filter, { fallbackToRawPoints: true }),
    [filter],
  );

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
    summaryRetryRef.current = 0;
    inFlightSummaryKeyRef.current = key;

    const url = `${MAP_CONFIG.API_PATHS.SUMMARY}?${params.toString()}`;

    fetchViewportSummary(key, url)
      .then((payload) => {
        if (summaryRequestIdRef.current !== requestId) {
          return;
        }
        lastSummaryKeyRef.current = key;
        onViewportSummaryChange?.(payload);
      })
      .catch((error) => {
        if (summaryRequestIdRef.current !== requestId) {
          return;
        }
        if (error?.name === 'AbortError') {
          lastSummaryKeyRef.current = null;
          return;
        }
        console.warn('Failed to load viewport summary', error);
        lastSummaryKeyRef.current = null;
        const attempt = summaryRetryRef.current + 1;
        summaryRetryRef.current = attempt;
        if (attempt <= 3) {
          const backoff = Math.min(1500, 350 * attempt);
          if (summaryTimeoutRef.current) {
            clearTimeout(summaryTimeoutRef.current);
          }
          summaryTimeoutRef.current = setTimeout(() => {
            if (summaryRequestIdRef.current !== requestId) {
              return;
            }
            inFlightSummaryKeyRef.current = null;
            scheduleSummaryFetch();
          }, backoff);
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

    if (typeof properties.kind !== 'string' || properties.kind.length === 0) {
      properties.kind = properties.cluster === 1 ? 'cluster' : 'point';
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

    if (properties.kind === 'cluster') {
      const target = feature?.geometry?.coordinates || [event.lngLat.lng, event.lngLat.lat];
      const currentZoom = Number.isFinite(map?.getZoom?.()) ? map.getZoom() : RAW_POINT_ZOOM;
      const zoomIncrement = currentZoom < RAW_POINT_ZOOM ? 1 : 0.5;
      const nextZoom = Math.min(currentZoom + zoomIncrement, RAW_POINT_ZOOM + 0.01);
      map.easeTo({
        center: target,
        zoom: nextZoom,
        duration: 400,
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
    if (pmtilesSource?.url) {
      return pmtilesSource.url;
    }

    const normaliseBase = (value) => value.replace(/\/+$/u, '');
    const template = tilesBaseUrl
      ? `${normaliseBase(tilesBaseUrl)}/{z}/{x}/{y}.pbf?dataset=${dataset}`
      : MAP_CONFIG.TILE_SOURCE.TICKETS.replace('{dataset}', dataset);

    if (/^https?:\/\//i.test(template)) {
      return template;
    }

    if (typeof window !== 'undefined' && window.location?.origin) {
      return `${window.location.origin}${template}`;
    }

    return template;
  }, [dataset, pmtilesSource]);

  const isParkingDataset = dataset === 'parking_tickets';
  const vectorLayerName = useMemo(() => {
    const fallback = dataset === 'parking_tickets' ? TILE_LAYER_NAME : dataset;
    return pmtilesSource?.vectorLayer || datasetManifest?.vectorLayer || fallback;
  }, [dataset, datasetManifest, pmtilesSource]);

  const vectorLayerDefinitions = useMemo(
    () => ([{ id: vectorLayerName }]),
    [vectorLayerName],
  );

  const vectorSourceMetadata = useMemo(() => ({
    'mapbox:vector_layers': vectorLayerDefinitions,
  }), [vectorLayerDefinitions]);

  const datasetStyle = useMemo(() => {
    if (dataset === 'red_light_locations') {
      const clusterSizeExpression = [
        'max',
        ['to-number', ['coalesce', ['get', 'cluster_size'], ['get', 'clusterSize'], 1], 1],
        1,
      ];
      const clusterScale = [
        'min',
        1.85,
        [
          '+',
          1,
          [
            '*',
            0.2,
            [
              'max',
              0,
              [
                '-',
                ['sqrt', clusterSizeExpression],
                1,
              ],
            ],
          ],
        ],
      ];
      const buildRadiusStop = (baseValue) => [
        'case',
        ['==', ['get', 'kind'], 'cluster'],
        ['*', baseValue, clusterScale],
        baseValue,
      ];
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
          7.5, buildRadiusStop(6.5),
          10, buildRadiusStop(8.5),
          13, buildRadiusStop(11.5),
          16, buildRadiusStop(14.5),
        ],
      };
    }
    if (dataset === 'ase_locations') {
      const clusterSizeExpression = [
        'max',
        ['to-number', ['coalesce', ['get', 'cluster_size'], ['get', 'clusterSize'], 1], 1],
        1,
      ];
      const clusterScale = [
        'min',
        1.8,
        [
          '+',
          1,
          [
            '*',
            0.18,
            [
              'max',
              0,
              [
                '-',
                ['sqrt', clusterSizeExpression],
                1,
              ],
            ],
          ],
        ],
      ];
      const buildRadiusStop = (baseValue) => [
        'case',
        ['==', ['get', 'kind'], 'cluster'],
        ['*', baseValue, clusterScale],
        baseValue,
      ];
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
          7.5, buildRadiusStop(6),
          10, buildRadiusStop(8),
          13, buildRadiusStop(11),
          16, buildRadiusStop(14),
        ],
      };
    }
    const baseCountExpression = ['to-number', ['coalesce', ['get', 'count'], ['get', 'ticketCount'], ['get', 'ticket_count'], 0], 0];
    return {
      pointColor: STYLE_CONSTANTS.COLORS.TICKET_POINT,
      strokeColor: '#fff',
      strokeWidth: 1,
      opacity: 0.85,
      minZoom: MAP_CONFIG.TILE_MIN_ZOOM,
      radiusExpression: [
        'interpolate',
        ['linear'],
        baseCountExpression,
        1, 4,
        25, 6,
        75, 9,
        150, 12
      ],
    };
  }, [dataset]);
  const pointLayer = useMemo(() => {
    const layer = {
      id: MAP_CONFIG.LAYER_IDS.TICKETS_POINTS,
      type: 'circle',
      minzoom: isParkingDataset
        ? Math.max(datasetStyle.minZoom, RAW_POINT_ZOOM)
        : datasetStyle.minZoom,
      maxzoom: 18,
      filter: pointFilter,
      paint: {
        'circle-color': datasetStyle.pointColor,
        'circle-radius': datasetStyle.radiusExpression,
        'circle-stroke-width': datasetStyle.strokeWidth,
        'circle-stroke-color': datasetStyle.strokeColor,
        'circle-opacity': datasetStyle.opacity,
      },
    };
    layer['source-layer'] = vectorLayerName;
    return layer;
  }, [datasetStyle, pointFilter, isParkingDataset, vectorLayerName]);

  const clusterLayer = null;
  const clusterCountLayer = null;

  const vectorSourceProps = useMemo(() => (
    pmtilesSource?.url
      ? {
          url: pmtilesSource.url,
          vector_layers: vectorLayerDefinitions,
        }
      : {
          tiles: [tileUrl],
          scheme: 'xyz',
          vector_layers: vectorLayerDefinitions,
        }
  ), [pmtilesSource?.url, tileUrl, vectorLayerDefinitions]);
  const vectorMinZoom = pmtilesSource?.minZoom ?? datasetStyle.minZoom;
  const vectorMaxZoom = pmtilesSource?.maxZoom ?? 18;

  const notifyDataStatus = useCallback((status) => {
    if (dataStatusRef.current === status) {
      return;
    }
    dataStatusRef.current = status;
    if (typeof onDataStatusChange === 'function') {
      onDataStatusChange(status);
    }
  }, [onDataStatusChange]);

  const scheduleDeferredUpdate = useCallback((fn) => {
    if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function') {
      window.requestAnimationFrame(() => {
        fn();
      });
      return;
    }
    setTimeout(fn, 0);
  }, []);

  useEffect(() => {
    if (isParkingDataset) {
      scheduleDeferredUpdate(() => {
        notifyDataStatus('loading');
      });
    }
  }, [isParkingDataset, notifyDataStatus, scheduleDeferredUpdate, vectorSourceKey, tileUrl]);

  useEffect(() => {
    if (!map) {
      return undefined;
    }

    let cancelled = false;

    const evaluateSourceStatus = () => {
      if (cancelled) {
        return;
      }
      try {
        const source = map.getSource(MAP_CONFIG.SOURCE_IDS.TICKETS);
        if (source && typeof source.loaded === 'function' && source.loaded()) {
          scheduleDeferredUpdate(() => {
            notifyDataStatus('ready');
          });
        }
      } catch {
        /* ignore source lookup failures */
      }
    };

    const handleSourceData = (event) => {
      if (event?.sourceId !== MAP_CONFIG.SOURCE_IDS.TICKETS) {
        return;
      }
      if (event.isSourceLoaded) {
        scheduleDeferredUpdate(() => {
          evaluateSourceStatus();
        });
      }
    };

    const handleDataLoading = (event) => {
      if (event?.sourceId !== MAP_CONFIG.SOURCE_IDS.TICKETS) {
        return;
      }
      scheduleDeferredUpdate(() => {
        notifyDataStatus('loading');
      });
    };

    map.on('sourcedata', handleSourceData);
    map.on('dataloading', handleDataLoading);
    evaluateSourceStatus();

    return () => {
      cancelled = true;
      map.off('sourcedata', handleSourceData);
      map.off('dataloading', handleDataLoading);
    };
  }, [map, notifyDataStatus, scheduleDeferredUpdate]);

  useEffect(() => {
    if (!visible || paintRecordedRef.current) {
      return;
    }
    paintRecordedRef.current = true;
    recordTicketsPaint();
  }, [visible, vectorSourceKey]);

  useEffect(() => {
    if (typeof document === 'undefined' || typeof window === 'undefined') {
      return undefined;
    }

    const createdLinks = [];
    const seenOrigins = new Set();

    const appendLink = (rel, href, attributes = {}) => {
      if (!href) {
        return;
      }
      const link = document.createElement('link');
      link.rel = rel;
      link.href = href;
      Object.entries(attributes).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== '') {
          link.setAttribute(key, value);
        }
      });
      document.head.appendChild(link);
      createdLinks.push(link);
    };

    const addOriginHints = (candidate) => {
      if (!candidate) {
        return;
      }
      try {
        const url = new URL(candidate, window.location.origin);
        if (seenOrigins.has(url.origin)) {
          return;
        }
        seenOrigins.add(url.origin);
        appendLink('preconnect', url.origin, { crossorigin: 'anonymous' });
        appendLink('dns-prefetch', url.origin);
      } catch {
        /* ignore invalid URLs */
      }
    };

    addOriginHints(tileUrl);
    addOriginHints(MAP_CONFIG.STYLE_URL);

    if (pmtilesSource?.url?.startsWith('pmtiles://')) {
      const normalized = pmtilesSource.url.replace('pmtiles://', '');
      addOriginHints(normalized);
    }

    try {
      const styleUrl = new URL(MAP_CONFIG.STYLE_URL, window.location.origin).toString();
      appendLink('prefetch', styleUrl, { as: 'fetch', crossorigin: 'anonymous' });
    } catch {
      /* ignore */
    }

    return () => {
      createdLinks.forEach((link) => {
        if (link.parentNode) {
          link.parentNode.removeChild(link);
        }
      });
    };
  }, [tileUrl, pmtilesSource]);
  if (!visible) {
    return null;
  }

  const shouldRenderParkingLayers = isParkingDataset;

  return (
    <Source
      key={vectorSourceKey}
      id={MAP_CONFIG.SOURCE_IDS.TICKETS}
      type="vector"
      {...vectorSourceProps}
      minzoom={vectorMinZoom}
      maxzoom={vectorMaxZoom}
      metadata={vectorSourceMetadata}
      promoteId={isParkingDataset ? 'centreline_id' : undefined}
    >
      {shouldRenderParkingLayers && clusterLayer ? <Layer {...clusterLayer} /> : null}
      {shouldRenderParkingLayers && clusterCountLayer ? <Layer {...clusterCountLayer} /> : null}
      <Layer {...pointLayer} />
    </Source>
  );
}
