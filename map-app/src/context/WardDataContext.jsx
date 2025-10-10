/* eslint-disable react-refresh/only-export-components */
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { MAP_CONFIG } from '../lib/mapSources.js';
import {
  loadCachedWardSummary,
  saveWardSummary,
  loadCachedWardGeojson,
  saveWardGeojson,
} from '../lib/wardStorage.js';

const WardDataContext = createContext(null);

function buildEtag(dataset, response, fallbackVersion) {
  const headerEtag = response?.headers?.get?.('etag');
  if (headerEtag) {
    return headerEtag;
  }
  if (Number.isFinite(fallbackVersion)) {
    return `W/"${dataset}:${fallbackVersion}"`;
  }
  return null;
}

export function WardDataProvider({ children }) {
  const [datasets, setDatasets] = useState(() => ({}));
  const pendingRef = useRef(new Map());
  const geojsonPendingRef = useRef(new Map());
  const bootstrappedRef = useRef(false);
  const SUPPORTED = useMemo(() => new Set(['red_light_locations', 'ase_locations', 'cameras_combined']), []);

  useEffect(() => {
    if (bootstrappedRef.current || typeof window === 'undefined') {
      return undefined;
    }
    bootstrappedRef.current = true;
    let cancelled = false;
    (async () => {
      for (const dataset of SUPPORTED) {
        const summaryEntry = await loadCachedWardSummary(dataset);
        if (cancelled) {
          return;
        }
        if (summaryEntry?.data) {
          setDatasets((previous) => ({
            ...previous,
            [dataset]: {
              ...(previous[dataset] || {}),
              summary: summaryEntry.data,
              etag: summaryEntry.etag || previous[dataset]?.etag || null,
              version: summaryEntry.version || previous[dataset]?.version || null,
            },
          }));
        }
        const geojsonEntry = await loadCachedWardGeojson(dataset);
        if (cancelled) {
          return;
        }
        if (geojsonEntry?.data) {
          setDatasets((previous) => ({
            ...previous,
            [dataset]: {
              ...(previous[dataset] || {}),
              geojson: geojsonEntry.data,
              geojsonEtag: geojsonEntry.etag || previous[dataset]?.geojsonEtag || null,
            },
          }));
        }
      }
    })().catch(() => {
      /* non-blocking */
    });
    return () => {
      cancelled = true;
    };
  }, [SUPPORTED]);

  const ensureDataset = useCallback((dataset) => {
    if (!dataset || !SUPPORTED.has(dataset)) {
      return Promise.resolve(null);
    }

    const existing = datasets[dataset];
    if (existing && existing.summary && !existing.error) {
      return Promise.resolve(existing);
    }

    if (pendingRef.current.has(dataset)) {
      return pendingRef.current.get(dataset);
    }

    const controller = new AbortController();
    const params = new URLSearchParams({ dataset });
    const existingEtag = existing?.etag;
    const headers = {
      Accept: 'application/json',
    };
    if (existingEtag) {
      headers['If-None-Match'] = existingEtag;
    }

    const request = fetch(`${MAP_CONFIG.API_PATHS.WARD_SUMMARY}?${params.toString()}`, {
      signal: controller.signal,
      headers,
    })
      .then(async (response) => {
        if (response.status === 304) {
          return { summary: null, etag: response.headers.get('etag') };
        }
        if (!response.ok) {
          throw new Error(`Ward summary request failed with status ${response.status}`);
        }
        const payload = await response.json();
        const generatedAt = Date.parse(payload?.generatedAt || '') || Date.now();
        const etag = buildEtag(dataset, response, generatedAt);
        return { summary: payload, etag, version: generatedAt };
      })
      .then((result) => {
        setDatasets((previous) => ({
          ...previous,
          [dataset]: {
            ...(previous[dataset] || {}),
            summary: result.summary ?? previous[dataset]?.summary ?? null,
            etag: result.etag ?? previous[dataset]?.etag ?? null,
            version: result.version ?? previous[dataset]?.version ?? null,
            loading: false,
            error: null,
          },
        }));
        if (result.summary) {
          saveWardSummary(dataset, {
            data: result.summary,
            etag: result.etag ?? null,
            version: result.version ?? null,
            generatedAt: result.summary.generatedAt ?? null,
          }).catch(() => {
            /* non-blocking */
          });
        }
        return result;
      })
      .catch((error) => {
        console.error('Failed to load ward summary', error);
        setDatasets((previous) => ({
          ...previous,
          [dataset]: {
            ...(previous[dataset] || {}),
            loading: false,
            error: error.message,
          },
        }));
        return null;
      })
      .finally(() => {
        pendingRef.current.delete(dataset);
      });

    pendingRef.current.set(dataset, request);
    setDatasets((previous) => ({
      ...previous,
      [dataset]: {
        ...(previous[dataset] || {}),
        loading: true,
        error: null,
      },
    }));

    return request;
  }, [datasets, SUPPORTED]);

  const preloadDataset = useCallback((dataset) => {
    if (!dataset || !SUPPORTED.has(dataset)) {
      return;
    }
    const params = new URLSearchParams({ dataset });

    ensureDataset(dataset)
      .catch(() => {
        /* handled in ensureDataset */
      })
      .finally(() => {
        fetch(`${MAP_CONFIG.API_PATHS.WARD_PREWARM}?${params.toString()}`, {
          method: 'POST',
          keepalive: true,
        }).catch(() => {
          /* non-blocking */
        });
      });
  }, [ensureDataset, SUPPORTED]);

  const value = useMemo(() => ({
    datasets,
    ensureDataset,
    preloadDataset,
    ensureGeojson: (dataset) => {
      if (!dataset || !SUPPORTED.has(dataset)) {
        return Promise.resolve(null);
      }
      const existing = datasets[dataset];
      const priorVersion = existing?.version ?? null;
      if (existing?.geojson) {
        return Promise.resolve(existing.geojson);
      }
      if (geojsonPendingRef.current.has(dataset)) {
        return geojsonPendingRef.current.get(dataset);
      }
      const params = new URLSearchParams({ dataset });
      const headers = {
        Accept: 'application/json',
      };
      if (existing?.geojsonEtag) {
        headers['If-None-Match'] = existing.geojsonEtag;
      }
      const request = fetch(`${MAP_CONFIG.API_PATHS.WARD_GEOJSON}?${params.toString()}`, { headers })
        .then(async (response) => {
          if (response.status === 304) {
            return { payload: null, etag: response.headers.get('etag') };
          }
          if (!response.ok) {
            throw new Error(`Ward geojson request failed with status ${response.status}`);
          }
          const payload = await response.json();
          return { payload, etag: response.headers.get('etag') };
        })
        .then(({ payload, etag }) => {
          setDatasets((previous) => ({
            ...previous,
            [dataset]: {
              ...(previous[dataset] || {}),
              geojson: payload ?? previous[dataset]?.geojson ?? null,
              geojsonEtag: etag ?? previous[dataset]?.geojsonEtag ?? null,
            },
          }));
          if (payload) {
            saveWardGeojson(dataset, {
              data: payload,
              etag: etag ?? null,
              version: priorVersion,
            }).catch(() => {
              /* cache optional */
            });
          }
          return payload ?? existing?.geojson ?? null;
        })
        .catch((error) => {
          console.error('Failed to load ward geojson', error);
          return null;
        })
        .finally(() => {
          geojsonPendingRef.current.delete(dataset);
        });
      geojsonPendingRef.current.set(dataset, request);
      return request;
    },
    getDataset: (dataset) => datasets[dataset] || null,
  }), [datasets, ensureDataset, preloadDataset, SUPPORTED]);

  return (
    <WardDataContext.Provider value={value}>
      {children}
    </WardDataContext.Provider>
  );
}

export function useWardData() {
  const context = useContext(WardDataContext);
  if (!context) {
    throw new Error('useWardData must be used within a WardDataProvider');
  }
  return context;
}
