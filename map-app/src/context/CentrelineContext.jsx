/* eslint-disable react-refresh/only-export-components */
/**
 * CentrelineContext
 * Single responsibility: expose precomputed street + centreline lookup data
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { MAP_CONFIG } from '../lib/mapSources.js';

const defaultValue = {
  isLoading: true,
  error: null,
  streetSummaries: [],
  centrelineDetails: {},
  getStreetSummary: () => null,
  getCentrelineDetail: () => null,
};

export const CentrelineContext = createContext(defaultValue);

function normaliseStreetName(name) {
  if (!name) {
    return '';
  }
  return String(name).trim().toUpperCase();
}

export function CentrelineProvider({ children }) {
  const [lookup, setLookup] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    async function loadLookup() {
      try {
        setLoading(true);
        const response = await fetch(MAP_CONFIG.DATA_PATHS.CENTRELINE_LOOKUP);
        if (!response.ok) {
          throw new Error(`Failed to load centreline lookup (${response.status})`);
        }
        const payload = await response.json();
        if (!cancelled) {
          setLookup(payload);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err : new Error('Unknown lookup failure'));
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    loadLookup();

    return () => {
      cancelled = true;
    };
  }, []);

  const streetSummaries = useMemo(() => {
    if (!lookup?.streets) {
      return [];
    }
    return Object.values(lookup.streets);
  }, [lookup]);

  const streetIndex = useMemo(() => {
    const index = new Map();
    streetSummaries.forEach((summary) => {
      const key = normaliseStreetName(summary?.street ?? summary?.name);
      if (key) {
        index.set(key, summary);
      }
    });
    return index;
  }, [streetSummaries]);

  const centrelineIndex = useMemo(() => {
    const index = new Map();
    if (lookup?.centreline) {
      Object.entries(lookup.centreline).forEach(([key, detail]) => {
        index.set(String(key), detail);
      });
    }
    return index;
  }, [lookup]);

  const getStreetSummary = useCallback((streetName) => {
    if (!streetName) {
      return null;
    }
    return streetIndex.get(normaliseStreetName(streetName)) ?? null;
  }, [streetIndex]);

  const getCentrelineDetail = useCallback((centrelineId) => {
    if (centrelineId === null || centrelineId === undefined) {
      return null;
    }
    const key = String(centrelineId);
    return centrelineIndex.get(key) ?? null;
  }, [centrelineIndex]);

  const centrelineDetails = useMemo(() => {
    if (!lookup?.centreline) {
      return {};
    }
    return lookup.centreline;
  }, [lookup]);

  const contextValue = useMemo(() => ({
    isLoading: loading,
    error,
    streetSummaries,
    centrelineDetails,
    getStreetSummary,
    getCentrelineDetail,
  }), [loading, error, streetSummaries, centrelineDetails, getStreetSummary, getCentrelineDetail]);

  return (
    <CentrelineContext.Provider value={contextValue}>
      {children}
    </CentrelineContext.Provider>
  );
}

export function useCentrelineLookup() {
  return useContext(CentrelineContext);
}

