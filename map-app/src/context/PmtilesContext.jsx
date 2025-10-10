import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { registerPmtilesSources } from '../lib/pmtilesProtocol.js';

const PmtilesContext = createContext({ manifest: null, ready: false, error: null, refresh: () => {} });

export function PmtilesProvider({ children }) {
  const [manifest, setManifest] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const loadManifest = useCallback(async () => {
    if (loading) {
      return;
    }
    setLoading(true);
    try {
      const response = await fetch('/api/pmtiles-manifest', { cache: 'no-store' });
      if (!response.ok) {
        if (response.status === 503) {
          const payload = await response.json().catch(() => null);
          setManifest(payload || { enabled: false });
          setError(null);
        } else {
          throw new Error(`PMTiles manifest request failed with status ${response.status}`);
        }
      } else {
        const payload = await response.json();
        setManifest(payload);
        setError(null);
      }
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [loading]);

  useEffect(() => {
    loadManifest();
  }, [loadManifest]);

  useEffect(() => {
    if (manifest?.enabled) {
      registerPmtilesSources(manifest);
    }
  }, [manifest]);

  const value = useMemo(() => ({
    manifest,
    ready: Boolean(manifest?.enabled),
    loading,
    error,
    refresh: loadManifest,
  }), [manifest, loading, error, loadManifest]);

  return (
    <PmtilesContext.Provider value={value}>
      {children}
    </PmtilesContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function usePmtiles() {
  return useContext(PmtilesContext);
}

