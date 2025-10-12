import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { registerPmtilesSources } from '../lib/pmtilesProtocol.js';

const PmtilesContext = createContext({ manifest: null, ready: false, error: null, refresh: () => {} });

export function PmtilesProvider({ children }) {
  const getInlineManifest = useCallback(() => {
    if (typeof window === 'undefined') {
      return null;
    }
    const payload = window.__PMTILES_MANIFEST__;
    if (!payload || typeof payload !== 'object') {
      return null;
    }
    return payload;
  }, []);

  const [manifest, setManifest] = useState(() => getInlineManifest());
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const seededInlineManifest = useRef(Boolean(manifest));

  const loadManifest = useCallback(async () => {
    if (loading) {
      return;
    }
    if (manifest?.enabled) {
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
  }, [loading, manifest]);

  useEffect(() => {
    if (manifest?.enabled || loading) {
      return;
    }
    if (!seededInlineManifest.current) {
      const inline = getInlineManifest();
      if (inline) {
        seededInlineManifest.current = true;
        setManifest(inline);
        setError(null);
        return;
      }
    }
    loadManifest();
  }, [getInlineManifest, loadManifest, loading, manifest]);

  useEffect(() => {
    if (manifest?.enabled) {
      registerPmtilesSources(manifest);
      if (typeof window !== 'undefined') {
        window.__PMTILES_MANIFEST__ = manifest;
      }
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

