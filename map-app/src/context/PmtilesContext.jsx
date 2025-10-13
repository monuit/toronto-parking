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

const tilesMode = (import.meta.env?.VITE_TILES_MODE || 'pmtiles').toLowerCase();
const pmtilesFeatureEnabled = tilesMode === 'pmtiles';

const DISABLED_CONTEXT_VALUE = Object.freeze({
  manifest: null,
  ready: false,
  loading: false,
  error: null,
  refresh: () => {},
});

function readInlineManifest() {
  if (typeof window === 'undefined') {
    return null;
  }
  const payload = window.__PMTILES_MANIFEST__;
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  return payload;
}

const PmtilesContext = createContext(DISABLED_CONTEXT_VALUE);

export function PmtilesProvider({ children }) {
  const disabled = !pmtilesFeatureEnabled;

  const [manifest, setManifest] = useState(() => (disabled ? null : readInlineManifest()));
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const seededInlineManifest = useRef(disabled ? true : Boolean(manifest));

  const loadManifest = useCallback(async () => {
    if (disabled || loading || manifest?.enabled) {
      return;
    }
    setLoading(true);
    try {
      const response = await fetch('/api/pmtiles-manifest', { cache: 'no-store' });
      if (!response.ok) {
        if (response.status === 204) {
          setManifest({ enabled: false });
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
      if (!disabled) {
        setError(err);
      }
    } finally {
      setLoading(false);
    }
  }, [disabled, loading, manifest]);

  useEffect(() => {
    if (disabled) {
      return undefined;
    }
    if (manifest?.enabled || loading) {
      return undefined;
    }
    if (!seededInlineManifest.current) {
      const inline = readInlineManifest();
      if (inline) {
        seededInlineManifest.current = true;
        setManifest(inline);
        setError(null);
        return undefined;
      }
    }
    loadManifest();
    return undefined;
  }, [disabled, loadManifest, loading, manifest]);

  useEffect(() => {
    if (disabled || !manifest?.enabled) {
      return undefined;
    }
    registerPmtilesSources(manifest);
    if (typeof window !== 'undefined') {
      window.__PMTILES_MANIFEST__ = manifest;
    }
    return undefined;
  }, [disabled, manifest]);

  const activeValue = useMemo(() => ({
    manifest,
    ready: Boolean(manifest?.enabled),
    loading,
    error,
    refresh: loadManifest,
  }), [manifest, loading, error, loadManifest]);

  const value = disabled ? DISABLED_CONTEXT_VALUE : activeValue;

  return <PmtilesContext.Provider value={value}>{children}</PmtilesContext.Provider>;
}

// eslint-disable-next-line react-refresh/only-export-components
export function usePmtiles() {
  return useContext(PmtilesContext);
}

