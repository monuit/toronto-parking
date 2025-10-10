const DB_NAME = 'toronto-ward-cache';
const DB_VERSION = 1;
const STORE_SUMMARY = 'summaries';
const STORE_GEOJSON = 'geojson';

const isBrowser = typeof window !== 'undefined';
const hasIndexedDB = isBrowser && typeof window.indexedDB !== 'undefined';

function getLocalStorage() {
  if (!isBrowser) {
    return null;
  }
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function getStorageKey(dataset, kind) {
  return `ward-cache:${kind}:${dataset}`;
}

function openDatabase() {
  if (!hasIndexedDB) {
    return Promise.resolve(null);
  }
  return new Promise((resolve, reject) => {
    const request = window.indexedDB.open(DB_NAME, DB_VERSION);
    request.onerror = () => {
      reject(request.error);
    };
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains(STORE_SUMMARY)) {
        db.createObjectStore(STORE_SUMMARY, { keyPath: 'dataset' });
      }
      if (!db.objectStoreNames.contains(STORE_GEOJSON)) {
        db.createObjectStore(STORE_GEOJSON, { keyPath: 'dataset' });
      }
    };
    request.onsuccess = () => {
      resolve(request.result);
    };
  });
}

async function writeEntry(storeName, entry) {
  if (hasIndexedDB) {
    try {
      const db = await openDatabase();
      if (!db) {
        return;
      }
      await new Promise((resolve, reject) => {
        const transaction = db.transaction(storeName, 'readwrite');
        transaction.oncomplete = () => {
          db.close();
          resolve();
        };
        transaction.onerror = () => {
          db.close();
          reject(transaction.error);
        };
        transaction.objectStore(storeName).put(entry);
      });
      return;
    } catch (error) {
      console.warn('Failed to persist ward cache entry:', error?.message ?? error);
    }
  }
  const storage = getLocalStorage();
  if (!storage) {
    return;
  }
  const key = getStorageKey(entry.dataset, storeName === STORE_SUMMARY ? 'summary' : 'geojson');
  storage.setItem(key, JSON.stringify(entry));
}

async function readEntry(storeName, dataset) {
  if (hasIndexedDB) {
    try {
      const db = await openDatabase();
      if (!db) {
        return null;
      }
      const result = await new Promise((resolve, reject) => {
        const transaction = db.transaction(storeName, 'readonly');
        transaction.oncomplete = () => {
          db.close();
        };
        transaction.onerror = () => {
          db.close();
          reject(transaction.error);
        };
        const request = transaction.objectStore(storeName).get(dataset);
        request.onsuccess = () => {
          resolve(request.result ?? null);
        };
        request.onerror = () => {
          reject(request.error);
        };
      });
      if (result) {
        return result;
      }
    } catch (error) {
      console.warn('Failed to read ward cache entry:', error?.message ?? error);
    }
  }
  const storage = getLocalStorage();
  if (!storage) {
    return null;
  }
  const key = getStorageKey(dataset, storeName === STORE_SUMMARY ? 'summary' : 'geojson');
  try {
    const raw = storage.getItem(key);
    return raw ? JSON.parse(raw) : null;
  } catch {
    storage.removeItem(key);
    return null;
  }
}

export async function loadCachedWardSummary(dataset) {
  if (!dataset) {
    return null;
  }
  const entry = await readEntry(STORE_SUMMARY, dataset);
  return entry || null;
}

export async function saveWardSummary(dataset, payload) {
  if (!dataset || !payload) {
    return;
  }
  const entry = {
    dataset,
    etag: payload.etag ?? null,
    version: payload.version ?? null,
    updatedAt: payload.generatedAt ?? new Date().toISOString(),
    data: payload.data ?? null,
  };
  await writeEntry(STORE_SUMMARY, entry);
}

export async function loadCachedWardGeojson(dataset) {
  if (!dataset) {
    return null;
  }
  const entry = await readEntry(STORE_GEOJSON, dataset);
  if (!entry || !entry.data) {
    return null;
  }
  return entry;
}

export async function saveWardGeojson(dataset, payload) {
  if (!dataset || !payload) {
    return;
  }
  const entry = {
    dataset,
    etag: payload.etag ?? null,
    version: payload.version ?? null,
    updatedAt: payload.generatedAt ?? new Date().toISOString(),
    data: payload.data ?? null,
  };
  await writeEntry(STORE_GEOJSON, entry);
}
