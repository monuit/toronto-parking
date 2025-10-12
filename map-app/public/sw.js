/* eslint-env serviceworker */

const TILE_CACHE_NAME = 'tile-cache-v2';
const ASSET_CACHE_NAME = 'map-assets-v1';
const MAX_TILE_ENTRIES = 360;
const PRECACHE_ASSETS = ['/styles/basic-style.json'];

self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(ASSET_CACHE_NAME)
      .then((cache) => cache.addAll(PRECACHE_ASSETS))
      .catch(() => undefined),
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(
        keys
          .filter((key) => key !== TILE_CACHE_NAME && key !== ASSET_CACHE_NAME)
          .map((key) => caches.delete(key)),
      );
      if (self.clients && self.clients.claim) {
        await self.clients.claim();
      }
    })(),
  );
});

const SAME_ORIGIN = self.location.origin;

function isSameOrigin(url) {
  return url.origin === SAME_ORIGIN;
}

function isTileRequest(request) {
  const url = new URL(request.url);
  if (!isSameOrigin(url)) {
    return false;
  }
  if (url.pathname.startsWith('/tiles/')) {
    return true;
  }
  if (url.pathname.startsWith('/pmtiles/')) {
    return true;
  }
  return url.pathname.endsWith('.pmtiles') || url.pathname.endsWith('.pbf');
}

function isAssetRequest(request) {
  const url = new URL(request.url);
  if (!isSameOrigin(url)) {
    return false;
  }
  if (url.pathname.startsWith('/styles/')) {
    return true;
  }
  if (url.pathname.includes('/sprite')) {
    return true;
  }
  if (url.pathname.startsWith('/proxy/maptiler/fonts/')) {
    return true;
  }
  return false;
}

function canonicalTileKey(request) {
  const url = new URL(request.url);
  url.hash = '';
  return url.toString();
}

async function trimCache(cacheName, maxEntries) {
  if (!Number.isFinite(maxEntries) || maxEntries <= 0) {
    return;
  }
  const cache = await caches.open(cacheName);
  const keys = await cache.keys();
  if (keys.length <= maxEntries) {
    return;
  }
  const excess = keys.length - maxEntries;
  for (let index = 0; index < excess; index += 1) {
    await cache.delete(keys[index]);
  }
}

async function staleWhileRevalidate(request, cacheName) {
  const cache = await caches.open(cacheName);
  const isTileCache = cacheName === TILE_CACHE_NAME;
  const canonicalKey = isTileCache ? canonicalTileKey(request) : null;
  let cachedResponse = null;
  if (isTileCache && canonicalKey) {
    cachedResponse = await cache.match(canonicalKey);
  }
  if (!cachedResponse) {
    cachedResponse = await cache.match(request);
  }
  const networkRequest = request.clone();
  const fetchPromise = fetch(networkRequest)
    .then(async (response) => {
      if (response && response.ok && (response.type === 'basic' || response.type === 'cors')) {
        if (isTileCache) {
          if (response.status === 200 && canonicalKey) {
            await cache.put(canonicalKey, response.clone());
          } else {
            await cache.put(request, response.clone());
          }
          await trimCache(cacheName, MAX_TILE_ENTRIES);
        } else {
          await cache.put(request, response.clone());
        }
      }
      return response;
    })
    .catch((error) => {
      if (cachedResponse) {
        return cachedResponse;
      }
      throw error;
    });
  return cachedResponse || fetchPromise;
}

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') {
    return;
  }
  if (isTileRequest(request)) {
    event.respondWith(staleWhileRevalidate(request, TILE_CACHE_NAME));
    return;
  }
  if (isAssetRequest(request)) {
    event.respondWith(staleWhileRevalidate(request, ASSET_CACHE_NAME));
  }
});
