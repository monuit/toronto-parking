/* eslint-env serviceworker */

const TILE_CACHE_NAME = 'tile-cache-v1';
const ASSET_CACHE_NAME = 'map-assets-v1';
const MAX_TILE_ENTRIES = 180;
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
  const cachedResponse = await cache.match(request);
  const fetchPromise = fetch(request)
    .then(async (response) => {
      if (response && response.ok && response.type === 'basic') {
        await cache.put(request, response.clone());
        if (cacheName === TILE_CACHE_NAME) {
          await trimCache(cacheName, MAX_TILE_ENTRIES);
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

async function cacheFirst(request, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(request);
  if (cached) {
    return cached;
  }
  const response = await fetch(request);
  if (response && response.ok) {
    await cache.put(request, response.clone());
  }
  return response;
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
    event.respondWith(cacheFirst(request, ASSET_CACHE_NAME));
  }
});
