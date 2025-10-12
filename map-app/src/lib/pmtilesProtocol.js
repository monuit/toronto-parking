import maplibregl from 'maplibre-gl';
import { PMTiles, Protocol } from 'pmtiles';

let protocolInstance = null;
const registeredUrls = new Set();

function isClient() {
  return typeof window !== 'undefined';
}

function ensureProtocol() {
  if (!isClient()) {
    return null;
  }
  if (!protocolInstance) {
    protocolInstance = new Protocol();
    maplibregl.addProtocol('pmtiles', protocolInstance.tile);
  }
  return protocolInstance;
}

function resolveAbsoluteUrl(url) {
  if (typeof url !== 'string' || url.length === 0) {
    return null;
  }
  const base = typeof window !== 'undefined' ? window.location?.origin : undefined;
  try {
    return new URL(url, base).toString();
  } catch {
    return null;
  }
}

function registerShardUrl(protocol, url) {
  const absolute = resolveAbsoluteUrl(url);
  if (!absolute || registeredUrls.has(absolute)) {
    return false;
  }
  try {
    protocol.add(new PMTiles(absolute));
    registeredUrls.add(absolute);
    return true;
  } catch {
    registeredUrls.delete(absolute);
    return false;
  }
}

export function registerPmtilesSources(manifest) {
  if (!manifest?.enabled || !isClient()) {
    return;
  }
  const protocol = ensureProtocol();
  if (!protocol) {
    return;
  }
  const collections = [manifest.datasets || {}, manifest.wardDatasets || {}];
  for (const collection of collections) {
    for (const dataset of Object.values(collection)) {
      if (!dataset) {
        continue;
      }
      const shardList = Array.isArray(dataset.shards) ? dataset.shards : [dataset];
      for (const shard of shardList) {
        if (registerShardUrl(protocol, shard?.originUrl)) {
          continue;
        }
        registerShardUrl(protocol, shard?.url);
      }
    }
  }
}

export function getRegisteredPmtilesUrls() {
  return new Set(registeredUrls);
}

export function getPmtilesDataset(manifest, datasetKey, category = 'datasets') {
  if (!manifest?.[category]) {
    return null;
  }
  return manifest[category][datasetKey] || null;
}

export function getPmtilesShardUrl(datasetConfig) {
  if (!datasetConfig) {
    return null;
  }
  if (Array.isArray(datasetConfig.shards)) {
    const primary = datasetConfig.shards[0];
    return primary?.url || null;
  }
  return datasetConfig.url || null;
}
