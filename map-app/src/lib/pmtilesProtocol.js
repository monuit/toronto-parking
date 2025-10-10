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
        if (!shard?.url || registeredUrls.has(shard.url)) {
          continue;
        }
        protocol.add(new PMTiles(shard.url));
        registeredUrls.add(shard.url);
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
