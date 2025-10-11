import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const SHARD_CONFIG_PATH = path.join(__dirname, 'shards.json');

let cachedConfig = null;

function loadShardConfig() {
  if (!cachedConfig) {
    const raw = fs.readFileSync(SHARD_CONFIG_PATH, 'utf-8');
    cachedConfig = JSON.parse(raw);
  }
  return cachedConfig;
}

export function getShardConfig() {
  return loadShardConfig();
}

export function getDatasetConfig() {
  return loadShardConfig().datasets || {};
}

export function getWardDatasetConfig() {
  return loadShardConfig().wardDatasets || {};
}

export const shardConfigPath = SHARD_CONFIG_PATH;
