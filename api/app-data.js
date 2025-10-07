/* eslint-env node */
import { readFile } from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Point to the public data directory in the deployed build
const DATA_DIR = path.resolve(__dirname, '../map-app/public/data');

async function loadDataFiles() {
  const [
    streetStats,
    neighbourhoodStats,
    officerStats,
    centreline,
    neighbourhoods
  ] = await Promise.all([
    readFile(path.join(DATA_DIR, 'street_stats.json'), 'utf-8').then(JSON.parse),
    readFile(path.join(DATA_DIR, 'neighbourhood_stats.json'), 'utf-8').then(JSON.parse),
    readFile(path.join(DATA_DIR, 'officer_stats.json'), 'utf-8').then(JSON.parse),
    readFile(path.join(DATA_DIR, 'centreline_lookup.json'), 'utf-8').then(JSON.parse),
    readFile(path.join(DATA_DIR, 'neighbourhoods.geojson'), 'utf-8').then(JSON.parse),
  ]);

  return {
    streetStats,
    neighbourhoodStats,
    officerStats,
    centreline,
    neighbourhoods
  };
}

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const data = await loadDataFiles();

    res.setHeader('Cache-Control', 'public, max-age=900, stale-while-revalidate=60');
    res.status(200).json(data);
  } catch (error) {
    console.error('Failed to load application data:', error);
    res.status(500).json({ error: 'Failed to load app data' });
  }
}
