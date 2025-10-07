/**
 * Merge split GeoJSON chunks into a single file
 * This runs once at server startup to combine the split data files
 */

/* global process */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../public/data');
const OUTPUT_FILE = path.join(DATA_DIR, 'tickets_aggregated.geojson');
const CHUNK_PATTERN = /^tickets_aggregated_part\d+\.geojson$/;

/**
 * Merge split GeoJSON files into a single file
 */
export async function mergeGeoJSONChunks() {
  console.log('🔗 Merging GeoJSON chunks...');

  // Check if merged file already exists
  if (fs.existsSync(OUTPUT_FILE)) {
    const stats = fs.statSync(OUTPUT_FILE);
    const sizeMB = (stats.size / (1024 * 1024)).toFixed(2);
    console.log(`✓ Merged file already exists: ${sizeMB} MB`);
    return OUTPUT_FILE;
  }

  // Find all chunk files
  const files = fs.readdirSync(DATA_DIR);
  const chunkFiles = files
    .filter(f => CHUNK_PATTERN.test(f))
    .sort((a, b) => {
      const numA = parseInt(a.match(/\d+/)[0]);
      const numB = parseInt(b.match(/\d+/)[0]);
      return numA - numB;
    });

  if (chunkFiles.length === 0) {
    throw new Error('No chunk files found to merge');
  }

  console.log(`  Found ${chunkFiles.length} chunks to merge`);

  // Merge all features
  const allFeatures = [];

  for (const chunkFile of chunkFiles) {
    const chunkPath = path.join(DATA_DIR, chunkFile);
    console.log(`  Reading ${chunkFile}...`);

    const data = JSON.parse(fs.readFileSync(chunkPath, 'utf-8'));
    allFeatures.push(...data.features);
  }

  console.log(`  Total features merged: ${allFeatures.length.toLocaleString()}`);

  // Create merged GeoJSON
  const mergedData = {
    type: 'FeatureCollection',
    features: allFeatures
  };

  // Write merged file
  console.log('  Writing merged file...');
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(mergedData));

  const stats = fs.statSync(OUTPUT_FILE);
  const sizeMB = (stats.size / (1024 * 1024)).toFixed(2);

  console.log(`✓ Merged file created: ${sizeMB} MB`);
  console.log(`✓ Output: ${OUTPUT_FILE}`);

  return OUTPUT_FILE;
}

// If run directly (not imported)
if (import.meta.url === `file://${process.argv[1]}`) {
  mergeGeoJSONChunks()
    .then(() => {
      console.log('\n✅ Merge complete!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('\n❌ Merge failed:', error.message);
      process.exit(1);
    });
}
