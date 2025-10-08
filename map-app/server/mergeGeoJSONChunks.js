/**
 * Merge split GeoJSON chunks into a single file
 * This runs once at server startup to combine the split data files
 */

/* global process */

import fs from 'node:fs';
import { promises as fsPromises } from 'node:fs';
import { once } from 'node:events';
import path from 'path';
import { fileURLToPath } from 'url';
import { ensureTicketsFileFromRedis, storeTicketsRaw, TICKETS_FILE } from './ticketsDataStore.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../public/data');
const OUTPUT_FILE = TICKETS_FILE;
const CHUNK_PATTERN = /^tickets_aggregated_part\d+\.geojson$/;

/**
 * Merge split GeoJSON files into a single file
 */
export async function mergeGeoJSONChunks() {
  console.log('🔗 Merging GeoJSON chunks...');

  // Check if merged file already exists or can be restored from Redis
  if (fs.existsSync(OUTPUT_FILE)) {
    const stats = fs.statSync(OUTPUT_FILE);
    const sizeMB = (stats.size / (1024 * 1024)).toFixed(2);
    console.log(`✓ Merged file already exists: ${sizeMB} MB`);
    return OUTPUT_FILE;
  }

  try {
    const restored = await ensureTicketsFileFromRedis();
    if (restored) {
      const stats = fs.statSync(OUTPUT_FILE);
      const sizeMB = (stats.size / (1024 * 1024)).toFixed(2);
      console.log(`✓ Restored merged file from Redis cache: ${sizeMB} MB`);
      return OUTPUT_FILE;
    }
  } catch (error) {
    console.warn('Unable to restore tickets_aggregated from Redis:', error.message);
  }

  // Find all chunk files
  const files = await fsPromises.readdir(DATA_DIR);
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

  const writeStream = fs.createWriteStream(OUTPUT_FILE, { encoding: 'utf-8' });
  const write = async (content) => {
    if (!writeStream.write(content)) {
      await once(writeStream, 'drain');
    }
  };

  let totalFeatures = 0;
  let firstFeature = true;

  try {
    await write('{"type":"FeatureCollection","features":[');

    for (const chunkFile of chunkFiles) {
      const chunkPath = path.join(DATA_DIR, chunkFile);
      console.log(`  Reading ${chunkFile}...`);

      let raw = await fsPromises.readFile(chunkPath, 'utf-8');
      const data = JSON.parse(raw);
      const features = Array.isArray(data.features) ? data.features : [];

      for (const feature of features) {
        if (!firstFeature) {
          await write(',');
        } else {
          firstFeature = false;
        }
        await write(JSON.stringify(feature));
      }

      totalFeatures += features.length;
      console.log(`    Added ${features.length.toLocaleString()} features (total: ${totalFeatures.toLocaleString()})`);

      data.features = null;
      raw = null;
    }

    await write(']}');
    writeStream.end();
    await once(writeStream, 'finish');
  } catch (error) {
    writeStream.destroy();
    throw error;
  }

  console.log(`  Total features merged: ${totalFeatures.toLocaleString()}`);

  console.log('  Writing merged file and caching in Redis...');
  let mergedRaw = await fsPromises.readFile(OUTPUT_FILE, 'utf-8');
  await storeTicketsRaw(mergedRaw);
  mergedRaw = null;

  const stats = await fsPromises.stat(OUTPUT_FILE);
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
