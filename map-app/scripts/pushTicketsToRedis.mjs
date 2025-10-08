import { fileURLToPath } from 'url';
import path from 'path';
import { readFile } from 'fs/promises';
import { storeTicketsRaw, TICKETS_FILE } from '../server/ticketsDataStore.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function resolveTicketsFile() {
  if (TICKETS_FILE) {
    return TICKETS_FILE;
  }
  const dataDir = process.env.DATA_DIR || path.resolve(__dirname, '../public/data');
  return path.join(dataDir, 'tickets_aggregated.geojson');
}

async function main() {
  const target = await resolveTicketsFile();
  const raw = await readFile(target, 'utf-8');
  await storeTicketsRaw(raw);
  console.log(`Cached ${path.basename(target)} in Redis.`);
  process.exit(0);
}

main().catch((error) => {
  console.error('Failed to cache tickets data in Redis:', error);
  process.exit(1);
});
