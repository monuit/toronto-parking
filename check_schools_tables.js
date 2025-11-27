import { Pool } from 'pg';
import { config } from 'dotenv';
import { resolve } from 'path';
import { fileURLToPath } from 'url';

const __dirname = resolve(fileURLToPath(import.meta.url), '..');
config({ path: resolve(__dirname, 'map-app/.env.local') });

const pool = new Pool({ connectionString: process.env.TILES_DB_URL });

async function check() {
  try {
    const res = await pool.query(`
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'public'
      AND table_name ILIKE '%school%'
      ORDER BY table_name
    `);
    console.log('School-related tables:', res.rows);

    // Also check for any enforcement/camera related tables
    const res2 = await pool.query(`
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'public'
      AND (table_name ILIKE '%enforcement%' OR table_name ILIKE '%camera%' OR table_name ILIKE '%ase%')
      ORDER BY table_name
    `);
    console.log('Enforcement/Camera tables:', res2.rows);

  } catch (e) {
    console.error('Error:', e.message);
  } finally {
    await pool.end();
  }
}

check();
