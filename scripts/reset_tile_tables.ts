import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

import dotenv from 'dotenv';
import pg from 'pg';

const __filename = fileURLToPath(import.meta.url);
const PROJECT_ROOT = path.resolve(path.dirname(__filename), '..');

const DEFAULT_TABLES = [
  'parking_ticket_tiles',
  'red_light_camera_tiles',
  'ase_camera_tiles',
];

const tables = (process.env.TILE_TABLES || '').split(',').map((entry) => entry.trim()).filter(Boolean);
const TARGET_TABLES = tables.length > 0 ? tables : DEFAULT_TABLES;
const TOP_TABLE_LIMIT = Number.parseInt(process.env.TABLE_USAGE_LIMIT || '25', 10);

function loadEnvironment(): void {
  const envPath = path.join(PROJECT_ROOT, '.env');
  dotenv.config({ path: envPath });
}

function resolveConnectionString(): { connectionString: string; ssl?: boolean | { rejectUnauthorized: boolean } } {
  const dsn = process.env.DATABASE_PRIVATE_URL;
  if (!dsn || dsn.trim().length === 0) {
    throw new Error('DATABASE_PRIVATE_URL is not defined; cannot connect to Postgres.');
  }

  const needsSsl =
    process.env.DATABASE_SSL === '1'
    || process.env.PGSSLMODE === 'require'
    || dsn.includes('railway');

  return {
    connectionString: dsn,
    ssl: needsSsl ? { rejectUnauthorized: false } : undefined,
  };
}

async function resetTileTables(): Promise<void> {
  loadEnvironment();
  const { connectionString, ssl } = resolveConnectionString();

  const pool = new pg.Pool({
    connectionString,
    ssl,
    application_name: 'reset-tile-tables',
  });

  try {
    await reportTopTables(pool, 'Top tables before reset');
    for (const table of TARGET_TABLES) {
      if (!table) {
        continue;
      }
      await processTable(pool, table);
    }
    await reportTopTables(pool, 'Top tables after reset');
    console.log('\nTile table reset complete.');
  } finally {
    await pool.end();
  }
}

type TableRef = { schema: string; name: string };

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

function formatQualified(ref: TableRef): string {
  return `${quoteIdentifier(ref.schema)}.${quoteIdentifier(ref.name)}`;
}

async function processTable(pool: pg.Pool, table: string): Promise<void> {
  console.log(`\n=== ${table} ===`);
  const family = await fetchTableFamily(pool, table);
  if (!family.parent) {
    console.warn(`Table '${table}' not found. Skipping. Run TileSchemaManager.ensure() before resetting.`);
    await reportUsageByName(pool, table, 'Current usage');
    return;
  }

  if (family.partitions.length > 0) {
    const names = family.partitions.map((ref) => `${ref.schema}.${ref.name}`).join(', ');
    console.log(`Found partitions: ${names}`);
  }

  await reportUsage(pool, family.all, 'Before reset');

  const parentName = formatQualified(family.parent);
  try {
    console.log(`Truncating ${parentName} (cascades to partitions)`);
    await pool.query(`TRUNCATE ${parentName} RESTART IDENTITY CASCADE;`);
    console.log(`Vacuuming ${parentName}`);
    await pool.query(`VACUUM (ANALYZE) ${parentName};`);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`Reset step failed for ${parentName}: ${message}`);
    return;
  }

  await reportUsage(pool, family.all, 'After reset');
}

async function fetchTableFamily(pool: pg.Pool, parentTable: string): Promise<{ parent: TableRef | null; partitions: TableRef[]; all: TableRef[] }> {
  const parentResult = await pool.query<{
    schema_name: string;
    table_name: string;
  }>(
    `SELECT n.nspname AS schema_name, c.relname AS table_name
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'r'
       AND n.nspname NOT IN ('pg_catalog', 'information_schema')
       AND c.relname = $1
     LIMIT 1;`,
    [parentTable],
  );

  if (parentResult.rowCount === 0) {
    return { parent: null, partitions: [], all: [] };
  }

  const parentRef: TableRef = {
    schema: parentResult.rows[0].schema_name,
    name: parentResult.rows[0].table_name,
  };

  const partitionResult = await pool.query<{
    schema_name: string;
    table_name: string;
  }>(
    `SELECT n.nspname AS schema_name, c.relname AS table_name
     FROM pg_inherits i
     JOIN pg_class c ON c.oid = i.inhrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     JOIN pg_class parent ON parent.oid = i.inhparent
     WHERE parent.relname = $1
       AND n.nspname NOT IN ('pg_catalog', 'information_schema')
     ORDER BY n.nspname, c.relname;`,
    [parentTable],
  );

  const partitions: TableRef[] = partitionResult.rows.map((row) => ({
    schema: row.schema_name,
    name: row.table_name,
  }));

  return {
    parent: parentRef,
    partitions,
    all: [parentRef, ...partitions],
  };
}

async function reportUsage(pool: pg.Pool, tableRefs: TableRef[], label: string): Promise<void> {
  if (tableRefs.length === 0) {
    console.log(`${label}: no tables found.`);
    return;
  }

  const qualifiedNames = tableRefs.map((ref) => `${ref.schema}.${ref.name}`);
  const result = await pool.query<{
    table_name: string;
    total_size: string;
    heap_size: string;
    index_size: string;
    approx_live_rows: number;
    approx_dead_rows: number;
  }>(
    `SELECT
       format('%s.%s', n.nspname, c.relname) AS table_name,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
       pg_size_pretty(pg_relation_size(c.oid)) AS heap_size,
       pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) AS index_size,
       COALESCE(pg_stat_get_live_tuples(c.oid), 0) AS approx_live_rows,
       COALESCE(pg_stat_get_dead_tuples(c.oid), 0) AS approx_dead_rows
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'r'
       AND (n.nspname || '.' || c.relname) = ANY($1::text[])
     ORDER BY pg_total_relation_size(c.oid) DESC;`,
    [qualifiedNames],
  );

  console.log(label);
  if (result.rowCount === 0) {
    console.log('  (no data)');
    return;
  }

  console.table(result.rows);
}

async function reportUsageByName(pool: pg.Pool, tableName: string, label: string): Promise<void> {
  const [schema, name] = tableName.includes('.')
    ? tableName.split('.', 2)
    : ['public', tableName];
  await reportUsage(pool, [{ schema, name }], label);
}

async function reportTopTables(pool: pg.Pool, label: string): Promise<void> {
  const limit = Number.isFinite(TOP_TABLE_LIMIT) && TOP_TABLE_LIMIT > 0 ? TOP_TABLE_LIMIT : 25;
  const result = await pool.query<{
    table_name: string;
    total_size: string;
    heap_size: string;
    index_size: string;
    toast_size: string;
    total_bytes: string;
    approx_live_rows: number;
    approx_dead_rows: number;
  }>(
    `SELECT
       format('%s.%s', n.nspname, c.relname) AS table_name,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
       pg_size_pretty(pg_relation_size(c.oid)) AS heap_size,
       pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
       pg_size_pretty(CASE WHEN c.reltoastrelid <> 0 THEN pg_total_relation_size(c.reltoastrelid) ELSE 0 END) AS toast_size,
       pg_total_relation_size(c.oid)::text AS total_bytes,
       COALESCE(pg_stat_get_live_tuples(c.oid), 0) AS approx_live_rows,
       COALESCE(pg_stat_get_dead_tuples(c.oid), 0) AS approx_dead_rows
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'r'
       AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
     ORDER BY pg_total_relation_size(c.oid) DESC
     LIMIT $1;`,
    [limit],
  );

  console.log(`\n${label}`);
  if (result.rowCount === 0) {
    console.log('  (no data)');
    return;
  }

  console.table(result.rows.map((row) => ({
    table_name: row.table_name,
    total_size: row.total_size,
    heap_size: row.heap_size,
    index_size: row.index_size,
    toast_size: row.toast_size,
    approx_live_rows: row.approx_live_rows,
    approx_dead_rows: row.approx_dead_rows,
  })));
}

resetTileTables()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    console.error('Tile table reset failed:', error.message);
    process.exit(1);
  });
