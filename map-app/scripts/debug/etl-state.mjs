import { Client } from 'pg';

const connectionString = 'postgres://postgres:REDACTED_POSTGRES_PASSWORD@interchange.proxy.rlwy.net:57747/railway';

async function run() {
  const client = new Client({ connectionString, ssl: { rejectUnauthorized: false } });
  await client.connect();

  const { rows } = await client.query(
    `SELECT dataset_slug, metadata FROM etl_state WHERE dataset_slug IN ('parking_tickets', 'red_light_locations', 'ase_locations')`
  );

  for (const row of rows) {
    console.log(row.dataset_slug, row.metadata ? JSON.stringify(row.metadata).slice(0, 400) : null);
  }

  await client.end();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
