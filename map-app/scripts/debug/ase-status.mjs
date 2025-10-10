import { Client } from 'pg';

const connectionString = 'postgres://postgres:REDACTED_POSTGRES_PASSWORD@interchange.proxy.rlwy.net:57747/railway';

async function run() {
  const client = new Client({ connectionString, ssl: { rejectUnauthorized: false } });
  await client.connect();

  const { rows } = await client.query(`
    SELECT status, COUNT(*)::BIGINT AS count
    FROM ase_camera_locations
    GROUP BY status
    ORDER BY count DESC
  `);

  console.log(rows);

  const totals = await client.query(`
    SELECT COUNT(*)::BIGINT AS total_rows,
           COUNT(DISTINCT location_code)::BIGINT AS distinct_codes
    FROM ase_camera_locations
  `);
  console.log(totals.rows[0]);

  const yearlyTotals = await client.query(`
    SELECT COUNT(*)::BIGINT AS rows,
           COUNT(DISTINCT location_code)::BIGINT AS distinct_codes,
           COUNT(DISTINCT TRIM(location_code))::BIGINT AS trimmed_codes
    FROM ase_yearly_locations
  `);
  console.log(yearlyTotals.rows[0]);

  await client.end();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
