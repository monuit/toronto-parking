import { Client } from 'pg';

const connectionString = 'postgres://postgres:REDACTED_POSTGRES_PASSWORD@interchange.proxy.rlwy.net:57747/railway';

async function run() {
  const client = new Client({ connectionString, ssl: { rejectUnauthorized: false } });
  await client.connect();

  const { rows } = await client.query(`
    SELECT
      COUNT(*) FILTER (WHERE location_code IS NULL)::BIGINT AS missing_code,
      COUNT(*)::BIGINT AS total_rows,
      COUNT(DISTINCT location_code)::BIGINT AS distinct_codes
    FROM red_light_camera_locations
  `);

  console.log(rows[0]);

  await client.end();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
