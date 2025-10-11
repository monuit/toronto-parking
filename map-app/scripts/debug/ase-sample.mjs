import { Client } from 'pg';

const connectionString = 'postgres://postgres:REDACTED_POSTGRES_PASSWORD@interchange.proxy.rlwy.net:57747/railway';

async function run() {
  const client = new Client({ connectionString, ssl: { rejectUnauthorized: false } });
  await client.connect();

  const baseCodes = await client.query(`
    SELECT location_code
    FROM ase_camera_locations
    ORDER BY location_code
    LIMIT 20
  `);
  console.log('base codes', baseCodes.rows);

  const { rows } = await client.query(`
    SELECT DISTINCT location_code
    FROM ase_yearly_locations
    ORDER BY location_code
    LIMIT 20
  `);

  console.log('yearly codes', rows);

  const counts = await client.query(`
    SELECT location_code, COUNT(*) AS entries
    FROM ase_yearly_locations
    GROUP BY location_code
    ORDER BY entries DESC
    LIMIT 10
  `);
  console.log('top yearly counts', counts.rows);

  const missing = await client.query(`
    SELECT DISTINCT location_code
    FROM ase_yearly_locations
    WHERE location_code NOT IN (SELECT location_code FROM ase_camera_locations)
    ORDER BY location_code
    LIMIT 20
  `);
  console.log('missing in base', missing.rows);

  await client.end();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
