import { Client } from 'pg';

const connectionString = 'postgres://postgres:CA3DeGBF23F5C3Aag3Ecg4f2eDGD52Be@interchange.proxy.rlwy.net:57747/railway';

async function run() {
  const client = new Client({ connectionString, ssl: { rejectUnauthorized: false } });
  await client.connect();

  const { rows } = await client.query(`
    SELECT COUNT(*)::BIGINT AS missing_location
    FROM parking_tickets
    WHERE location2 IS NULL OR TRIM(location2) = '' OR LOWER(TRIM(location2)) = 'nan'
  `);

  console.log(rows[0]);

  await client.end();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
