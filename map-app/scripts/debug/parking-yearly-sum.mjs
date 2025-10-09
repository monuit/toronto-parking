import { Client } from 'pg';

const connectionString = 'postgres://postgres:CA3DeGBF23F5C3Aag3Ecg4f2eDGD52Be@interchange.proxy.rlwy.net:57747/railway';

async function run() {
  const client = new Client({ connectionString, ssl: { rejectUnauthorized: false } });
  await client.connect();
  const { rows } = await client.query(`
    SELECT SUM(ticket_count)::BIGINT AS tickets
    FROM parking_ticket_yearly_locations
  `);
  console.log(rows[0]);
  await client.end();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
