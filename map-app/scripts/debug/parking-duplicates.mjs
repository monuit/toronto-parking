import { Client } from 'pg';

const connectionString = 'postgres://postgres:CA3DeGBF23F5C3Aag3Ecg4f2eDGD52Be@interchange.proxy.rlwy.net:57747/railway';

async function run() {
  const client = new Client({ connectionString, ssl: { rejectUnauthorized: false } });
  await client.connect();

  const { rows } = await client.query(`
    SELECT COUNT(*)::BIGINT AS total_rows,
           COUNT(DISTINCT ticket_number)::BIGINT AS distinct_tickets,
           COUNT(*)::BIGINT - COUNT(DISTINCT ticket_number)::BIGINT AS duplicate_tickets
    FROM parking_tickets
  `);

  console.log(rows[0]);

  await client.end();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
