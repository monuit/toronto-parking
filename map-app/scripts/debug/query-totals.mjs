import { Client } from 'pg';

const connectionString = 'postgres://postgres:REDACTED_POSTGRES_PASSWORD@interchange.proxy.rlwy.net:57747/railway';

async function run() {
  const client = new Client({ connectionString, ssl: { rejectUnauthorized: false } });
  await client.connect();

  const queries = [
    {
      name: 'parking_ticket_yearly_locations',
      sql: `SELECT COUNT(DISTINCT location) AS locations,
                    SUM(ticket_count)::bigint AS tickets,
                    SUM(total_revenue)::numeric AS revenue
             FROM parking_ticket_yearly_locations`,
    },
    {
      name: 'parking_tickets',
      sql: `SELECT COUNT(*)::bigint AS tickets,
                    SUM(set_fine_amount)::numeric AS revenue
             FROM parking_tickets`,
    },
    {
      name: 'parking_tickets_staging',
      sql: `SELECT COUNT(*)::bigint AS tickets FROM parking_tickets_staging`,
    },
    {
      name: 'parking_tickets_latest_date',
      sql: `SELECT MAX(date_of_infraction) AS max_date FROM parking_tickets`,
    },
    {
      name: 'parking_tickets_min_date',
      sql: `SELECT MIN(date_of_infraction) AS min_date FROM parking_tickets`,
    },
    {
      name: 'parking_tickets_by_year',
      sql: `SELECT date_part('year', date_of_infraction)::int AS year,
                    COUNT(*)::bigint AS tickets,
                    SUM(set_fine_amount)::numeric AS revenue
             FROM parking_tickets
             GROUP BY 1
             ORDER BY 1`,
    },
    {
      name: 'parking_tickets_duplicate_count',
      sql: `SELECT COUNT(*)::bigint AS total_rows,
                    COUNT(DISTINCT ticket_number)::bigint AS distinct_tickets,
                    COUNT(*)::bigint - COUNT(DISTINCT ticket_number)::bigint AS duplicate_tickets
             FROM parking_tickets`,
    },
    {
      name: 'red_light_yearly_locations',
      sql: `SELECT COUNT(DISTINCT location_code) AS locations,
                    SUM(ticket_count)::bigint AS tickets,
                    SUM(total_revenue)::numeric AS revenue
             FROM red_light_yearly_locations`,
    },
    {
      name: 'red_light_camera_locations sample',
      sql: `SELECT * FROM red_light_camera_locations LIMIT 1`,
    },
    {
      name: 'ase_yearly_locations sample',
      sql: `SELECT * FROM ase_yearly_locations LIMIT 1`,
    },
    {
      name: 'ase_yearly_locations totals',
      sql: `SELECT COUNT(DISTINCT location_code) AS locations,
                    SUM(ticket_count)::bigint AS tickets,
                    SUM(total_revenue)::numeric AS revenue
             FROM ase_yearly_locations`,
    },
  ];

  for (const query of queries) {
    const result = await client.query(query.sql);
    console.log(query.name, result.rows.length === 1 ? result.rows[0] : result.rows);
  }

  await client.end();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
