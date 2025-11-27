#!/usr/bin/env node

import pg from 'pg';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load environment
dotenv.config({ path: join(__dirname, '.env.local') });

const { Client } = pg;

async function main() {
  const client = new Client({
    connectionString: process.env.TILES_DB_URL
  });

  try {
    await client.connect();
    console.log('✅ Connected to database\n');

    // Step 1: Find school-related tables
    console.log('📋 Step 1: Finding school-related tables...\n');
    const tablesResult = await client.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND (table_name LIKE '%school%' OR table_name LIKE '%enforcement%')
      ORDER BY table_name
    `);

    console.log('Tables found:');
    tablesResult.rows.forEach(r => console.log('  -', r.table_name));
    console.log('');

    // Step 2: Check if enforcement_schools table exists
    const hasSchoolTable = tablesResult.rows.some(r => r.table_name === 'enforcement_schools');

    if (!hasSchoolTable) {
      console.log('⚠️  No enforcement_schools table found.');
      console.log('Looking for alternative school zone data...\n');
    } else {
      console.log('✅ Found enforcement_schools table!\n');

      // Step 3: Get school count and sample data
      console.log('📊 Step 3: Analyzing school zones...\n');
      const schoolCount = await client.query(`
        SELECT COUNT(*) as total FROM enforcement_schools
      `);
      console.log(`Total schools in database: ${schoolCount.rows[0].total}\n`);

      // Sample schools
      const sampleSchools = await client.query(`
        SELECT * FROM enforcement_schools LIMIT 5
      `);
      console.log('Sample school records:');
      console.log(JSON.stringify(sampleSchools.rows, null, 2));
      console.log('');
    }

    // Step 4: Check parking_tickets schema for location data
    console.log('📋 Step 4: Checking parking_tickets schema...\n');
    const ticketSchema = await client.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = 'parking_tickets'
      ORDER BY ordinal_position
    `);

    console.log('Parking tickets columns:');
    ticketSchema.rows.forEach(r => {
      console.log(`  - ${r.column_name} (${r.data_type})`);
    });
    console.log('');

    // Step 5: Try to match tickets near schools
    if (hasSchoolTable) {
      console.log('📍 Step 5: Analyzing tickets near school zones...\n');

      // Check if we have geometry data
      const hasGeom = ticketSchema.rows.some(r => r.column_name === 'geom');

      if (hasGeom) {
        console.log('Querying tickets within 100m of schools...\n');

        const nearSchools = await client.query(`
          SELECT
            COUNT(*) as tickets_near_schools,
            ROUND(AVG(pt.set_fine_amount::numeric), 2) as avg_fine_near_schools
          FROM parking_tickets pt
          CROSS JOIN LATERAL (
            SELECT 1
            FROM enforcement_schools es
            WHERE ST_DWithin(
              pt.geom::geography,
              es.geom::geography,
              100
            )
            LIMIT 1
          ) nearby
          WHERE pt.date_of_infraction >= '2023-01-01'
        `);

        console.log('Tickets near schools (2023-2024, within 100m):');
        console.log(JSON.stringify(nearSchools.rows[0], null, 2));
        console.log('');

        // Compare to overall statistics
        const overallStats = await client.query(`
          SELECT
            COUNT(*) as total_tickets,
            ROUND(AVG(set_fine_amount::numeric), 2) as avg_fine_overall
          FROM parking_tickets
          WHERE date_of_infraction >= '2023-01-01'
        `);

        console.log('Overall tickets (2023-2024):');
        console.log(JSON.stringify(overallStats.rows[0], null, 2));
        console.log('');

        // Calculate percentage
        const nearSchoolCount = parseInt(nearSchools.rows[0].tickets_near_schools);
        const totalCount = parseInt(overallStats.rows[0].total_tickets);
        const percentage = ((nearSchoolCount / totalCount) * 100).toFixed(2);

        console.log('📊 ANALYSIS:');
        console.log(`  - ${percentage}% of tickets issued within 100m of schools`);
        console.log(`  - Near schools avg fine: $${nearSchools.rows[0].avg_fine_near_schools}`);
        console.log(`  - Overall avg fine: $${overallStats.rows[0].avg_fine_overall}`);

        if (nearSchools.rows[0].avg_fine_near_schools > overallStats.rows[0].avg_fine_overall) {
          console.log('  - ⚠️  School zone fines are HIGHER than average');
        } else {
          console.log('  - ℹ️  School zone fines are similar to average');
        }
        console.log('');
      }
    }

    // Step 6: Check for infraction codes related to school zones
    console.log('📋 Step 6: Looking for school zone-specific infractions...\n');
    const schoolInfractions = await client.query(`
      SELECT
        infraction_code,
        infraction_description,
        COUNT(*) as ticket_count,
        ROUND(AVG(set_fine_amount::numeric), 2) as avg_fine
      FROM parking_tickets
      WHERE infraction_description ILIKE '%school%'
        AND date_of_infraction >= '2023-01-01'
      GROUP BY infraction_code, infraction_description
      ORDER BY ticket_count DESC
      LIMIT 10
    `);

    if (schoolInfractions.rows.length > 0) {
      console.log('School zone-specific infractions (2023-2024):');
      schoolInfractions.rows.forEach(r => {
        console.log(`  - ${r.infraction_code}: ${r.infraction_description}`);
        console.log(`    Count: ${r.ticket_count} | Avg Fine: $${r.avg_fine}`);
      });
    } else {
      console.log('⚠️  No school zone-specific infractions found in descriptions.');
    }
    console.log('');

  } catch (err) {
    console.error('❌ Error:', err.message);
    console.error(err.stack);
  } finally {
    await client.end();
    console.log('✅ Connection closed');
  }
}

main();
