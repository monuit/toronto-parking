#!/usr/bin/env node
/**
 * Check infraction descriptions in parking_tickets table
 * Verify data quality and show examples
 */

const { Pool } = require('pg');
require('dotenv').config({ path: '.env.local' });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function checkInfractionDescriptions() {
  try {
    console.log('🔍 Checking infraction_description field...\n');

    // 1. Check for NULL/empty values
    const nullCheck = await pool.query(`
      SELECT
        COUNT(*) as total_tickets,
        SUM(CASE WHEN infraction_description IS NULL THEN 1 ELSE 0 END) as null_count,
        SUM(CASE WHEN infraction_description = '' THEN 1 ELSE 0 END) as empty_count,
        SUM(CASE WHEN infraction_description IS NOT NULL AND infraction_description != '' THEN 1 ELSE 0 END) as populated_count
      FROM parking_tickets
    `);

    const stats = nullCheck.rows[0];
    console.log('📊 STATS:');
    console.log(`  Total tickets: ${stats.total_tickets}`);
    console.log(`  Populated: ${stats.populated_count} (${((stats.populated_count / stats.total_tickets) * 100).toFixed(2)}%)`);
    console.log(`  NULL values: ${stats.null_count}`);
    console.log(`  Empty strings: ${stats.empty_count}\n`);

    // 2. Show unique infraction descriptions
    const uniqueCheck = await pool.query(`
      SELECT
        COUNT(DISTINCT infraction_description) as unique_descriptions,
        COUNT(DISTINCT infraction_code) as unique_codes
      FROM parking_tickets
      WHERE infraction_description IS NOT NULL AND infraction_description != ''
    `);

    console.log('📋 UNIQUE VALUES:');
    console.log(`  Unique descriptions: ${uniqueCheck.rows[0].unique_descriptions}`);
    console.log(`  Unique codes: ${uniqueCheck.rows[0].unique_codes}\n`);

    // 3. Show examples of descriptions
    const examples = await pool.query(`
      SELECT
        infraction_code,
        infraction_description,
        COUNT(*) as frequency
      FROM parking_tickets
      WHERE infraction_description IS NOT NULL AND infraction_description != ''
      GROUP BY infraction_code, infraction_description
      ORDER BY frequency DESC
      LIMIT 20
    `);

    console.log('📝 TOP 20 INFRACTION DESCRIPTIONS:\n');
    examples.rows.forEach((row, idx) => {
      console.log(`${idx + 1}. Code: ${row.infraction_code}`);
      console.log(`   Description: ${row.infraction_description}`);
      console.log(`   Frequency: ${row.frequency}\n`);
    });

    // 4. Check for "PARK-SIGNED" type descriptions
    const parkSignedCheck = await pool.query(`
      SELECT
        infraction_description,
        COUNT(*) as count
      FROM parking_tickets
      WHERE infraction_description LIKE 'PARK%SIGN%'
         OR infraction_description LIKE '%PROHIBIT%'
      GROUP BY infraction_description
      ORDER BY count DESC
      LIMIT 10
    `);

    console.log('🎯 DESCRIPTIONS WITH "PARK-SIGNED" or "PROHIBIT":\n');
    if (parkSignedCheck.rows.length === 0) {
      console.log('  (No exact matches found)\n');
    } else {
      parkSignedCheck.rows.forEach((row) => {
        console.log(`  "${row.infraction_description}" (${row.count} tickets)`);
      });
      console.log('');
    }

    // 5. Show sample tickets with full details
    const samples = await pool.query(`
      SELECT
        ticket_number,
        date_of_infraction,
        infraction_code,
        infraction_description,
        set_fine_amount,
        location1
      FROM parking_tickets
      WHERE infraction_description IS NOT NULL AND infraction_description != ''
      LIMIT 5
    `);

    console.log('💳 SAMPLE TICKETS:\n');
    samples.rows.forEach((row, idx) => {
      console.log(`${idx + 1}. Ticket #${row.ticket_number}`);
      console.log(`   Date: ${row.date_of_infraction}`);
      console.log(`   Code: ${row.infraction_code}`);
      console.log(`   Description: ${row.infraction_description}`);
      console.log(`   Fine: $${row.set_fine_amount}`);
      console.log(`   Location: ${row.location1}\n`);
    });

  } catch (err) {
    console.error('❌ Error:', err.message);
  } finally {
    await pool.end();
  }
}

checkInfractionDescriptions();
