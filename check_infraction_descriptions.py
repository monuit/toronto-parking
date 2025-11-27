#!/usr/bin/env python3
"""
Check infraction descriptions in parking_tickets table
Verify data quality and show examples
"""

import os
import psycopg
from dotenv import load_dotenv

load_dotenv('.env.local')

# Try multiple env var names
database_url = os.getenv('DATABASE_URL') or os.getenv('TILES_DB_URL') or os.getenv('CORE_DB_URL')
print(f'📡 Connecting to database...')
if not database_url:
    print('❌ DATABASE_URL, TILES_DB_URL, or CORE_DB_URL not found')
    exit(1)

def check_infraction_descriptions():
    try:
        print('🔍 Checking infraction_description field...\n')

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                # 1. Check for NULL/empty values
                cur.execute("""
                    SELECT
                        COUNT(*) as total_tickets,
                        SUM(CASE WHEN infraction_description IS NULL THEN 1 ELSE 0 END) as null_count,
                        SUM(CASE WHEN infraction_description = '' THEN 1 ELSE 0 END) as empty_count,
                        SUM(CASE WHEN infraction_description IS NOT NULL AND infraction_description != '' THEN 1 ELSE 0 END) as populated_count
                    FROM parking_tickets
                """)

                stats = cur.fetchone()
                total, null_count, empty_count, populated = stats

                print('📊 STATS:')
                print(f'  Total tickets: {total}')
                print(f'  Populated: {populated} ({(populated / total * 100):.2f}%)')
                print(f'  NULL values: {null_count}')
                print(f'  Empty strings: {empty_count}\n')

                # 2. Show unique infraction descriptions
                cur.execute("""
                    SELECT
                        COUNT(DISTINCT infraction_description) as unique_descriptions,
                        COUNT(DISTINCT infraction_code) as unique_codes
                    FROM parking_tickets
                    WHERE infraction_description IS NOT NULL AND infraction_description != ''
                """)

                unique_stats = cur.fetchone()
                print('📋 UNIQUE VALUES:')
                print(f'  Unique descriptions: {unique_stats[0]}')
                print(f'  Unique codes: {unique_stats[1]}\n')

                # 3. Show examples of descriptions
                cur.execute("""
                    SELECT
                        infraction_code,
                        infraction_description,
                        COUNT(*) as frequency
                    FROM parking_tickets
                    WHERE infraction_description IS NOT NULL AND infraction_description != ''
                    GROUP BY infraction_code, infraction_description
                    ORDER BY frequency DESC
                    LIMIT 20
                """)

                examples = cur.fetchall()
                print('📝 TOP 20 INFRACTION DESCRIPTIONS:\n')
                for idx, (code, desc, freq) in enumerate(examples, 1):
                    print(f'{idx}. Code: {code}')
                    print(f'   Description: {desc}')
                    print(f'   Frequency: {freq}\n')

                # 4. Check for "PARK-SIGNED" type descriptions
                cur.execute("""
                    SELECT
                        infraction_description,
                        COUNT(*) as count
                    FROM parking_tickets
                    WHERE infraction_description LIKE 'PARK%SIGN%'
                       OR infraction_description LIKE '%PROHIBIT%'
                    GROUP BY infraction_description
                    ORDER BY count DESC
                    LIMIT 10
                """)

                park_signed = cur.fetchall()
                print('🎯 DESCRIPTIONS WITH "PARK-SIGNED" or "PROHIBIT":\n')
                if not park_signed:
                    print('  (No exact matches found)\n')
                else:
                    for desc, count in park_signed:
                        print(f'  "{desc}" ({count} tickets)')
                    print('')

                # 5. Show sample tickets with full details
                cur.execute("""
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
                """)

                samples = cur.fetchall()
                print('💳 SAMPLE TICKETS:\n')
                for idx, (ticket, date, code, desc, fine, loc) in enumerate(samples, 1):
                    print(f'{idx}. Ticket #{ticket}')
                    print(f'   Date: {date}')
                    print(f'   Code: {code}')
                    print(f'   Description: {desc}')
                    print(f'   Fine: ${fine}')
                    print(f'   Location: {loc}\n')

    except Exception as err:
        print(f'❌ Error: {err}')

if __name__ == '__main__':
    check_infraction_descriptions()
