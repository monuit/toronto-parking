#!/usr/bin/env python3
"""
Export infraction codes by regions/streets with counts, fines, and dates
"""

import os
import csv
import json
import psycopg
from dotenv import load_dotenv

load_dotenv('map-app/.env.local')

# Try multiple env var names
database_url = os.getenv('DATABASE_URL') or os.getenv('TILES_DB_URL') or os.getenv('CORE_DB_URL')
if not database_url:
    print('❌ DATABASE_URL, TILES_DB_URL, or CORE_DB_URL not found')
    exit(1)

def export_infractions_by_location():
    try:
        print('📡 Connecting to database...')

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                # Query infraction codes grouped by street_normalized
                # street_normalized is the geocoded street name
                cur.execute("""
                    SELECT
                        street_normalized,
                        infraction_code,
                        COUNT(*) as ticket_count,
                        ROUND(AVG(set_fine_amount::numeric), 2) as avg_fine,
                        CAST(MIN(set_fine_amount::numeric) AS DECIMAL(10, 2)) as min_fine,
                        CAST(MAX(set_fine_amount::numeric) AS DECIMAL(10, 2)) as max_fine,
                        MAX(date_of_infraction) as last_ticket_date
                    FROM parking_tickets
                    WHERE street_normalized IS NOT NULL
                      AND street_normalized != ''
                      AND infraction_code IS NOT NULL
                    GROUP BY street_normalized, infraction_code
                    ORDER BY street_normalized, ticket_count DESC
                """)

                rows = cur.fetchall()

                print(f'📊 Found {len(rows)} street/code combinations\n')

                # Export to CSV
                csv_file = 'infractions_by_street.csv'
                print(f'💾 Exporting to {csv_file}...')

                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'Street',
                        'Infraction Code',
                        'Ticket Count',
                        'Avg Fine',
                        'Min Fine',
                        'Max Fine',
                        'Last Ticket Date'
                    ])

                    for row in rows:
                        writer.writerow(row)

                print(f'   ✅ {csv_file} created ({len(rows)} rows)\n')

                # Export to JSON
                json_file = 'infractions_by_street.json'
                print(f'💾 Exporting to {json_file}...')

                json_data = {
                    'total_combinations': len(rows),
                    'infractions_by_street': [
                        {
                            'street': row[0],
                            'code': row[1],
                            'ticket_count': row[2],
                            'avg_fine': float(row[3]) if row[3] else None,
                            'min_fine': float(row[4]) if row[4] else None,
                            'max_fine': float(row[5]) if row[5] else None,
                            'last_ticket_date': str(row[6]) if row[6] else None,
                        }
                        for row in rows
                    ]
                }

                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)

                print(f'   ✅ {json_file} created ({len(rows)} rows)\n')

                # Get summary stats
                cur.execute("""
                    SELECT COUNT(DISTINCT street_normalized) as unique_streets
                    FROM parking_tickets
                    WHERE street_normalized IS NOT NULL AND street_normalized != ''
                """)
                unique_streets = cur.fetchone()[0]

                print(f'📈 SUMMARY:\n')
                print(f'  Total street/code combinations: {len(rows):,}')
                print(f'  Unique streets: {unique_streets:,}\n')

                # Show top 20 combinations
                print('🏆 TOP 20 STREET/CODE COMBINATIONS:\n')
                for idx, (street, code, count, avg_fine, min_fine, max_fine, last_date) in enumerate(rows[:20], 1):
                    print(f'{idx}. Street: {street} | Code: {code}')
                    print(f'   Tickets: {count:,} | Avg Fine: ${avg_fine} | Range: ${min_fine}-${max_fine} | Last: {last_date}')
                    print('')

    except Exception as err:
        print(f'❌ Error: {err}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    export_infractions_by_location()
