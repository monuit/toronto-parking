#!/usr/bin/env python3
"""
Export all distinct infraction descriptions with codes and counts to CSV and JSON
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

def export_infraction_descriptions():
    try:
        print('📡 Connecting to database...')

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                # Query all distinct infraction codes and descriptions
                cur.execute("""
                    SELECT
                        infraction_code,
                        infraction_description,
                        COUNT(*) as ticket_count,
                        ROUND(AVG(set_fine_amount::numeric), 2) as avg_fine,
                        MIN(set_fine_amount::numeric) as min_fine,
                        MAX(set_fine_amount::numeric) as max_fine,
                        COUNT(DISTINCT date_of_infraction::text) as days_with_infraction,
                        MIN(date_of_infraction) as first_ticket_date,
                        MAX(date_of_infraction) as last_ticket_date
                    FROM parking_tickets
                    WHERE infraction_description IS NOT NULL AND infraction_description != ''
                    GROUP BY infraction_code, infraction_description
                    ORDER BY ticket_count DESC
                """)

                rows = cur.fetchall()

                print(f'📊 Found {len(rows)} distinct infraction descriptions\n')

                # Export to CSV
                csv_file = 'infraction_descriptions.csv'
                print(f'💾 Exporting to {csv_file}...')

                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'Infraction Code',
                        'Description',
                        'Ticket Count',
                        'Average Fine',
                        'Min Fine',
                        'Max Fine',
                        'Days with Infraction',
                        'First Ticket Date',
                        'Last Ticket Date'
                    ])

                    for row in rows:
                        writer.writerow(row)

                print(f'   ✅ {csv_file} created ({len(rows)} rows)\n')

                # Export to JSON
                json_file = 'infraction_descriptions.json'
                print(f'💾 Exporting to {json_file}...')

                json_data = {
                    'metadata': {
                        'total_distinct_infractions': len(rows),
                        'export_date': str(psycopg.sql.SQL("SELECT NOW()").as_string(conn))
                    },
                    'infractions': [
                        {
                            'code': row[0],
                            'description': row[1],
                            'ticket_count': row[2],
                            'avg_fine': float(row[3]) if row[3] else None,
                            'min_fine': float(row[4]) if row[4] else None,
                            'max_fine': float(row[5]) if row[5] else None,
                            'days_with_infraction': row[6],
                            'first_ticket_date': str(row[7]) if row[7] else None,
                            'last_ticket_date': str(row[8]) if row[8] else None,
                        }
                        for row in rows
                    ]
                }

                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)

                print(f'   ✅ {json_file} created ({len(rows)} rows)\n')

                # Print summary
                print('📈 SUMMARY:\n')
                total_tickets = sum(row[2] for row in rows)
                total_revenue = sum(row[2] * (row[3] or 0) for row in rows)

                print(f'  Total distinct infractions: {len(rows)}')
                print(f'  Total tickets: {total_tickets:,}')
                print(f'  Total estimated revenue: ${total_revenue:,.2f}\n')

                print('🏆 TOP 10 MOST COMMON:\n')
                for idx, row in enumerate(rows[:10], 1):
                    code, desc, count, avg_fine, min_fine, max_fine, days, first, last = row
                    print(f'{idx}. [{code}] {desc}')
                    print(f'   Count: {count:,} | Avg Fine: ${avg_fine} | Range: ${min_fine}-${max_fine}')
                    print('')

    except Exception as err:
        print(f'❌ Error: {err}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    export_infraction_descriptions()
