#!/usr/bin/env python3
"""
Export only distinct infraction codes and descriptions to CSV and JSON
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

def export_distinct_infractions():
    try:
        print('📡 Connecting to database...')

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                # Query only distinct infraction codes and descriptions
                cur.execute("""
                    SELECT DISTINCT
                        infraction_code,
                        infraction_description
                    FROM parking_tickets
                    WHERE infraction_description IS NOT NULL AND infraction_description != ''
                    ORDER BY infraction_code, infraction_description
                """)

                rows = cur.fetchall()

                print(f'📊 Found {len(rows)} distinct infraction codes/descriptions\n')

                # Export to CSV
                csv_file = 'infraction_codes_descriptions.csv'
                print(f'💾 Exporting to {csv_file}...')

                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Infraction Code', 'Description'])

                    for code, desc in rows:
                        writer.writerow([code, desc])

                print(f'   ✅ {csv_file} created ({len(rows)} rows)\n')

                # Export to JSON
                json_file = 'infraction_codes_descriptions.json'
                print(f'💾 Exporting to {json_file}...')

                json_data = {
                    'total_distinct': len(rows),
                    'infractions': [
                        {
                            'code': code,
                            'description': desc
                        }
                        for code, desc in rows
                    ]
                }

                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)

                print(f'   ✅ {json_file} created ({len(rows)} rows)\n')

                # Print first 20 as preview
                print('🔍 PREVIEW (first 20):\n')
                for idx, (code, desc) in enumerate(rows[:20], 1):
                    print(f'{idx}. [{code}] {desc}')

                if len(rows) > 20:
                    print(f'\n... and {len(rows) - 20} more')

    except Exception as err:
        print(f'❌ Error: {err}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    export_distinct_infractions()
