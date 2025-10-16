"""Check available years in ASE data"""
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

env_path = Path('map-app/.env.local')
load_dotenv(env_path)
DATABASE_URL = os.getenv('TILES_DB_URL')

with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor(row_factory=dict_row) as cur:
        # Check ASE camera locations
        cur.execute("""
            SELECT location, years, ticket_count FROM ase_camera_locations
            WHERE ticket_count > 0
            LIMIT 10
        """)
        results = cur.fetchall()
        print("ASE Camera Locations (top by ticket count):")
        for row in results:
            print(f"  {row['location']}: years={row['years']}, tickets={row['ticket_count']}")
