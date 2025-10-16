"""List available tables in the database"""
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg

env_path = Path('map-app/.env.local')
load_dotenv(env_path)
DATABASE_URL = os.getenv('TILES_DB_URL')

with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
        tables = cur.fetchall()
        print('Available tables:')
        for table in tables:
            print(f'  - {table[0]}')
