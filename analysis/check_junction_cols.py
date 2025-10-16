"""Quick check of junction table columns."""

import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg

ENV_PATH = Path(__file__).parent.parent / "map-app" / ".env.local"
load_dotenv(ENV_PATH)

DATABASE_URL = os.getenv("TILES_DB_URL")

async def main():
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        result = await conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'schools_with_nearby_cameras'
            ORDER BY ordinal_position;
        """)
        cols = await result.fetchall()
        print("Columns in schools_with_nearby_cameras:")
        for col in cols:
            print(f"  • {col[0]}")

if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
