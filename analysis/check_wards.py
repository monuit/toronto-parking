"""Check ward values in both tables."""

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
        print("Unique wards in ase_camera_locations:")
        result = await conn.execute("""
            SELECT DISTINCT ward FROM ase_camera_locations
            WHERE ward IS NOT NULL
            ORDER BY ward;
        """)
        rows = await result.fetchall()
        for row in rows:
            print(f"  • {row[0]}")

        print("\nUnique wards in schools_with_nearby_cameras:")
        result2 = await conn.execute("""
            SELECT DISTINCT ward FROM schools_with_nearby_cameras
            WHERE ward IS NOT NULL
            ORDER BY ward;
        """)
        rows2 = await result2.fetchall()
        for row in rows2:
            print(f"  • {row[0]}")

if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
