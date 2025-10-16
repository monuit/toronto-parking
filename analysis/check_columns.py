import os
from dotenv import load_dotenv
import psycopg

load_dotenv(r"c:\Users\boredbedouin\Desktop\toronto-parking\map-app\.env.local")
conn = psycopg.connect(os.getenv("TILES_DB_URL"))
cursor = conn.cursor()

# Get columns
cursor.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'ase_camera_locations'
    ORDER BY ordinal_position;
""")

print("ASE_CAMERA_LOCATIONS Columns:")
print("=" * 50)
for col in cursor.fetchall():
    print(f"  {col[0]:<30} {col[1]}")

conn.close()
