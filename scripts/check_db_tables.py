"""Check tables in Railway database."""
import psycopg2

RAILWAY_URL = "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway?sslmode=require"

conn = psycopg2.connect(RAILWAY_URL)
cur = conn.cursor()

cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
""")

tables = [r[0] for r in cur.fetchall()]
print("Tables in Railway DB:")
for t in tables:
    print(f"  {t}")
print(f"\nTotal: {len(tables)} tables")

conn.close()
