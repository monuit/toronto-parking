"""Comprehensive Railway DB verification - compare CSV files to DB tables."""
import psycopg2
import os
import csv
import sys

csv.field_size_limit(sys.maxsize)

RAILWAY_URL = "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway?sslmode=require"
CSV_DIR = r"F:\Coding\toronto-parking\data_export\public"


def count_csv_rows(filepath):
    """Count actual CSV rows (excluding header), handling multi-line fields correctly."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            return sum(1 for _ in reader)
    except:
        return -1


def main():
    conn = psycopg2.connect(RAILWAY_URL)
    cur = conn.cursor()

    # Get all CSV files
    csv_files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
    csv_tables = {f.replace('.csv', ''): f for f in csv_files}

    # Get all tables from DB with row counts
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    db_tables = [r[0] for r in cur.fetchall()]

    print("=" * 100)
    print("Railway PostGIS - Full Verification")
    print("=" * 100)
    print()

    # Compare
    results = []
    for table in sorted(set(list(csv_tables.keys()) + db_tables)):
        csv_file = csv_tables.get(table)
        in_db = table in db_tables

        csv_count = 0
        db_count = 0
        status = ""

        if csv_file:
            csv_path = os.path.join(CSV_DIR, csv_file)
            csv_count = count_csv_rows(csv_path)

        if in_db:
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                db_count = cur.fetchone()[0]
            except Exception as e:
                db_count = -1
                status = f"ERROR: {e}"

        if csv_file and in_db:
            if csv_count == db_count:
                status = "✓ OK"
            elif db_count == 0 and csv_count > 0:
                status = "⚠ EMPTY"
            elif db_count > 0 and csv_count > db_count:
                diff = csv_count - db_count
                pct = (diff / csv_count) * 100
                status = f"⚠ MISSING {diff:,} ({pct:.1f}%)"
            elif db_count > csv_count:
                status = "⚠ EXTRA ROWS"
            else:
                status = "⚠ MISMATCH"
        elif csv_file and not in_db:
            status = "❌ NOT IN DB"
        elif in_db and not csv_file:
            status = "ℹ DB ONLY"

        results.append((table, csv_count, db_count, status))

    # Print results
    print(f"{'Table':<45} {'CSV Rows':>15} {'DB Rows':>15} {'Status':<30}")
    print("-" * 100)

    issues = []
    for table, csv_count, db_count, status in results:
        csv_str = f"{csv_count:,}" if csv_count >= 0 else "N/A"
        db_str = f"{db_count:,}" if db_count >= 0 else "N/A"
        print(f"{table:<45} {csv_str:>15} {db_str:>15} {status:<30}")

        if "EMPTY" in status or "MISSING" in status or "NOT IN DB" in status:
            issues.append((table, csv_count, db_count, status))

    print("-" * 100)

    # Summary
    print()
    if issues:
        print(f"⚠ Found {len(issues)} tables with issues:")
        for table, csv_count, db_count, status in issues:
            print(f"  - {table}: {status}")
    else:
        print("✓ All tables verified successfully!")

    conn.close()


if __name__ == "__main__":
    main()
