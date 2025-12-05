"""Reimport tables with missing rows using robust error handling."""
import psycopg2
import os
import csv
import sys
from io import StringIO

csv.field_size_limit(sys.maxsize)

RAILWAY_URL = "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway?sslmode=require"
CSV_DIR = r"F:\Coding\toronto-parking\data_export\public"

TABLES_TO_FIX = [
    "parking_ticket_yearly_locations",
]


def get_connection():
    """Get a fresh database connection."""
    return psycopg2.connect(RAILWAY_URL)


def get_csv_header(table_name):
    """Get CSV header columns."""
    csv_path = os.path.join(CSV_DIR, f"{table_name}.csv")
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        return next(reader)


def import_with_batch(conn, table_name, batch_size=50000):
    """Import table in batches to handle errors gracefully."""
    csv_path = os.path.join(CSV_DIR, f"{table_name}.csv")

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        columns = ', '.join([f'"{col}"' for col in header])

        total_imported = 0
        batch = []
        batch_num = 0

        for row in reader:
            batch.append(row)

            if len(batch) >= batch_size:
                batch_num += 1
                imported = import_batch(conn, table_name, header, batch)
                total_imported += imported
                print(
                    f"    Batch {batch_num}: imported {imported}/{len(batch)} rows (total: {total_imported:,})")
                batch = []

        # Final batch
        if batch:
            batch_num += 1
            imported = import_batch(conn, table_name, header, batch)
            total_imported += imported
            print(
                f"    Batch {batch_num}: imported {imported}/{len(batch)} rows (total: {total_imported:,})")

        return total_imported


def import_batch(conn, table_name, header, rows):
    """Import a batch of rows using COPY."""
    columns = ', '.join([f'"{col}"' for col in header])

    # Build CSV string
    output = StringIO()
    writer = csv.writer(output)
    for row in rows:
        writer.writerow(row)
    output.seek(0)

    cur = conn.cursor()
    try:
        cur.copy_expert(
            f'COPY "{table_name}" ({columns}) FROM STDIN WITH CSV',
            output
        )
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        print(f"      Error in batch: {e}")
        # Try row by row
        return import_row_by_row(conn, table_name, header, rows)


def import_row_by_row(conn, table_name, header, rows):
    """Import rows one by one to find problematic rows."""
    columns = ', '.join([f'"{col}"' for col in header])
    placeholders = ', '.join(['%s'] * len(header))

    cur = conn.cursor()
    imported = 0

    for row in rows:
        try:
            cur.execute(
                f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})',
                row
            )
            conn.commit()
            imported += 1
        except Exception as e:
            conn.rollback()
            # Skip problematic row silently
            pass

    return imported


def main():
    print("=" * 60)
    print("Railway PostGIS - Reimport Missing Rows")
    print("=" * 60)

    for table in TABLES_TO_FIX:
        print(f"\n[{table}]")

        # Get current count
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        current_count = cur.fetchone()[0]

        # Count CSV rows
        csv_path = os.path.join(CSV_DIR, f"{table}.csv")
        with open(csv_path, 'r', encoding='utf-8') as f:
            csv_count = sum(1 for _ in f) - 1

        print(f"  Current DB rows: {current_count:,}")
        print(f"  CSV rows: {csv_count:,}")
        print(f"  Missing: {csv_count - current_count:,}")

        if current_count >= csv_count:
            print(f"  ✓ Table is complete")
            conn.close()
            continue

        # Truncate and reimport
        print(f"  Truncating table...")
        cur.execute(f'TRUNCATE TABLE "{table}"')
        conn.commit()
        conn.close()

        print(f"  Reimporting with batch processing...")
        conn = get_connection()
        imported = import_with_batch(conn, table)
        conn.close()

        print(f"  ✓ Imported {imported:,} rows")

        # Verify
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        final_count = cur.fetchone()[0]
        conn.close()

        if final_count == csv_count:
            print(f"  ✓ All rows imported successfully!")
        else:
            print(f"  ⚠ Still missing {csv_count - final_count:,} rows")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
