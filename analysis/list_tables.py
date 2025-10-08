from __future__ import annotations

import psycopg

DSN = "postgresql://postgres:REDACTED_POSTGRES_PASSWORD@interchange.proxy.rlwy.net:57747/railway"


def main() -> None:
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_name = 'centreline_segments'
                """
            )
            print(cur.fetchall())


if __name__ == "__main__":
    main()
