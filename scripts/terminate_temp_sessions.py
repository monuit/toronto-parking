from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = PROJECT_ROOT / ".env"


def load_env() -> None:
    if DOTENV_PATH.exists():
        load_dotenv(DOTENV_PATH)


def resolve_dsn() -> str:
    candidates = (
        os.getenv("DATABASE_PRIVATE_URL"),
        os.getenv("DATABASE_URL"),
        os.getenv("DATABASE_PUBLIC_URL"),
        os.getenv("POSTGRES_URL"),
    )
    for dsn in candidates:
        if not dsn:
            continue
        try:
            with psycopg.connect(dsn, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return dsn
        except Exception:
            continue
    raise RuntimeError("Unable to resolve a working Postgres DSN")


def fetch_sessions(conn: psycopg.Connection, min_age: dt.timedelta) -> list[dict]:
    query = """
        WITH temp_schemas AS (
            SELECT
                n.nspname,
                substring(n.nspname FROM 'pg_temp_(\\d+)')::int AS backend_id,
                COUNT(c.oid) AS relcount,
                COALESCE(SUM(pg_total_relation_size(c.oid)), 0) AS total_bytes
            FROM pg_namespace n
            LEFT JOIN pg_class c ON c.relnamespace = n.oid
            WHERE n.nspname LIKE 'pg_temp_%%'
            GROUP BY n.nspname
        )
        SELECT
            a.pid,
            a.usename,
            a.application_name,
            a.client_addr,
            a.state,
            a.state_change,
            a.backend_start,
            a.xact_start,
            a.wait_event_type,
            a.wait_event,
            temp.nspname AS temp_schema,
            temp.relcount AS temp_relcount,
            temp.total_bytes AS temp_bytes
        FROM pg_stat_activity a
        LEFT JOIN LATERAL (
            SELECT t.nspname, t.relcount, t.total_bytes
            FROM temp_schemas t
            WHERE pg_stat_get_backend_pid(t.backend_id) = a.pid
            LIMIT 1
        ) AS temp ON TRUE
        WHERE a.datname = current_database()
          AND a.pid <> pg_backend_pid()
          AND (
                temp.nspname IS NOT NULL
                OR a.state = 'idle in transaction'
                OR a.state = 'idle in transaction (aborted)'
          )
          AND COALESCE(a.state_change, a.xact_start, a.backend_start, now()) <= (clock_timestamp() - %s)
        ORDER BY a.state_change ASC;
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (min_age,))
        return cur.fetchall()


def terminate_sessions(conn: psycopg.Connection, pids: list[int]) -> list[tuple[int, bool]]:
    if not pids:
        return []
    results: list[tuple[int, bool]] = []
    with conn.cursor() as cur:
        for pid in pids:
            try:
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                terminated = cur.fetchone()[0]
                results.append((pid, bool(terminated)))
            except Exception:
                results.append((pid, False))
    conn.commit()
    return results


def format_bytes(num: int | None) -> str:
    if not num:
        return "0B"
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    value = float(num)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}TB"


def main() -> None:
    parser = argparse.ArgumentParser(description="Terminate lingering Postgres temp-table sessions")
    parser.add_argument(
        "--min-age",
        type=int,
        default=10,
        help="Minimum age in minutes since last state change before termination (default: 10)",
    )
    parser.add_argument(
        "--terminate",
        action="store_true",
        help="Terminate matching sessions (otherwise just report)",
    )
    args = parser.parse_args()

    load_env()
    dsn = resolve_dsn()
    min_age = dt.timedelta(minutes=args.min_age)

    with psycopg.connect(dsn) as conn:
        sessions = fetch_sessions(conn, min_age)
        if not sessions:
            print("No sessions matched the criteria.")
            return

        now_ts = dt.datetime.now(dt.timezone.utc)
        for session in sessions:
            state_change = session.get("state_change") or session.get("backend_start")
            if isinstance(state_change, dt.datetime) and state_change.tzinfo is None:
                state_change = state_change.replace(tzinfo=dt.timezone.utc)
            age = now_ts - state_change if isinstance(state_change, dt.datetime) else dt.timedelta(0)
            print(
                f"PID={session['pid']} user={session['usename']} state={session['state']} "
                f"age={age} temp_schema={session.get('temp_schema')} rels={session.get('temp_relcount') or 0} "
                f"size={format_bytes(session.get('temp_bytes'))}"
            )
        if args.terminate:
            terminated = terminate_sessions(conn, [row["pid"] for row in sessions])
            for pid, success in terminated:
                status = "terminated" if success else "failed"
                print(f"PID {pid}: {status}")
        else:
            print("Run with --terminate to kill these sessions.")


if __name__ == "__main__":
    main()
