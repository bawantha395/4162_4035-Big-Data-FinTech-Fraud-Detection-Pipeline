"""
Database helpers for BankSecure Pipeline
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.settings import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def fetch_all(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fetch_one(sql: str, params: tuple = ()) -> dict[str, Any] | None:
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


def execute(sql: str, params: tuple = ()) -> None:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()
    conn.close()


def execute_many(sql: str, param_list: list[tuple]) -> int:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.executemany(sql, param_list)
        count = cur.rowcount
    conn.commit()
    conn.close()
    return count
