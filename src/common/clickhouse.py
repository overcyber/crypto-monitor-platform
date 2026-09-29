from __future__ import annotations

from typing import Any

from .config import settings


def client():
    """Return a fresh ClickHouse client. Never cache/share sessions across threads."""
    import clickhouse_connect
    s = settings()
    return clickhouse_connect.get_client(
        host=s.clickhouse_host,
        port=s.clickhouse_port,
        username=s.clickhouse_user,
        password=s.clickhouse_password,
        database=s.clickhouse_db,
        connect_timeout=5,
        send_receive_timeout=30,
    )


def rows(query: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    c = client()
    try:
        result = c.query(query, parameters=parameters or {})
        return list(result.named_results())
    finally:
        try:
            c.close()
        except Exception:
            pass


def command(query: str, parameters: dict[str, Any] | None = None):
    c = client()
    try:
        return c.command(query, parameters=parameters or {})
    finally:
        try:
            c.close()
        except Exception:
            pass


def insert(table: str, data, *, column_names):
    c = client()
    try:
        return c.insert(table, data, column_names=column_names)
    finally:
        try:
            c.close()
        except Exception:
            pass
