"""Shared Postgres helpers for the LUCID orchestrator."""

from __future__ import annotations

import os
import time
from datetime import datetime

import psycopg2

DB_URL = os.environ["LUCID_DB_URL"]


def connect(url: str | None = None) -> psycopg2.extensions.connection:
    target = url or DB_URL
    for attempt in range(10):
        try:
            return psycopg2.connect(target)
        except psycopg2.OperationalError:
            if attempt == 9:
                raise
            time.sleep(1)
    raise RuntimeError("unreachable")


def init_schema(url: str | None = None) -> None:
    """Ensure orchestrator-owned backbone tables exist on startup."""
    with connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username      TEXT PRIMARY KEY,
                    role          TEXT NOT NULL,
                    created_at    TIMESTAMPTZ
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS users_role_idx ON users(role)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS authn_log (
                    id        BIGSERIAL PRIMARY KEY,
                    ts        TIMESTAMPTZ NOT NULL,
                    username  TEXT,
                    clientid  TEXT NOT NULL,
                    result    TEXT NOT NULL
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS authn_log_ts_idx ON authn_log(ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS authn_log_username_idx ON authn_log(username)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS authz_log (
                    id        BIGSERIAL PRIMARY KEY,
                    ts        TIMESTAMPTZ NOT NULL,
                    username  TEXT,
                    clientid  TEXT NOT NULL,
                    topic     TEXT NOT NULL,
                    action    TEXT NOT NULL,
                    result    TEXT NOT NULL
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS authz_log_ts_idx ON authz_log(ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS authz_log_username_idx ON authz_log(username)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS topic_links (
                    id               TEXT PRIMARY KEY,
                    name             TEXT NOT NULL,
                    source_topic     TEXT NOT NULL,
                    target_topic     TEXT NOT NULL,
                    select_clause    TEXT NOT NULL DEFAULT '*',
                    payload_template TEXT,
                    qos              INTEGER NOT NULL DEFAULT 0,
                    emqx_rule_id     TEXT,
                    enabled          BOOLEAN NOT NULL DEFAULT true,
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS topic_links_enabled_idx ON topic_links(enabled)")
            cur.execute("CREATE INDEX IF NOT EXISTS topic_links_created_at_idx ON topic_links(created_at DESC)")
        conn.commit()


def upsert_agent(conn: psycopg2.extensions.connection, agent_id: str, ts: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agents (agent_id, first_seen_ts, last_seen_ts)
            VALUES (%s, %s, %s)
            ON CONFLICT (agent_id) DO UPDATE SET last_seen_ts = EXCLUDED.last_seen_ts
            """,
            (agent_id, ts, ts),
        )


def ensure_agent(conn: psycopg2.extensions.connection, agent_id: str, ts: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agents (agent_id, first_seen_ts, last_seen_ts)
            VALUES (%s, %s, %s)
            ON CONFLICT (agent_id) DO NOTHING
            """,
            (agent_id, ts, ts),
        )


def upsert_component(
    conn: psycopg2.extensions.connection,
    agent_id: str,
    component_id: str,
    ts: datetime,
) -> None:
    upsert_agent(conn, agent_id, ts)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO components (agent_id, component_id, first_seen_ts, last_seen_ts)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (agent_id, component_id) DO UPDATE SET last_seen_ts = EXCLUDED.last_seen_ts
            """,
            (agent_id, component_id, ts, ts),
        )


def ensure_component(
    conn: psycopg2.extensions.connection,
    agent_id: str,
    component_id: str,
    ts: datetime,
) -> None:
    ensure_agent(conn, agent_id, ts)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO components (agent_id, component_id, first_seen_ts, last_seen_ts)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (agent_id, component_id) DO NOTHING
            """,
            (agent_id, component_id, ts, ts),
        )


def upsert_user_metadata(
    conn: psycopg2.extensions.connection,
    username: str,
    role: str,
    created_at: datetime | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (username, role, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (username) DO UPDATE SET
                role = EXCLUDED.role
            """,
            (username, role, created_at),
        )


def delete_user_metadata(conn: psycopg2.extensions.connection, username: str) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM users WHERE username = %s", (username,))
        return cur.rowcount


def get_user_role(conn: psycopg2.extensions.connection, username: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT role FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    return row[0] if row else None


def purge_agent_data(conn: psycopg2.extensions.connection, agent_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT agent_id FROM agents WHERE agent_id = %s", (agent_id,))
        if cur.fetchone() is None:
            return False

        for table_name in (
            "component_cfg_telemetry",
            "component_cfg_logging",
            "component_cfg",
            "component_metadata",
            "component_state",
            "component_status",
            "component_events",
            "component_telemetry",
            "logs",
            "commands",
            "agent_cfg_telemetry",
            "agent_cfg_logging",
            "agent_cfg",
            "agent_metadata",
            "agent_state",
            "agent_status",
            "agent_telemetry",
            "agent_events",
            "client_events",
            "mqtt_rejected_messages",
            "components",
            "agents",
        ):
            cur.execute(f"DELETE FROM {table_name} WHERE agent_id = %s", (agent_id,))
    return True
