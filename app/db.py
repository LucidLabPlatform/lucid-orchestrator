"""Shared Postgres helpers for the LUCID orchestrator."""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Iterable

import psycopg2
import psycopg2.extras

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
                CREATE TABLE IF NOT EXISTS mqtt_users (
                    username          TEXT PRIMARY KEY,
                    role              TEXT NOT NULL,
                    has_password_user BOOLEAN NOT NULL DEFAULT true,
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_synced_at    TIMESTAMPTZ,
                    sync_status       TEXT NOT NULL DEFAULT 'pending',
                    last_error        TEXT
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS mqtt_users_role_idx ON mqtt_users(role)")
            cur.execute("CREATE INDEX IF NOT EXISTS mqtt_users_last_synced_idx ON mqtt_users(last_synced_at DESC)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS mqtt_acl_rules (
                    username        TEXT NOT NULL,
                    priority        INTEGER NOT NULL,
                    topic           TEXT NOT NULL,
                    action          TEXT NOT NULL,
                    permission      TEXT NOT NULL,
                    last_synced_at  TIMESTAMPTZ,
                    PRIMARY KEY (username, priority),
                    FOREIGN KEY (username) REFERENCES mqtt_users(username) ON DELETE CASCADE
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS mqtt_acl_rules_username_idx ON mqtt_acl_rules(username)")

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
            cur.execute("ALTER TABLE topic_links ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()")
            cur.execute("ALTER TABLE topic_links ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE topic_links ADD COLUMN IF NOT EXISTS sync_status TEXT NOT NULL DEFAULT 'pending'")
            cur.execute("ALTER TABLE topic_links ADD COLUMN IF NOT EXISTS last_error TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS topic_links_enabled_idx ON topic_links(enabled)")
            cur.execute("CREATE INDEX IF NOT EXISTS topic_links_created_at_idx ON topic_links(created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS topic_links_last_synced_idx ON topic_links(last_synced_at DESC)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    domain         TEXT PRIMARY KEY,
                    status         TEXT NOT NULL DEFAULT 'pending',
                    last_synced_at TIMESTAMPTZ,
                    last_error     TEXT,
                    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
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


def replace_mqtt_shadow(
    conn: psycopg2.extensions.connection,
    principals: Iterable[dict],
    acl_rules: Iterable[dict],
    synced_at: datetime,
) -> None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT username, created_at FROM mqtt_users")
        existing_rows = cur.fetchall()
    existing_created_at = {row["username"]: row["created_at"] for row in existing_rows}

    with conn.cursor() as cur:
        cur.execute("DELETE FROM mqtt_acl_rules")
        cur.execute("DELETE FROM mqtt_users")
        for principal in principals:
            created_at = existing_created_at.get(principal["username"]) or synced_at
            cur.execute(
                """
                INSERT INTO mqtt_users (
                    username, role, has_password_user, created_at, updated_at,
                    last_synced_at, sync_status, last_error
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'synced', NULL)
                """,
                (
                    principal["username"],
                    principal["role"],
                    principal.get("has_password_user", True),
                    created_at,
                    synced_at,
                    synced_at,
                ),
            )
        for rule in acl_rules:
            cur.execute(
                """
                INSERT INTO mqtt_acl_rules (username, priority, topic, action, permission, last_synced_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    rule["username"],
                    rule["priority"],
                    rule["topic"],
                    rule["action"],
                    rule["permission"],
                    synced_at,
                ),
            )


def list_mqtt_users(
    conn: psycopg2.extensions.connection,
    roles: tuple[str, ...] | None = None,
) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if roles:
            cur.execute(
                """
                SELECT username, role, has_password_user, created_at, updated_at, last_synced_at, sync_status, last_error
                FROM mqtt_users
                WHERE role = ANY(%s)
                ORDER BY role, created_at NULLS LAST, username
                """,
                (list(roles),),
            )
        else:
            cur.execute(
                """
                SELECT username, role, has_password_user, created_at, updated_at, last_synced_at, sync_status, last_error
                FROM mqtt_users
                ORDER BY role, created_at NULLS LAST, username
                """
            )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_mqtt_user(conn: psycopg2.extensions.connection, username: str) -> dict | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT username, role, has_password_user, created_at, updated_at, last_synced_at, sync_status, last_error
            FROM mqtt_users
            WHERE username = %s
            """,
            (username,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def list_topic_links(conn: psycopg2.extensions.connection) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, name, source_topic, target_topic, select_clause,
                   payload_template, qos, emqx_rule_id, enabled, created_at,
                   updated_at, last_synced_at, sync_status, last_error
            FROM topic_links
            ORDER BY created_at DESC, name
            """
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def set_sync_state(
    conn: psycopg2.extensions.connection,
    domain: str,
    *,
    status: str,
    synced_at: datetime | None = None,
    error: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_state (domain, status, last_synced_at, last_error, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (domain) DO UPDATE SET
                status = EXCLUDED.status,
                last_synced_at = EXCLUDED.last_synced_at,
                last_error = EXCLUDED.last_error,
                updated_at = now()
            """,
            (domain, status, synced_at, error),
        )


def get_sync_state(conn: psycopg2.extensions.connection) -> dict[str, dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT domain, status, last_synced_at, last_error, updated_at
            FROM sync_state
            ORDER BY domain
            """
        )
        rows = cur.fetchall()
    return {row["domain"]: dict(row) for row in rows}


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
