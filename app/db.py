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
                    last_error        TEXT,
                    first_seen_ts     TIMESTAMPTZ,
                    last_seen_ts      TIMESTAMPTZ
                )
            """)
            cur.execute("ALTER TABLE mqtt_users ADD COLUMN IF NOT EXISTS first_seen_ts TIMESTAMPTZ")
            cur.execute("ALTER TABLE mqtt_users ADD COLUMN IF NOT EXISTS last_seen_ts  TIMESTAMPTZ")
            # Reclassify obsolete role values before adding the CHECK constraint.
            # Sync will overwrite these on its next pass; this is a transition safeguard.
            cur.execute("""
                UPDATE mqtt_users
                SET role = 'other'
                WHERE role NOT IN ('agent', 'superuser', 'central-command', 'other')
            """)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'mqtt_users_role_check'
                    ) THEN
                        ALTER TABLE mqtt_users
                        ADD CONSTRAINT mqtt_users_role_check
                        CHECK (role IN ('agent', 'superuser', 'central-command', 'other'));
                    END IF;
                END $$;
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

            # authn_log is a TimescaleDB hypertable created by init.sql.
            # CREATE TABLE IF NOT EXISTS would silently no-op on existing hypertables, so
            # we only add missing indexes and the permanent denied tables here.
            cur.execute("CREATE INDEX IF NOT EXISTS authn_log_ts_idx ON authn_log(ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS authn_log_username_idx ON authn_log(username)")
            cur.execute("DROP TABLE IF EXISTS authz_log CASCADE")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS authn_denied (
                    id        BIGSERIAL PRIMARY KEY,
                    ts        TIMESTAMPTZ NOT NULL,
                    username  TEXT,
                    clientid  TEXT NOT NULL,
                    result    TEXT NOT NULL
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS authn_denied_ts_idx ON authn_denied(ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS authn_denied_username_idx ON authn_denied(username)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS authz_denied (
                    id        BIGSERIAL PRIMARY KEY,
                    ts        TIMESTAMPTZ NOT NULL,
                    username  TEXT,
                    clientid  TEXT NOT NULL,
                    topic     TEXT NOT NULL,
                    action    TEXT NOT NULL,
                    result    TEXT NOT NULL
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS authz_denied_ts_idx ON authz_denied(ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS authz_denied_username_idx ON authz_denied(username)")

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
            cur.execute("ALTER TABLE topic_links ADD COLUMN IF NOT EXISTS owner_type TEXT NOT NULL DEFAULT 'manual'")
            cur.execute("ALTER TABLE topic_links ADD COLUMN IF NOT EXISTS owner_id TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS topic_links_enabled_idx ON topic_links(enabled)")
            cur.execute("CREATE INDEX IF NOT EXISTS topic_links_created_at_idx ON topic_links(created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS topic_links_last_synced_idx ON topic_links(last_synced_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS topic_links_owner_idx ON topic_links(owner_type, owner_id)")
            # Drop partial index from previous migration attempt, then create
            # a full unique index. PostgreSQL treats NULL as distinct so
            # multiple NULL emqx_rule_id rows are still allowed.
            cur.execute("DROP INDEX IF EXISTS topic_links_emqx_rule_id_unique")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS topic_links_emqx_rule_id_unique ON topic_links(emqx_rule_id)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS experiment_templates (
                    id                TEXT PRIMARY KEY,
                    name              TEXT NOT NULL,
                    version           TEXT NOT NULL DEFAULT '1.0.0',
                    description       TEXT NOT NULL DEFAULT '',
                    parameters_schema JSONB NOT NULL DEFAULT '{}',
                    definition        JSONB NOT NULL,
                    tags              TEXT[] NOT NULL DEFAULT '{}',
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS experiment_runs (
                    id          TEXT PRIMARY KEY,
                    template_id TEXT NOT NULL REFERENCES experiment_templates,
                    status      TEXT NOT NULL DEFAULT 'pending',
                    parameters  JSONB NOT NULL DEFAULT '{}',
                    started_at  TIMESTAMPTZ,
                    ended_at    TIMESTAMPTZ,
                    error       TEXT,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS experiment_steps (
                    id               BIGSERIAL PRIMARY KEY,
                    run_id           TEXT NOT NULL REFERENCES experiment_runs,
                    step_index       INTEGER NOT NULL,
                    step_name        TEXT NOT NULL,
                    agent_id         TEXT,
                    component_id     TEXT,
                    action           TEXT,
                    request_payload  JSONB,
                    response_payload JSONB,
                    status           TEXT NOT NULL DEFAULT 'pending',
                    attempt          INTEGER NOT NULL DEFAULT 0,
                    started_at       TIMESTAMPTZ,
                    ended_at         TIMESTAMPTZ,
                    duration_ms      INTEGER
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_exp_runs_template ON experiment_runs(template_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_exp_runs_status ON experiment_runs(status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_exp_steps_run_idx ON experiment_steps(run_id, step_index)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    domain         TEXT PRIMARY KEY,
                    status         TEXT NOT NULL DEFAULT 'pending',
                    last_synced_at TIMESTAMPTZ,
                    last_error     TEXT,
                    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            # Phase 4: repoint every FK that pointed to agents(agent_id) onto
            # mqtt_users(username), then drop the agents table. Idempotent —
            # the FK loop yields 0 rows once migrated, DROP TABLE IF EXISTS is
            # a no-op afterwards.
            cur.execute("""
                DO $$
                DECLARE
                    rec RECORD;
                BEGIN
                    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'agents') THEN
                        FOR rec IN
                            SELECT conname, conrelid::regclass::text AS table_name
                            FROM pg_constraint
                            WHERE contype = 'f' AND confrelid = 'agents'::regclass
                        LOOP
                            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',
                                rec.table_name, rec.conname);
                            EXECUTE format(
                                'ALTER TABLE %s ADD CONSTRAINT %I FOREIGN KEY (agent_id) '
                                'REFERENCES mqtt_users(username) ON DELETE CASCADE',
                                rec.table_name, rec.conname
                            );
                        END LOOP;
                        DROP TABLE agents;
                    END IF;
                END $$;
            """)
        conn.commit()


def upsert_component(
    conn: psycopg2.extensions.connection,
    agent_id: str,
    component_id: str,
    ts: datetime,
) -> None:
    """Upsert a components row. Caller must ensure mqtt_users has a role='agent'
    row for ``agent_id`` (the components FK requires it)."""
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
    """Ensure a components row exists. Caller must ensure mqtt_users has the
    role='agent' parent row first (FK constraint)."""
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
    """UPSERT the EMQX user/ACL snapshot into Postgres.

    Uses INSERT ... ON CONFLICT for principals so existing rows survive
    (their first_seen_ts/last_seen_ts and any FK-CASCADE child rows in
    agent_cfg/state/etc. are preserved). Removed principals are deleted
    explicitly, which cascades through the FKs cleanly.
    """
    principals = list(principals)
    acl_rules = list(acl_rules)
    incoming_usernames = [p["username"] for p in principals]

    with conn.cursor() as cur:
        # Drop principals that no longer exist in EMQX (and their cascading
        # cfg/state/metadata via FK ON DELETE CASCADE).
        if incoming_usernames:
            cur.execute(
                "DELETE FROM mqtt_users WHERE username <> ALL(%s)",
                (incoming_usernames,),
            )
        else:
            cur.execute("DELETE FROM mqtt_users")
        # Upsert principals — preserves existing rows (and their child FKs).
        for principal in principals:
            cur.execute(
                """
                INSERT INTO mqtt_users (
                    username, role, has_password_user, created_at, updated_at,
                    last_synced_at, sync_status, last_error
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'synced', NULL)
                ON CONFLICT (username) DO UPDATE SET
                    role              = EXCLUDED.role,
                    has_password_user = EXCLUDED.has_password_user,
                    updated_at        = EXCLUDED.updated_at,
                    last_synced_at    = EXCLUDED.last_synced_at,
                    sync_status       = 'synced',
                    last_error        = NULL
                """,
                (
                    principal["username"],
                    principal["role"],
                    principal.get("has_password_user", True),
                    synced_at,
                    synced_at,
                    synced_at,
                ),
            )
        # ACL rules are simpler: full replace per principal is cheap, and
        # nothing FK-references mqtt_acl_rules beyond mqtt_users.
        cur.execute("DELETE FROM mqtt_acl_rules")
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
            SELECT
                tl.id,
                tl.name,
                tl.source_topic,
                tl.target_topic,
                tl.select_clause,
                tl.payload_template,
                tl.qos,
                tl.emqx_rule_id,
                tl.enabled,
                tl.created_at,
                tl.updated_at,
                tl.last_synced_at,
                tl.sync_status,
                tl.last_error,
                tl.owner_type,
                tl.owner_id,
                er.status AS owner_run_status,
                CASE
                    WHEN tl.owner_type = 'experiment-run' AND er.status IN ('pending', 'running') THEN TRUE
                    ELSE FALSE
                END AS read_only
            FROM topic_links tl
            LEFT JOIN experiment_runs er
              ON tl.owner_type = 'experiment-run'
             AND tl.owner_id = er.id
            ORDER BY tl.created_at DESC, tl.name
            """
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_topic_link(conn: psycopg2.extensions.connection, link_id: str) -> dict | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                tl.id,
                tl.name,
                tl.source_topic,
                tl.target_topic,
                tl.select_clause,
                tl.payload_template,
                tl.qos,
                tl.emqx_rule_id,
                tl.enabled,
                tl.created_at,
                tl.updated_at,
                tl.last_synced_at,
                tl.sync_status,
                tl.last_error,
                tl.owner_type,
                tl.owner_id,
                er.status AS owner_run_status,
                CASE
                    WHEN tl.owner_type = 'experiment-run' AND er.status IN ('pending', 'running') THEN TRUE
                    ELSE FALSE
                END AS read_only
            FROM topic_links tl
            LEFT JOIN experiment_runs er
              ON tl.owner_type = 'experiment-run'
             AND tl.owner_id = er.id
            WHERE tl.id = %s
            """,
            (link_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def mark_active_experiment_runs_failed(
    conn: psycopg2.extensions.connection,
    *,
    ended_at: datetime,
    error: str,
) -> list[str]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id
            FROM experiment_runs
            WHERE status IN ('pending', 'running')
            ORDER BY created_at
            """
        )
        rows = cur.fetchall()
        run_ids = [row["id"] for row in rows]
        if not run_ids:
            return []
        cur.execute(
            """
            UPDATE experiment_runs
            SET status = 'failed',
                ended_at = COALESCE(ended_at, %s),
                error = COALESCE(error, %s)
            WHERE status IN ('pending', 'running')
            """,
            (ended_at, error),
        )
    return run_ids


def mark_running_experiment_steps_cancelled(
    conn: psycopg2.extensions.connection,
    run_id: str,
    *,
    ended_at: datetime,
) -> None:
    """Mark any experiment_steps still in 'running' state for *run_id* as 'cancelled'.

    Called from _abort_run and _cancel_run so zombie step rows don't linger.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE experiment_steps
            SET status = 'cancelled',
                ended_at = COALESCE(ended_at, %s)
            WHERE run_id = %s
              AND status = 'running'
            """,
            (ended_at, run_id),
        )


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
    """Delete time-series and per-agent data not covered by FK CASCADE.

    Most cfg/state/metadata tables CASCADE-delete via the FK to mqtt_users
    when the principal is revoked from EMQX and the sync removes the row.
    This function cleans the rest: hypertables (logs/telemetry/events),
    commands, client_events, and mqtt_rejected_messages.
    """
    with conn.cursor() as cur:
        for table_name in (
            "logs",
            "commands",
            "agent_telemetry",
            "agent_events",
            "component_telemetry",
            "component_events",
            "client_events",
            "mqtt_rejected_messages",
        ):
            cur.execute(f"DELETE FROM {table_name} WHERE agent_id = %s", (agent_id,))
    return True
