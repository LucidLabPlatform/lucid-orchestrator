from __future__ import annotations

import uuid
from datetime import datetime, timezone

import psycopg2.extras

from app import db as DB
from app.sync import TOPIC_LINKS_DOMAIN
from app.topic_links.manager import TopicLinkDef

ACTIVE_RUN_STATUSES = {"pending", "running"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _lock_error(row: dict) -> RuntimeError:
    owner_id = row.get("owner_id") or "unknown"
    return RuntimeError(f"Topic link is owned by active experiment run '{owner_id}'")


def _fetch_link(conn, link_id: str) -> dict | None:
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


def _fetch_owned_links(conn, owner_type: str, owner_id: str) -> list[dict]:
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
            WHERE tl.owner_type = %s AND tl.owner_id = %s
            ORDER BY tl.created_at DESC, tl.id DESC
            """,
            (owner_type, owner_id),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def create_topic_link(
    app,
    *,
    name: str,
    source_topic: str,
    target_topic: str,
    select_clause: str = "*",
    payload_template: str | None = None,
    qos: int = 0,
    owner_type: str = "manual",
    owner_id: str | None = None,
    link_id: str | None = None,
) -> dict:
    row_id = link_id or str(uuid.uuid4())
    created_at = _now()
    link_def = TopicLinkDef(
        name=name,
        source_topic=source_topic,
        target_topic=target_topic,
        select_clause=select_clause,
        payload_template=payload_template,
        qos=qos,
    )
    rule_id = app.state.tlm.create_link(link_def)

    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO topic_links (
                    id, name, source_topic, target_topic, select_clause,
                    payload_template, qos, emqx_rule_id, enabled, created_at,
                    updated_at, last_synced_at, sync_status, last_error,
                    owner_type, owner_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row_id,
                    name,
                    source_topic,
                    target_topic,
                    select_clause,
                    payload_template,
                    qos,
                    rule_id,
                    True,
                    created_at,
                    created_at,
                    created_at,
                    "synced",
                    None,
                    owner_type,
                    owner_id,
                ),
            )
            DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="synced", synced_at=created_at, error=None)
        conn.commit()

        row = _fetch_link(conn, row_id)
    if row is None:
        raise RuntimeError("Created topic link could not be reloaded")
    return row


def activate_topic_link(app, link_id: str, *, ignore_lock: bool = False) -> dict:
    with DB.connect() as conn:
        row = _fetch_link(conn, link_id)
        if row is None:
            raise LookupError("Topic link not found")
        if row.get("read_only") and not ignore_lock:
            raise _lock_error(row)
        if not row.get("emqx_rule_id"):
            raise RuntimeError("Topic link has no EMQX rule ID")

    ts = _now()
    app.state.tlm.activate_link(row["emqx_rule_id"])

    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE topic_links
                SET enabled = true,
                    updated_at = %s,
                    last_synced_at = %s,
                    sync_status = 'synced',
                    last_error = NULL
                WHERE id = %s
                """,
                (ts, ts, link_id),
            )
            DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="synced", synced_at=ts, error=None)
        conn.commit()
        updated = _fetch_link(conn, link_id)
    return updated or row


def deactivate_topic_link(app, link_id: str, *, ignore_lock: bool = False) -> dict:
    with DB.connect() as conn:
        row = _fetch_link(conn, link_id)
        if row is None:
            raise LookupError("Topic link not found")
        if row.get("read_only") and not ignore_lock:
            raise _lock_error(row)
        if not row.get("emqx_rule_id"):
            raise RuntimeError("Topic link has no EMQX rule ID")

    ts = _now()
    app.state.tlm.deactivate_link(row["emqx_rule_id"])

    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE topic_links
                SET enabled = false,
                    updated_at = %s,
                    last_synced_at = %s,
                    sync_status = 'synced',
                    last_error = NULL
                WHERE id = %s
                """,
                (ts, ts, link_id),
            )
            DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="synced", synced_at=ts, error=None)
        conn.commit()
        updated = _fetch_link(conn, link_id)
    return updated or row


def delete_topic_link(app, link_id: str, *, ignore_lock: bool = False) -> dict:
    with DB.connect() as conn:
        row = _fetch_link(conn, link_id)
        if row is None:
            raise LookupError("Topic link not found")
        if row.get("read_only") and not ignore_lock:
            raise _lock_error(row)

    rule_id = row.get("emqx_rule_id")
    if rule_id:
        app.state.tlm.delete_link(rule_id)

    ts = _now()
    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM topic_links WHERE id = %s", (link_id,))
            DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="synced", synced_at=ts, error=None)
        conn.commit()
    return row


def delete_owned_topic_links(app, owner_type: str, owner_id: str) -> list[dict]:
    removed: list[dict] = []
    with DB.connect() as conn:
        rows = _fetch_owned_links(conn, owner_type, owner_id)
    for row in rows:
        try:
            removed.append(delete_topic_link(app, row["id"], ignore_lock=True))
        except LookupError:
            continue
    return removed


def find_owned_topic_link(
    conn,
    *,
    owner_type: str,
    owner_id: str,
    source_topic: str,
    target_topic: str,
) -> dict | None:
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
            WHERE tl.owner_type = %s
              AND tl.owner_id = %s
              AND tl.source_topic = %s
              AND tl.target_topic = %s
            ORDER BY tl.created_at DESC, tl.id DESC
            LIMIT 1
            """,
            (owner_type, owner_id, source_topic, target_topic),
        )
        row = cur.fetchone()
    return dict(row) if row else None
