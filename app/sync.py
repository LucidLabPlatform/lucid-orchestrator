from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from app import db as DB
from app.auth_service import AuthServiceError

log = logging.getLogger(__name__)

MQTT_USERS_DOMAIN = "mqtt-users"
TOPIC_LINKS_DOMAIN = "topic-links"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def sync_mqtt_users(app, strict: bool = False) -> dict:
    synced_at = _now()
    try:
        snapshot = app.state.auth.get_mqtt_state()
    except AuthServiceError as exc:
        with DB.connect() as conn:
            DB.set_sync_state(conn, MQTT_USERS_DOMAIN, status="error", error=str(exc))
            conn.commit()
        if strict:
            raise
        log.warning("MQTT user sync failed: %s", exc)
        return {"ok": False, "error": str(exc)}

    principals = snapshot.get("principals", [])
    acl_rules = snapshot.get("acl_rules", [])

    with DB.connect() as conn:
        DB.replace_mqtt_shadow(conn, principals, acl_rules, synced_at)
        for principal in principals:
            if principal.get("role") == "agent":
                DB.ensure_agent(conn, principal["username"], synced_at)
        DB.set_sync_state(conn, MQTT_USERS_DOMAIN, status="synced", synced_at=synced_at, error=None)
        conn.commit()

    return {"ok": True, "principals": len(principals), "acl_rules": len(acl_rules)}


def sync_topic_links(app, strict: bool = False) -> dict:
    synced_at = _now()
    try:
        remote_links = app.state.tlm.list_links()
    except Exception as exc:  # noqa: BLE001
        with DB.connect() as conn:
            DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="error", error=str(exc))
            conn.commit()
        if strict:
            raise
        log.warning("Topic-link sync failed: %s", exc)
        return {"ok": False, "error": str(exc)}

    with DB.connect() as conn:
        local_links = DB.list_topic_links(conn)
        local_by_rule_id = {row["emqx_rule_id"]: row for row in local_links if row.get("emqx_rule_id")}
        seen_rule_ids: set[str] = set()
        created = 0
        updated = 0
        deleted = 0

        with conn.cursor() as cur:
            for link in remote_links:
                rule_id = link["emqx_rule_id"]
                seen_rule_ids.add(rule_id)
                existing = local_by_rule_id.get(rule_id)
                if existing is None:
                    cur.execute(
                        """
                        INSERT INTO topic_links (
                            id, name, source_topic, target_topic, select_clause,
                            payload_template, qos, emqx_rule_id, enabled,
                            created_at, updated_at, last_synced_at, sync_status, last_error,
                            owner_type, owner_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'synced', NULL, 'manual', NULL)
                        """,
                        (
                            str(uuid.uuid4()),
                            link["name"],
                            link["source_topic"],
                            link["target_topic"],
                            link["select_clause"],
                            link["payload_template"],
                            link["qos"],
                            link["emqx_rule_id"],
                            link["enabled"],
                            synced_at,
                            synced_at,
                            synced_at,
                        ),
                    )
                    created += 1
                    continue

                changed = any(
                    existing[field] != link[field]
                    for field in (
                        "name",
                        "source_topic",
                        "target_topic",
                        "select_clause",
                        "payload_template",
                        "qos",
                        "enabled",
                    )
                )
                cur.execute(
                    """
                    UPDATE topic_links
                    SET name = %s,
                        source_topic = %s,
                        target_topic = %s,
                        select_clause = %s,
                        payload_template = %s,
                        qos = %s,
                        enabled = %s,
                        updated_at = %s,
                        last_synced_at = %s,
                        sync_status = 'synced',
                        last_error = NULL
                    WHERE id = %s
                    """,
                    (
                        link["name"],
                        link["source_topic"],
                        link["target_topic"],
                        link["select_clause"],
                        link["payload_template"],
                        link["qos"],
                        link["enabled"],
                        synced_at if changed else existing["updated_at"],
                        synced_at,
                        existing["id"],
                    ),
                )
                if changed:
                    updated += 1

            for row in local_links:
                rule_id = row.get("emqx_rule_id")
                if not rule_id:
                    continue
                if rule_id in seen_rule_ids:
                    continue
                cur.execute("DELETE FROM topic_links WHERE id = %s", (row["id"],))
                deleted += 1

        DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="synced", synced_at=synced_at, error=None)
        conn.commit()

    return {"ok": True, "created": created, "updated": updated, "deleted": deleted}


async def sync_forever(app, interval_s: float = 5.0) -> None:
    while True:
        await asyncio.to_thread(sync_mqtt_users, app, False)
        await asyncio.to_thread(sync_topic_links, app, False)
        await asyncio.sleep(interval_s)
