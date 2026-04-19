"""Periodic heartbeat checker — marks agents offline when they go stale.

Runs as an asyncio background task. Every `check_interval_s` seconds it
queries the DB for agents whose `last_seen_ts` exceeds `timeout_s` but
whose status is still "online". For each stale agent it:

1. Updates `agent_status.state` to "offline" in the database.
2. Broadcasts a synthetic MQTT-style status event to all WebSocket clients
   so the dashboard updates in real time.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import psycopg2.extras

from app import db as DB
from app.ws_manager import WebSocketManager

log = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _find_stale_agents(conn, timeout_s: float) -> list[str]:
    """Return agent_ids that are online but haven't been seen within timeout_s."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.agent_id
            FROM agents a
            JOIN agent_status s ON s.agent_id = a.agent_id
            WHERE s.state = 'online'
              AND a.last_seen_ts < NOW() - INTERVAL '%s seconds'
            """,
            (timeout_s,),
        )
        return [row[0] for row in cur.fetchall()]


def _mark_agent_offline(conn, agent_id: str, ts: datetime) -> None:
    """Set agent_status.state to 'offline' for the given agent."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE agent_status
            SET state = 'offline', received_ts = %s
            WHERE agent_id = %s AND state != 'offline'
            """,
            (ts, agent_id),
        )


async def heartbeat_checker(
    ws_mgr: WebSocketManager,
    *,
    timeout_s: float = 90.0,
    check_interval_s: float = 15.0,
) -> None:
    """Background task that marks stale agents as offline.

    Args:
        ws_mgr: WebSocketManager for broadcasting status changes.
        timeout_s: Seconds since last_seen before an agent is considered stale.
                   Default 90s (3x a typical 30s heartbeat).
        check_interval_s: How often to run the check. Default 15s.
    """
    log.info(
        "Heartbeat checker started (timeout=%ss, interval=%ss)",
        timeout_s,
        check_interval_s,
    )
    while True:
        try:
            await asyncio.sleep(check_interval_s)

            with DB.connect() as conn:
                stale = _find_stale_agents(conn, timeout_s)
                if not stale:
                    continue

                ts = _now()
                for agent_id in stale:
                    _mark_agent_offline(conn, agent_id, ts)
                    log.info("Agent '%s' marked offline (heartbeat timeout)", agent_id)
                conn.commit()

            # Broadcast status changes to WebSocket clients
            for agent_id in stale:
                await ws_mgr.broadcast({
                    "type": "mqtt",
                    "topic": f"lucid/agents/{agent_id}/status",
                    "agent_id": agent_id,
                    "component_id": None,
                    "topic_type": "status",
                    "scope": "agent",
                    "payload": {"state": "offline"},
                    "ts": ts.isoformat(),
                })

        except asyncio.CancelledError:
            log.info("Heartbeat checker stopped")
            return
        except Exception as exc:
            log.exception("Heartbeat checker error: %s", exc)
            await asyncio.sleep(check_interval_s)
