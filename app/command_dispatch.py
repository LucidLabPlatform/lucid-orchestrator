from __future__ import annotations

import uuid
from datetime import datetime, timezone

from app import db as DB


def _now() -> datetime:
    return datetime.now(timezone.utc)


def command_topic(agent_id: str, action: str, component_id: str | None = None) -> str:
    if component_id:
        return f"lucid/agents/{agent_id}/components/{component_id}/cmd/{action}"
    return f"lucid/agents/{agent_id}/cmd/{action}"


def command_payload(action: str, body: dict, request_id: str) -> dict:
    return {**body, "action": action, "request_id": request_id}


async def send_command(
    app,
    *,
    agent_id: str,
    action: str,
    component_id: str | None = None,
    body: dict | None = None,
    wait: bool = False,
    timeout_s: float = 30.0,
) -> dict:
    payload_body = body or {}
    request_id = str(payload_body.get("request_id") or uuid.uuid4())
    payload = command_payload(action, payload_body, request_id)
    topic = command_topic(agent_id, action, component_id=component_id)
    ts = _now()

    with DB.connect() as conn:
        if component_id:
            DB.ensure_component(conn, agent_id, component_id, ts)
        else:
            DB.ensure_agent(conn, agent_id, ts)
        conn.commit()

    bridge = app.state.bridge
    if wait:
        result = await app.state.rrm.send_and_wait(
            bridge=bridge,
            topic=topic,
            payload=payload,
            timeout_s=timeout_s,
        )
        return {"request_id": request_id, "topic": topic, "result": result}

    bridge.publish(topic, payload)
    return {"request_id": request_id, "topic": topic}
