"""REST API and WebSocket routes for the LUCID orchestrator."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

import psycopg2.extras
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from app import db as DB
from app.auth_service import AuthServiceError
from app.sync import TOPIC_LINKS_DOMAIN, sync_mqtt_users, sync_topic_links
from app.topic_links.manager import TopicLinkDef

log = logging.getLogger(__name__)

router = APIRouter()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_auth_result(kind: str, value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if kind == "authn":
        return "allow" if normalized in {"success", "allow", "ok"} else "deny"
    if normalized in {"allow", "authorized", "matched_allow", "ok", "success"}:
        return "allow"
    return "deny"


async def _broadcast_ws(ws_clients: set, event: dict) -> None:
    msg = json.dumps(event)
    dead = set()
    for ws in list(ws_clients):
        try:
            await asyncio.wait_for(ws.send_text(msg), timeout=2.0)
        except Exception:
            dead.add(ws)
    ws_clients -= dead


def _raise_auth_error(exc: AuthServiceError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.detail)


def _query_agents(conn, agent_id: str | None = None) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        where = "WHERE a.agent_id = %s" if agent_id else ""
        params = (agent_id,) if agent_id else ()

        cur.execute(
            f"""
            SELECT
                a.agent_id,
                a.first_seen_ts,
                a.last_seen_ts,
                s.state,
                s.connected_since_ts,
                s.uptime_s,
                s.version AS status_version,
                st.cpu_percent,
                st.memory_percent,
                st.disk_percent,
                st.components AS state_components,
                m.version AS metadata_version,
                m.platform,
                m.architecture,
                cfg.heartbeat_s,
                cfg_logging.log_level AS cfg_log_level,
                cfg_telemetry.cpu_pct_enabled,
                cfg_telemetry.cpu_pct_interval_s,
                cfg_telemetry.cpu_pct_threshold,
                cfg_telemetry.memory_pct_enabled,
                cfg_telemetry.memory_pct_interval_s,
                cfg_telemetry.memory_pct_threshold,
                cfg_telemetry.disk_pct_enabled,
                cfg_telemetry.disk_pct_interval_s,
                cfg_telemetry.disk_pct_threshold
            FROM agents a
            LEFT JOIN agent_status s ON s.agent_id = a.agent_id
            LEFT JOIN agent_state st ON st.agent_id = a.agent_id
            LEFT JOIN agent_metadata m ON m.agent_id = a.agent_id
            LEFT JOIN agent_cfg cfg ON cfg.agent_id = a.agent_id
            LEFT JOIN agent_cfg_logging cfg_logging ON cfg_logging.agent_id = a.agent_id
            LEFT JOIN agent_cfg_telemetry cfg_telemetry ON cfg_telemetry.agent_id = a.agent_id
            {where}
            ORDER BY a.last_seen_ts DESC, a.agent_id
            """,
            params,
        )
        agent_rows = cur.fetchall()

        if not agent_rows:
            return []

        agent_ids = [row["agent_id"] for row in agent_rows]
        placeholders = ",".join(["%s"] * len(agent_ids))
        cur.execute(
            f"""
            SELECT
                c.agent_id,
                c.component_id,
                c.first_seen_ts,
                c.last_seen_ts,
                cs.state AS status_state,
                cm.version AS metadata_version,
                cm.capabilities,
                cst.payload AS state_payload,
                ccfg.payload AS cfg_payload,
                ccfg_log.log_level AS cfg_log_level,
                ccfg_tel.payload AS cfg_telemetry_payload
            FROM components c
            LEFT JOIN component_status cs
                ON cs.agent_id = c.agent_id AND cs.component_id = c.component_id
            LEFT JOIN component_metadata cm
                ON cm.agent_id = c.agent_id AND cm.component_id = c.component_id
            LEFT JOIN component_state cst
                ON cst.agent_id = c.agent_id AND cst.component_id = c.component_id
            LEFT JOIN component_cfg ccfg
                ON ccfg.agent_id = c.agent_id AND ccfg.component_id = c.component_id
            LEFT JOIN component_cfg_logging ccfg_log
                ON ccfg_log.agent_id = c.agent_id AND ccfg_log.component_id = c.component_id
            LEFT JOIN component_cfg_telemetry ccfg_tel
                ON ccfg_tel.agent_id = c.agent_id AND ccfg_tel.component_id = c.component_id
            WHERE c.agent_id IN ({placeholders})
            ORDER BY c.agent_id, c.component_id
            """,
            tuple(agent_ids),
        )
        component_rows = cur.fetchall()

    components_by_agent: dict[str, dict[str, dict]] = {}
    for row in component_rows:
        component_cfg = row["cfg_payload"] or {}
        if row["cfg_log_level"] is not None:
            component_cfg = {**component_cfg, "logging": {"level": row["cfg_log_level"]}}
        if row["cfg_telemetry_payload"] is not None:
            component_cfg = {**component_cfg, "telemetry": row["cfg_telemetry_payload"]}

        components_by_agent.setdefault(row["agent_id"], {})[row["component_id"]] = {
            "component_id": row["component_id"],
            "first_seen_ts": row["first_seen_ts"],
            "last_seen_ts": row["last_seen_ts"],
            "status": {"state": row["status_state"]} if row["status_state"] is not None else None,
            "metadata": {
                "version": row["metadata_version"],
                "capabilities": row["capabilities"],
            } if row["metadata_version"] is not None or row["capabilities"] is not None else None,
            "state": row["state_payload"],
            "cfg": component_cfg or None,
        }

    agents: list[dict] = []
    for row in agent_rows:
        cfg = {}
        if row["heartbeat_s"] is not None:
            cfg["heartbeat_s"] = row["heartbeat_s"]
        if row["cfg_log_level"] is not None:
            cfg["logging"] = {"level": row["cfg_log_level"]}
        telemetry_cfg = {
            "cpu_percent": {
                "enabled": row["cpu_pct_enabled"],
                "interval_s": row["cpu_pct_interval_s"],
                "threshold": row["cpu_pct_threshold"],
            },
            "memory_percent": {
                "enabled": row["memory_pct_enabled"],
                "interval_s": row["memory_pct_interval_s"],
                "threshold": row["memory_pct_threshold"],
            },
            "disk_percent": {
                "enabled": row["disk_pct_enabled"],
                "interval_s": row["disk_pct_interval_s"],
                "threshold": row["disk_pct_threshold"],
            },
        }
        if any(value is not None for metric in telemetry_cfg.values() for value in metric.values()):
            cfg["telemetry"] = telemetry_cfg

        agents.append(
            {
                "agent_id": row["agent_id"],
                "first_seen_ts": row["first_seen_ts"],
                "last_seen_ts": row["last_seen_ts"],
                "status": {
                    "state": row["state"],
                    "connected_since_ts": row["connected_since_ts"],
                    "uptime_s": row["uptime_s"],
                    "version": row["status_version"],
                } if row["state"] is not None else None,
                "state": {
                    "cpu_percent": row["cpu_percent"],
                    "memory_percent": row["memory_percent"],
                    "disk_percent": row["disk_percent"],
                    "components": row["state_components"],
                } if any(
                    row[key] is not None
                    for key in ("cpu_percent", "memory_percent", "disk_percent", "state_components")
                ) else None,
                "metadata": {
                    "version": row["metadata_version"],
                    "platform": row["platform"],
                    "architecture": row["architecture"],
                } if any(row[key] is not None for key in ("metadata_version", "platform", "architecture")) else None,
                "cfg": cfg or None,
                "components": components_by_agent.get(row["agent_id"], {}),
            }
        )
    return agents


def _flatten_logs(rows: list[dict]) -> list[dict]:
    flattened: list[dict] = []
    for row in rows:
        payload = row["payload"]
        base = {
            "agent_id": row["agent_id"],
            "component_id": row["component_id"],
            "received_ts": row["received_ts"],
        }
        if isinstance(payload, dict) and isinstance(payload.get("lines"), list):
            for line in payload["lines"]:
                if isinstance(line, dict):
                    flattened.append({**base, **line})
            continue
        if isinstance(payload, dict):
            flattened.append({**base, **payload})
            continue
        flattened.append({**base, "message": str(payload)})
    return flattened


def _command_topic(agent_id: str, action: str, component_id: str | None = None) -> str:
    if component_id:
        return f"lucid/agents/{agent_id}/components/{component_id}/cmd/{action}"
    return f"lucid/agents/{agent_id}/cmd/{action}"


def _command_payload(action: str, body: dict, request_id: str) -> dict:
    return {**body, "action": action, "request_id": request_id}


async def _send_command(
    request: Request,
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
    payload = _command_payload(action, payload_body, request_id)
    topic = _command_topic(agent_id, action, component_id=component_id)
    ts = _now()

    with DB.connect() as conn:
        if component_id:
            DB.ensure_component(conn, agent_id, component_id, ts)
        else:
            DB.ensure_agent(conn, agent_id, ts)
        conn.commit()

    bridge = request.app.state.bridge
    if wait:
        try:
            result = await request.app.state.rrm.send_and_wait(
                bridge=bridge,
                topic=topic,
                payload=payload,
                timeout_s=timeout_s,
            )
        except asyncio.TimeoutError as exc:
            raise HTTPException(status_code=504, detail=f"Timed out waiting for {topic}") from exc
        return {"request_id": request_id, "topic": topic, "result": result}

    bridge.publish(topic, payload)
    return {"request_id": request_id, "topic": topic}


class AddAgentRequest(BaseModel):
    agent_id: str


class RotatePasswordResponse(BaseModel):
    username: str
    role: str
    password: str


class InternalCommandRequest(BaseModel):
    agent_id: str
    action: str
    component_id: str | None = None
    payload: dict = Field(default_factory=dict)
    wait: bool = False
    timeout_s: float = 30.0


class TopicLinkCreateRequest(BaseModel):
    name: str
    source_topic: str
    target_topic: str
    select_clause: str = "*"
    payload_template: str | None = None
    qos: int = Field(default=0, ge=0, le=2)


@router.get("/agents")
def list_agents():
    with DB.connect() as conn:
        return _query_agents(conn)


@router.get("/agents/{agent_id}")
def get_agent(agent_id: str):
    with DB.connect() as conn:
        agents = _query_agents(conn, agent_id=agent_id)
    if not agents:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agents[0]


@router.delete("/agents/{agent_id}")
def delete_agent(agent_id: str, request: Request):
    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM agents WHERE agent_id = %s", (agent_id,))
            exists = cur.fetchone() is not None
        if not exists:
            raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")

    try:
        request.app.state.auth.delete_agent(agent_id)
    except AuthServiceError as exc:
        _raise_auth_error(exc)

    try:
        sync_mqtt_users(request.app, strict=True)
    except AuthServiceError as exc:
        _raise_auth_error(exc)

    with DB.connect() as conn:
        DB.purge_agent_data(conn, agent_id)
        DB.delete_user_metadata(conn, agent_id)
        conn.commit()
    return {"deleted": True, "agent_id": agent_id}


@router.get("/agents/{agent_id}/logs")
def agent_logs(agent_id: str, limit: int = 100):
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT agent_id, component_id, payload, received_ts
                FROM logs
                WHERE agent_id = %s
                ORDER BY received_ts DESC
                LIMIT %s
                """,
                (agent_id, limit),
            )
            rows = cur.fetchall()
    return _flatten_logs([dict(row) for row in rows])


@router.get("/agents/{agent_id}/commands")
def agent_commands(agent_id: str, limit: int = 50):
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    request_id,
                    component_id,
                    action,
                    topic,
                    payload,
                    publisher_username,
                    publisher_clientid,
                    result_received,
                    result_ok,
                    result_ts,
                    sent_ts
                FROM commands
                WHERE agent_id = %s
                ORDER BY sent_ts DESC
                LIMIT %s
                """,
                (agent_id, limit),
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


@router.post("/agents/{agent_id}/cmd/{action}")
async def send_agent_command(agent_id: str, action: str, request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Command body must be a JSON object")
    return await _send_command(request, agent_id=agent_id, action=action, body=body)


@router.post("/agents/{agent_id}/components/{component_id}/cmd/{action}")
async def send_component_command(agent_id: str, component_id: str, action: str, request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Command body must be a JSON object")
    return await _send_command(
        request,
        agent_id=agent_id,
        component_id=component_id,
        action=action,
        body=body,
    )


@router.post("/internal/command")
async def internal_command(body: InternalCommandRequest, request: Request):
    return await _send_command(
        request,
        agent_id=body.agent_id,
        component_id=body.component_id,
        action=body.action,
        body=body.payload,
        wait=body.wait,
        timeout_s=body.timeout_s,
    )


@router.get("/users")
def list_users(request: Request):
    sync_mqtt_users(request.app, strict=False)
    with DB.connect() as conn:
        rows = DB.list_mqtt_users(conn, roles=("agent", "central-command"))
    return [row for row in rows if row.get("has_password_user", True)]


@router.post("/users/agent")
def create_agent_user(body: AddAgentRequest, request: Request):
    sync_mqtt_users(request.app, strict=False)
    with DB.connect() as conn:
        existing = DB.get_mqtt_user(conn, body.agent_id)
        if existing is not None and existing.get("has_password_user", True):
            raise HTTPException(status_code=409, detail=f"User '{body.agent_id}' already exists")

    try:
        result = request.app.state.auth.create_agent(body.agent_id)
    except AuthServiceError as exc:
        _raise_auth_error(exc)

    try:
        sync_mqtt_users(request.app, strict=True)
    except AuthServiceError as exc:
        _raise_auth_error(exc)

    return {"username": body.agent_id, "role": "agent", "password": result["password"]}


@router.post("/users/cc")
def create_cc_user(request: Request):
    try:
        result = request.app.state.auth.create_cc()
    except AuthServiceError as exc:
        _raise_auth_error(exc)

    try:
        sync_mqtt_users(request.app, strict=True)
    except AuthServiceError as exc:
        _raise_auth_error(exc)
    username = result["username"]
    return {"username": username, "role": "central-command", "password": result["password"]}


@router.delete("/users/{username}")
def delete_user(username: str, request: Request):
    sync_mqtt_users(request.app, strict=False)
    with DB.connect() as conn:
        row = DB.get_mqtt_user(conn, username)
        role = row["role"] if row and row.get("has_password_user", True) else None
    if role is None:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")

    try:
        if role == "central-command":
            if username != request.app.state.cc_username:
                raise HTTPException(status_code=400, detail="Only the configured Central Command user can be managed here")
            request.app.state.auth.delete_cc()
        elif role == "agent":
            request.app.state.auth.delete_agent(username)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported role '{role}'")
    except AuthServiceError as exc:
        _raise_auth_error(exc)

    try:
        sync_mqtt_users(request.app, strict=True)
    except AuthServiceError as exc:
        _raise_auth_error(exc)
    return {"deleted": username}


@router.post("/users/{username}/rotate-password", response_model=RotatePasswordResponse)
def rotate_password(username: str, request: Request):
    sync_mqtt_users(request.app, strict=False)
    with DB.connect() as conn:
        row = DB.get_mqtt_user(conn, username)
        role = row["role"] if row and row.get("has_password_user", True) else None
    if role is None:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")

    try:
        if role == "central-command":
            if username != request.app.state.cc_username:
                raise HTTPException(status_code=400, detail="Only the configured Central Command user can be managed here")
            result = request.app.state.auth.create_cc()
            username = result["username"]
        elif role == "agent":
            result = request.app.state.auth.create_agent(username)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported role '{role}'")
    except AuthServiceError as exc:
        _raise_auth_error(exc)

    try:
        sync_mqtt_users(request.app, strict=True)
    except AuthServiceError as exc:
        _raise_auth_error(exc)
    return RotatePasswordResponse(username=username, role=role, password=result["password"])


@router.get("/auth-log")
def auth_log(limit: int = 200):
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ts, 'authn' AS type, username, clientid, NULL::text AS topic, NULL::text AS action, result
                FROM authn_log
                UNION ALL
                SELECT ts, 'authz' AS type, username, clientid, topic, action, result
                FROM authz_log
                ORDER BY ts DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    return [
        {
            **dict(row),
            "result": _normalize_auth_result(dict(row)["type"], dict(row)["result"]),
        }
        for row in rows
    ]


@router.get("/sync-state")
def sync_state():
    with DB.connect() as conn:
        return DB.get_sync_state(conn)


@router.get("/schema/tables")
def schema_tables():
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    c.table_name,
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    CASE WHEN pk.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_primary_key
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT kcu.table_name, kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = 'public'
                ) pk ON pk.table_name = c.table_name AND pk.column_name = c.column_name
                WHERE c.table_schema = 'public'
                ORDER BY c.table_name, c.ordinal_position
                """
            )
            rows = cur.fetchall()

    tables: dict[str, list[dict]] = {}
    for row in rows:
        data = dict(row)
        tables.setdefault(data["table_name"], []).append(
            {
                "column": data["column_name"],
                "type": data["data_type"],
                "nullable": data["is_nullable"] == "YES",
                "default": data["column_default"],
                "primary_key": bool(data["is_primary_key"]),
            }
        )
    return {"tables": tables}


@router.get("/schema/relations")
def schema_relations():
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    tc.table_name AS from_table,
                    kcu.column_name AS from_column,
                    ccu.table_name AS to_table,
                    ccu.column_name AS to_column,
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON ccu.constraint_name = tc.constraint_name
                 AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = 'public'
                ORDER BY tc.table_name, kcu.column_name
                """
            )
            rows = cur.fetchall()
    return {"relations": [dict(row) for row in rows]}


@router.get("/topic-links")
def list_topic_links(request: Request):
    sync_topic_links(request.app, strict=False)
    with DB.connect() as conn:
        return DB.list_topic_links(conn)


@router.get("/topic-links/{link_id}")
def get_topic_link(link_id: str, request: Request):
    sync_topic_links(request.app, strict=False)
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, source_topic, target_topic, select_clause,
                       payload_template, qos, emqx_rule_id, enabled, created_at,
                       updated_at, last_synced_at, sync_status, last_error
                FROM topic_links
                WHERE id = %s
                """,
                (link_id,),
            )
            row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Topic link not found")
    return dict(row)


@router.post("/topic-links")
async def create_topic_link(body: TopicLinkCreateRequest, request: Request):
    link_id = str(uuid.uuid4())
    link_def = TopicLinkDef(
        name=body.name,
        source_topic=body.source_topic,
        target_topic=body.target_topic,
        select_clause=body.select_clause,
        payload_template=body.payload_template,
        qos=body.qos,
    )

    try:
        rule_id = request.app.state.tlm.create_link(link_def)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    created_at = _now()
    row = {
        "id": link_id,
        "name": body.name,
        "source_topic": body.source_topic,
        "target_topic": body.target_topic,
        "select_clause": body.select_clause,
        "payload_template": body.payload_template,
        "qos": body.qos,
        "emqx_rule_id": rule_id,
        "enabled": True,
        "created_at": created_at,
        "updated_at": created_at,
        "last_synced_at": created_at,
        "sync_status": "synced",
        "last_error": None,
    }

    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO topic_links (
                    id, name, source_topic, target_topic, select_clause,
                    payload_template, qos, emqx_rule_id, enabled, created_at,
                    updated_at, last_synced_at, sync_status, last_error
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["id"],
                    row["name"],
                    row["source_topic"],
                    row["target_topic"],
                    row["select_clause"],
                    row["payload_template"],
                    row["qos"],
                    row["emqx_rule_id"],
                    row["enabled"],
                    row["created_at"],
                    row["updated_at"],
                    row["last_synced_at"],
                    row["sync_status"],
                    row["last_error"],
                ),
            )
            DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="synced", synced_at=created_at, error=None)
        conn.commit()

    await _broadcast_ws(request.app.state.ws_clients, {"type": "topic_link_created", "link_id": link_id})
    return {**row, "created_at": created_at.isoformat()}


def _load_topic_link(conn, link_id: str) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM topic_links WHERE id = %s", (link_id,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Topic link not found")
    return dict(row)


@router.put("/topic-links/{link_id}/activate")
async def activate_topic_link(link_id: str, request: Request):
    with DB.connect() as conn:
        row = _load_topic_link(conn, link_id)

    if not row["emqx_rule_id"]:
        raise HTTPException(status_code=409, detail="Topic link has no EMQX rule ID")

    try:
        request.app.state.tlm.activate_link(row["emqx_rule_id"])
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

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
                (_now(), _now(), link_id),
            )
            DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="synced", synced_at=_now(), error=None)
        conn.commit()

    await _broadcast_ws(request.app.state.ws_clients, {"type": "topic_link_updated", "link_id": link_id})
    return {"id": link_id, "enabled": True}


@router.put("/topic-links/{link_id}/deactivate")
async def deactivate_topic_link(link_id: str, request: Request):
    with DB.connect() as conn:
        row = _load_topic_link(conn, link_id)

    if not row["emqx_rule_id"]:
        raise HTTPException(status_code=409, detail="Topic link has no EMQX rule ID")

    try:
        request.app.state.tlm.deactivate_link(row["emqx_rule_id"])
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

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
                (_now(), _now(), link_id),
            )
            DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="synced", synced_at=_now(), error=None)
        conn.commit()

    await _broadcast_ws(request.app.state.ws_clients, {"type": "topic_link_updated", "link_id": link_id})
    return {"id": link_id, "enabled": False}


@router.delete("/topic-links/{link_id}")
async def delete_topic_link(link_id: str, request: Request):
    with DB.connect() as conn:
        row = _load_topic_link(conn, link_id)

    rule_id = row["emqx_rule_id"]
    if rule_id:
        try:
            request.app.state.tlm.delete_link(rule_id)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM topic_links WHERE id = %s", (link_id,))
            DB.set_sync_state(conn, TOPIC_LINKS_DOMAIN, status="synced", synced_at=_now(), error=None)
        conn.commit()

    await _broadcast_ws(request.app.state.ws_clients, {"type": "topic_link_deleted", "link_id": link_id})
    return {"deleted": True, "id": link_id}


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients = ws.app.state.ws_clients
    ws_clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(ws)
