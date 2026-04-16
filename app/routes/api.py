"""REST API and WebSocket routes for the LUCID orchestrator."""

from __future__ import annotations

import logging

import psycopg2.extras
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from app import db as DB
from app.auth_service import AuthServiceError
from app.command_dispatch import send_command
from app.ws_manager import WebSocketManager
from app.sync import sync_mqtt_users, sync_topic_links
from app.topic_links import service as topic_link_service

log = logging.getLogger(__name__)

router = APIRouter()

def _normalize_auth_result(kind: str, value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if kind == "authn":
        return "allow" if normalized in {"success", "allow", "ok"} else "deny"
    if normalized in {"allow", "authorized", "matched_allow", "ok", "success"}:
        return "allow"
    return "deny"


def _raise_auth_error(exc: AuthServiceError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.detail)


def _was_uninstalled(conn, agent_id: str, component_id: str) -> bool:
    """Check if the most recent command for a component was a successful uninstall."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT action, result_ok
            FROM commands
            WHERE agent_id = %s AND component_id = %s
            ORDER BY sent_ts DESC
            LIMIT 1
            """,
            (agent_id, component_id),
        )
        row = cur.fetchone()
    return row is not None and row["action"] == "uninstall" and row["result_ok"] is True


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
                s.state AS status_state,
                s.connected_since_ts,
                s.uptime_s,
                s.received_ts AS status_received_ts,
                st.components AS state_components,
                st.received_ts AS state_received_ts,
                m.version AS metadata_version,
                m.platform,
                m.architecture,
                m.received_ts AS metadata_received_ts,
                cfg.heartbeat_s,
                cfg.received_ts AS cfg_received_ts,
                cfg_logging.log_level AS cfg_log_level,
                cfg_logging.received_ts AS cfg_logging_received_ts,
                cfg_telemetry.cpu_pct_enabled,
                cfg_telemetry.cpu_pct_interval_s,
                cfg_telemetry.cpu_pct_threshold,
                cfg_telemetry.memory_pct_enabled,
                cfg_telemetry.memory_pct_interval_s,
                cfg_telemetry.memory_pct_threshold,
                cfg_telemetry.disk_pct_enabled,
                cfg_telemetry.disk_pct_interval_s,
                cfg_telemetry.disk_pct_threshold,
                cfg_telemetry.received_ts AS cfg_telemetry_received_ts
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
                cs.received_ts AS status_received_ts,
                cm.version AS metadata_version,
                cm.capabilities,
                cm.received_ts AS metadata_received_ts,
                cst.payload AS state_payload,
                cst.received_ts AS state_received_ts,
                ccfg.payload AS cfg_payload,
                ccfg.received_ts AS cfg_received_ts,
                ccfg_log.log_level AS cfg_log_level,
                ccfg_log.received_ts AS cfg_logging_received_ts,
                ccfg_tel.payload AS cfg_telemetry_payload,
                ccfg_tel.received_ts AS cfg_telemetry_received_ts
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
            component_cfg = {**component_cfg, "logging": {"log_level": row["cfg_log_level"]}}
        if row["cfg_telemetry_payload"] is not None:
            component_cfg = {**component_cfg, "telemetry": row["cfg_telemetry_payload"]}

        has_comp_status = row["status_received_ts"] is not None
        has_comp_metadata = row["metadata_received_ts"] is not None
        has_comp_state = row["state_received_ts"] is not None
        has_comp_cfg = any(
            row[key] is not None
            for key in ("cfg_received_ts", "cfg_logging_received_ts", "cfg_telemetry_received_ts")
        )

        components_by_agent.setdefault(row["agent_id"], {})[row["component_id"]] = {
            "component_id": row["component_id"],
            "first_seen_ts": row["first_seen_ts"],
            "last_seen_ts": row["last_seen_ts"],
            "status": {"state": row["status_state"], "received_ts": row["status_received_ts"]} if has_comp_status else None,
            "metadata": {
                "version": row["metadata_version"],
                "capabilities": row["capabilities"],
                "received_ts": row["metadata_received_ts"],
            } if has_comp_metadata else None,
            "state": row["state_payload"] if has_comp_state else None,
            "cfg": (component_cfg if component_cfg else {"received_ts": max(
                ts for ts in (row["cfg_received_ts"], row["cfg_logging_received_ts"], row["cfg_telemetry_received_ts"]) if ts is not None
            )}) if has_comp_cfg else None,
        }

    # Filter out uninstalled components using two sources of truth:
    # 1. agent state.components list (if available)
    # 2. commands table: exclude if last command was a successful uninstall
    for row in agent_rows:
        aid = row["agent_id"]
        if aid not in components_by_agent:
            continue
        installed = row.get("state_components")
        if installed is not None:
            # Components may be a list of strings or a list of dicts with component_id
            installed_set = set(
                c["component_id"] if isinstance(c, dict) else c
                for c in installed
            )
            components_by_agent[aid] = {
                cid: comp for cid, comp in components_by_agent[aid].items()
                if cid in installed_set
            }
        else:
            components_by_agent[aid] = {
                cid: comp for cid, comp in components_by_agent[aid].items()
                if not _was_uninstalled(conn, aid, cid)
            }

    agents: list[dict] = []
    for row in agent_rows:
        cfg: dict = {}
        if row["heartbeat_s"] is not None:
            cfg["heartbeat_s"] = row["heartbeat_s"]
        if row["cfg_log_level"] is not None:
            cfg["logging"] = {"log_level": row["cfg_log_level"]}
        telemetry_cfg = {
            "cpu_percent": {
                "enabled": row["cpu_pct_enabled"],
                "interval_s": row["cpu_pct_interval_s"],
                "change_threshold_percent": row["cpu_pct_threshold"],
            },
            "memory_percent": {
                "enabled": row["memory_pct_enabled"],
                "interval_s": row["memory_pct_interval_s"],
                "change_threshold_percent": row["memory_pct_threshold"],
            },
            "disk_percent": {
                "enabled": row["disk_pct_enabled"],
                "interval_s": row["disk_pct_interval_s"],
                "change_threshold_percent": row["disk_pct_threshold"],
            },
        }
        if any(value is not None for metric in telemetry_cfg.values() for value in metric.values()):
            cfg["telemetry"] = telemetry_cfg

        has_cfg = any(
            row[key] is not None
            for key in ("cfg_received_ts", "cfg_logging_received_ts", "cfg_telemetry_received_ts")
        )
        has_status = row["status_received_ts"] is not None
        has_state = row["state_received_ts"] is not None
        has_metadata = row["metadata_received_ts"] is not None

        agents.append(
            {
                "agent_id": row["agent_id"],
                "first_seen_ts": row["first_seen_ts"],
                "last_seen_ts": row["last_seen_ts"],
                "status": {
                    "state": row["status_state"],
                    "connected_since_ts": row["connected_since_ts"],
                    "uptime_s": row["uptime_s"],
                    "received_ts": row["status_received_ts"],
                } if has_status else None,
                "state": (
                    {
                        "components": row["state_components"],
                        "received_ts": row["state_received_ts"],
                    }
                ) if has_state else None,
                "metadata": {
                    "version": row["metadata_version"],
                    "platform": row["platform"],
                    "architecture": row["architecture"],
                    "received_ts": row["metadata_received_ts"],
                } if has_metadata else None,
                "cfg": (cfg if cfg else {"received_ts": max(
                    ts for ts in (row["cfg_received_ts"], row["cfg_logging_received_ts"], row["cfg_telemetry_received_ts"]) if ts is not None
                )}) if has_cfg else None,
                "components": components_by_agent.get(row["agent_id"], {}),
            }
        )
    if agent_id:
        return agents

    agent_ids = {agent["agent_id"] for agent in agents}
    deduped: list[dict] = []
    for agent in agents:
        canonical = agent["agent_id"].removeprefix("lucid.agent.")
        if canonical != agent["agent_id"] and canonical in agent_ids:
            continue
        deduped.append(agent)
    return deduped


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


async def _dispatch_command(
    request: Request,
    *,
    agent_id: str,
    action: str,
    component_id: str | None = None,
    body: dict | None = None,
    wait: bool = False,
    timeout_s: float = 30.0,
) -> dict:
    try:
        return await send_command(
            request.app,
            agent_id=agent_id,
            component_id=component_id,
            action=action,
            body=body,
            wait=wait,
            timeout_s=timeout_s,
        )
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=f"Timed out waiting for agent '{agent_id}'") from exc


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


@router.get("/agents/{agent_id}/command-catalog")
def agent_command_catalog(agent_id: str):
    from app.command_catalog import get_agent_commands, get_component_commands

    with DB.connect() as conn:
        agents = _query_agents(conn, agent_id)
    if not agents:
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found. Call list_agents first to get valid agent IDs.")

    agent = agents[0]
    components_catalog: dict[str, list[dict]] = {}
    for cid, comp in (agent.get("components") or {}).items():
        caps = (comp.get("metadata") or {}).get("capabilities")
        components_catalog[cid] = get_component_commands(caps)

    return {"agent": get_agent_commands(), "components": components_catalog}


@router.get("/topic-tree")
def topic_tree():
    """Return the full MQTT topic tree for all agents with their components."""
    from app.command_catalog import AGENT_COMMANDS, COMPONENT_TEMPLATES, _BASE_COMMANDS

    with DB.connect() as conn:
        agents = _query_agents(conn)

    agent_topics = {
        "retained": ["metadata", "status", "state", "cfg", "cfg/logging", "cfg/telemetry"],
        "streams": ["logs", "telemetry/{metric}"],
        "commands": [cmd["action"] for cmd in AGENT_COMMANDS],
        "events": [f"{cmd['action']}/result" for cmd in AGENT_COMMANDS],
    }

    result = []
    for agent in agents:
        aid = agent["agent_id"]
        status = (agent.get("status") or {}).get("state")
        prefix = f"lucid/agents/{aid}"

        agent_entry = {
            "agent_id": aid,
            "status": status,
            "prefix": prefix,
            "topics": {
                "retained": [f"{prefix}/{t}" for t in agent_topics["retained"]],
                "streams": [f"{prefix}/{t}" for t in agent_topics["streams"]],
                "commands": [f"{prefix}/cmd/{t}" for t in agent_topics["commands"]],
                "events": [f"{prefix}/evt/{t}" for t in agent_topics["events"]],
            },
            "components": [],
        }

        for cid, comp in (agent.get("components") or {}).items():
            caps = (comp.get("metadata") or {}).get("capabilities") or []
            comp_status = (comp.get("status") or {}).get("state")
            comp_prefix = f"{prefix}/components/{cid}"

            all_actions = list(caps) + [a for a in _BASE_COMMANDS if a not in caps]

            agent_entry["components"].append({
                "component_id": cid,
                "status": comp_status,
                "prefix": comp_prefix,
                "capabilities": caps,
                "topics": {
                    "retained": [f"{comp_prefix}/{t}" for t in ["metadata", "status", "state", "cfg", "cfg/logging", "cfg/telemetry"]],
                    "streams": [f"{comp_prefix}/{t}" for t in ["logs", "telemetry/{{metric}}"]],
                    "commands": [f"{comp_prefix}/cmd/{a}" for a in all_actions],
                    "events": [f"{comp_prefix}/evt/{a}/result" for a in all_actions],
                },
            })

        result.append(agent_entry)

    return result


@router.post("/agents/{agent_id}/cmd/{action:path}")
async def send_agent_command(agent_id: str, action: str, request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Command body must be a JSON object")
    return await _dispatch_command(request, agent_id=agent_id, action=action, body=body)


@router.post("/agents/{agent_id}/components/{component_id}/cmd/{action:path}")
async def send_component_command(agent_id: str, component_id: str, action: str, request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Command body must be a JSON object")
    return await _dispatch_command(
        request,
        agent_id=agent_id,
        component_id=component_id,
        action=action,
        body=body,
    )


@router.post("/internal/command")
async def internal_command(body: InternalCommandRequest, request: Request):
    return await _dispatch_command(
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
        rows = DB.list_mqtt_users(conn, roles=("agent", "central-command", "observer"))
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


@router.post("/users/observer")
def create_observer_user(body: AddAgentRequest, request: Request):
    sync_mqtt_users(request.app, strict=False)
    with DB.connect() as conn:
        existing = DB.get_mqtt_user(conn, body.agent_id)
        if existing is not None and existing.get("has_password_user", True):
            raise HTTPException(status_code=409, detail=f"User '{body.agent_id}' already exists")

    try:
        result = request.app.state.auth.create_observer(body.agent_id)
    except AuthServiceError as exc:
        _raise_auth_error(exc)

    try:
        sync_mqtt_users(request.app, strict=True)
    except AuthServiceError as exc:
        _raise_auth_error(exc)

    return {"username": body.agent_id, "role": "observer", "password": result["password"]}


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
        elif role == "observer":
            request.app.state.auth.delete_observer(username)
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
        elif role == "observer":
            result = request.app.state.auth.create_observer(username)
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
        row = DB.get_topic_link(conn, link_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Topic link not found")
    return row


@router.post("/topic-links")
async def create_topic_link(body: TopicLinkCreateRequest, request: Request):
    try:
        row = topic_link_service.create_topic_link(
            request.app,
            name=body.name,
            source_topic=body.source_topic,
            target_topic=body.target_topic,
            select_clause=body.select_clause,
            payload_template=body.payload_template,
            qos=body.qos,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    ws_mgr: WebSocketManager = request.app.state.ws_mgr
    await ws_mgr.broadcast({"type": "topic_link_created", "link_id": row["id"]})
    return row


@router.put("/topic-links/{link_id}/activate")
async def activate_topic_link(link_id: str, request: Request):
    try:
        row = topic_link_service.activate_topic_link(request.app, link_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    ws_mgr: WebSocketManager = request.app.state.ws_mgr
    await ws_mgr.broadcast({"type": "topic_link_updated", "link_id": link_id})
    return {"id": link_id, "enabled": row["enabled"]}


@router.put("/topic-links/{link_id}/deactivate")
async def deactivate_topic_link(link_id: str, request: Request):
    try:
        row = topic_link_service.deactivate_topic_link(request.app, link_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    ws_mgr: WebSocketManager = request.app.state.ws_mgr
    await ws_mgr.broadcast({"type": "topic_link_updated", "link_id": link_id})
    return {"id": link_id, "enabled": row["enabled"]}


@router.delete("/topic-links/{link_id}")
async def delete_topic_link(link_id: str, request: Request):
    try:
        topic_link_service.delete_topic_link(request.app, link_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    ws_mgr: WebSocketManager = request.app.state.ws_mgr
    await ws_mgr.broadcast({"type": "topic_link_deleted", "link_id": link_id})
    return {"deleted": True, "id": link_id}


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_mgr: WebSocketManager = ws.app.state.ws_mgr
    await ws_mgr.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await ws_mgr.disconnect(ws)
