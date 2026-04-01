from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg2.extras
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app import db as DB
from app.experiments.parser import load_template_from_dict, substitute_params

router = APIRouter()

_TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RunRequest(BaseModel):
    template_id: str
    params: dict[str, Any] = {}


@router.get("/templates")
def list_templates():
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, version, description, parameters_schema, definition, tags, created_at
                FROM experiment_templates
                ORDER BY name
                """
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


@router.get("/templates/{template_id}")
def get_template(template_id: str):
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, version, description, parameters_schema, definition, tags, created_at
                FROM experiment_templates
                WHERE id = %s
                """,
                (template_id,),
            )
            row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Template '{template_id}' not found")
    return dict(row)


@router.delete("/templates/{template_id}")
def delete_template(template_id: str):
    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM experiment_templates WHERE id = %s", (template_id,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail=f"Template '{template_id}' not found")
            cur.execute("DELETE FROM experiment_steps WHERE run_id IN (SELECT id FROM experiment_runs WHERE template_id = %s)", (template_id,))
            cur.execute("DELETE FROM experiment_runs WHERE template_id = %s", (template_id,))
            cur.execute("DELETE FROM experiment_templates WHERE id = %s", (template_id,))
        conn.commit()
    return {"deleted": True, "id": template_id}


@router.post("/templates", status_code=201)
async def upsert_template(request: Request):
    try:
        body = await request.json()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

    try:
        template = load_template_from_dict(body)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if not template.id or not template.id.strip():
        raise HTTPException(status_code=422, detail="Template 'id' must not be empty")

    now = _now()
    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO experiment_templates (
                    id, name, version, description, parameters_schema,
                    definition, tags, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    version = EXCLUDED.version,
                    description = EXCLUDED.description,
                    parameters_schema = EXCLUDED.parameters_schema,
                    definition = EXCLUDED.definition,
                    tags = EXCLUDED.tags
                """,
                (
                    template.id,
                    template.name,
                    template.version,
                    template.description,
                    psycopg2.extras.Json(template.parameters_schema_dict()),
                    psycopg2.extras.Json(template.to_definition_dict()),
                    template.tags,
                    now,
                ),
            )
        conn.commit()
    return {"id": template.id, "name": template.name, "version": template.version}


@router.post("/run", status_code=202)
async def start_run(body: RunRequest, request: Request):
    engine = request.app.state.experiment_engine

    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT definition FROM experiment_templates WHERE id = %s", (body.template_id,))
            row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"Template '{body.template_id}' not found")

    try:
        template = load_template_from_dict(row["definition"])
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=422, detail=f"Template definition invalid: {exc}") from exc

    try:
        substitute_params(template, body.params)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    run_id = str(uuid.uuid4())
    now = _now()

    with DB.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO experiment_runs (id, template_id, status, parameters, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    body.template_id,
                    "pending",
                    psycopg2.extras.Json(body.params),
                    now,
                ),
            )
        conn.commit()

    asyncio.create_task(engine.run(run_id, template, body.params))
    return {"run_id": run_id, "status": "pending", "template_id": body.template_id}


@router.get("/runs")
def list_runs(status: str | None = None):
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if status:
                cur.execute(
                    """
                    SELECT id, template_id, status, parameters, started_at, ended_at, error, created_at
                    FROM experiment_runs
                    WHERE status = %s
                    ORDER BY created_at DESC
                    """,
                    (status,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, template_id, status, parameters, started_at, ended_at, error, created_at
                    FROM experiment_runs
                    ORDER BY created_at DESC
                    """
                )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


@router.get("/runs/{run_id}")
def get_run(run_id: str):
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, template_id, status, parameters, started_at, ended_at, error, created_at
                FROM experiment_runs
                WHERE id = %s
                """,
                (run_id,),
            )
            run_row = cur.fetchone()
            if run_row is None:
                raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")

            cur.execute(
                """
                SELECT id, step_index, step_name, agent_id, component_id,
                       action, status, attempt, request_payload, response_payload,
                       started_at, ended_at, duration_ms
                FROM experiment_steps
                WHERE run_id = %s
                ORDER BY step_index, attempt
                """,
                (run_id,),
            )
            step_rows = cur.fetchall()

    run = dict(run_row)
    run["steps"] = [dict(row) for row in step_rows]
    return run


@router.get("/runs/{run_id}/steps")
def get_run_steps(run_id: str):
    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM experiment_runs WHERE id = %s", (run_id,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
            cur.execute(
                """
                SELECT id, step_index, step_name, agent_id, component_id,
                       action, status, attempt, request_payload, response_payload,
                       started_at, ended_at, duration_ms
                FROM experiment_steps
                WHERE run_id = %s
                ORDER BY step_index, attempt
                """,
                (run_id,),
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


@router.delete("/runs/{run_id}")
def cancel_run(run_id: str, request: Request):
    engine = request.app.state.experiment_engine

    with DB.connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT status FROM experiment_runs WHERE id = %s", (run_id,))
            row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    if row["status"] in _TERMINAL_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"Run '{run_id}' is already in terminal status '{row['status']}'",
        )

    engine.cancel(run_id)
    return {"cancelled": True, "run_id": run_id}
