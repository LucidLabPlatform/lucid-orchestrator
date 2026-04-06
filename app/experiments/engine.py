from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import psycopg2.extras

from app import db as DB
from app.command_dispatch import send_command
from app.events import broadcast_ws
from app.experiments.models import StepDef, TemplateDef
from app.experiments.parser import resolve_params_in_step, substitute_params
from app.topic_links import service as topic_link_service

log = logging.getLogger(__name__)

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _extract_field(data: Any, field_path: str) -> Any:
    """Extract a nested value from *data* using a dot-separated path.

    Example: ``_extract_field({"value": {"state": "ok"}}, "value.state")``
    returns ``"ok"``.
    """
    for key in field_path.split("."):
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None
    return data


def _check_condition(payload: Any, condition: dict[str, Any]) -> bool:
    """Return *True* when *payload* satisfies *condition*.

    Supported operators (keys in *condition*):
      - ``field`` (required): dot-path into *payload*
      - ``equals``: exact match
      - ``in``: value must be in the given list
      - ``not_equals``: value must differ
    """
    field_path = condition.get("field")
    if not field_path:
        return False
    value = _extract_field(payload, field_path)

    if "equals" in condition:
        return value == condition["equals"]
    if "in" in condition:
        return value in condition["in"]
    if "not_equals" in condition:
        return value != condition["not_equals"]
    return False


class ExperimentEngine:
    def __init__(self, app) -> None:
        self._app = app
        self._cancel_flags: dict[str, bool] = {}
        self._pending_approvals: dict[str, asyncio.Future] = {}

    async def run(self, run_id: str, template: TemplateDef, params: dict[str, Any]) -> None:
        self._cancel_flags[run_id] = False
        started_at = _now()

        await self._db(self._sync_update_run, run_id, STATUS_RUNNING, started_at, None, None)
        await self._broadcast({"type": "experiment_started", "run_id": run_id, "ts": started_at.isoformat()})
        log.info("Experiment run %s started", run_id)

        try:
            resolved = substitute_params(template, params)
        except ValueError as exc:
            await self._abort_run(run_id, str(exc))
            return

        step_results: dict[str, Any] = {}

        for idx, step in enumerate(resolved.steps):
            if self._cancel_flags.get(run_id):
                await self._cancel_run(run_id, idx)
                return

            success, result = await self._run_step_with_retries(run_id, idx, step, step_results)

            if self._cancel_flags.get(run_id):
                await self._cancel_run(run_id, idx)
                return

            if success:
                step_results[step.name] = result
            elif step.on_failure == "abort":
                await self._abort_run(run_id, f"Step '{step.name}' failed: {result}")
                return
            else:
                log.warning("Run %s step '%s' failed (continuing): %s", run_id, step.name, result)

        await self._cleanup_topic_links(run_id)
        ended_at = _now()
        await self._db(self._sync_update_run, run_id, STATUS_COMPLETED, None, ended_at, None)
        await self._broadcast({"type": "experiment_completed", "run_id": run_id, "ts": ended_at.isoformat()})
        log.info("Experiment run %s completed", run_id)
        self._cancel_flags.pop(run_id, None)

    def cancel(self, run_id: str) -> None:
        self._cancel_flags[run_id] = True
        # Also resolve any pending approval so the engine unblocks
        future = self._pending_approvals.pop(run_id, None)
        if future and not future.done():
            future.cancel()
        log.info("Cancel requested for run %s", run_id)

    def approve(self, run_id: str) -> None:
        """Resolve a pending approval step so the experiment continues."""
        future = self._pending_approvals.pop(run_id, None)
        if future is None or future.done():
            raise ValueError(f"No pending approval for run '{run_id}'")
        future.set_result({"approved": True, "approved_at": _now().isoformat()})
        log.info("Approval granted for run %s", run_id)

    async def _cancel_run(self, run_id: str, step_index: int) -> None:
        await self._cleanup_topic_links(run_id)
        ended_at = _now()
        await self._db(self._sync_update_run, run_id, STATUS_CANCELLED, None, ended_at, "Cancelled by user")
        await self._broadcast(
            {"type": "experiment_cancelled", "run_id": run_id, "step_index": step_index, "ts": ended_at.isoformat()}
        )
        log.info("Run %s cancelled at step %d", run_id, step_index)
        self._cancel_flags.pop(run_id, None)

    async def _run_step_with_retries(
        self,
        run_id: str,
        step_index: int,
        step: StepDef,
        step_results: dict[str, Any],
    ) -> tuple[bool, Any]:
        last_error: Any = None
        max_attempts = step.retries + 1

        for attempt in range(max_attempts):
            request_payload = self._step_request_payload(step, step_results)
            step_id = await self._db(
                self._sync_insert_step_start,
                run_id,
                step_index,
                step,
                attempt,
                request_payload,
            )
            started_at = _now()

            await self._broadcast(
                {
                    "type": "step_started",
                    "run_id": run_id,
                    "step_index": step_index,
                    "step_name": step.name,
                    "attempt": attempt,
                    "ts": started_at.isoformat(),
                }
            )

            try:
                result = await self._execute_step(step, step_results, run_id=run_id, step_index=step_index)
                ended_at = _now()
                duration_ms = int((ended_at - started_at).total_seconds() * 1000)
                await self._db(
                    self._sync_update_step_done,
                    step_id,
                    STATUS_COMPLETED,
                    result,
                    ended_at,
                    duration_ms,
                )
                await self._broadcast(
                    {
                        "type": "step_completed",
                        "run_id": run_id,
                        "step_index": step_index,
                        "step_name": step.name,
                        "attempt": attempt,
                        "duration_ms": duration_ms,
                        "result": result,
                        "ts": ended_at.isoformat(),
                    }
                )
                return True, result
            except Exception as exc:  # noqa: BLE001
                ended_at = _now()
                duration_ms = int((ended_at - started_at).total_seconds() * 1000)
                last_error = str(exc) or type(exc).__name__
                await self._db(
                    self._sync_update_step_done,
                    step_id,
                    STATUS_FAILED,
                    {"error": last_error},
                    ended_at,
                    duration_ms,
                )
                await self._broadcast(
                    {
                        "type": "step_failed",
                        "run_id": run_id,
                        "step_index": step_index,
                        "step_name": step.name,
                        "attempt": attempt,
                        "error": last_error,
                        "ts": ended_at.isoformat(),
                    }
                )
                log.warning(
                    "Run %s step '%s' attempt %d/%d failed: %s",
                    run_id,
                    step.name,
                    attempt + 1,
                    max_attempts,
                    exc,
                )
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2 ** attempt)

        return False, last_error

    async def _execute_step(
        self, step: StepDef, step_results: dict[str, Any], run_id: str = "", step_index: int = 0,
    ) -> Any:
        if step.type == "command":
            return await self._execute_command(step, step_results)
        if step.type == "delay":
            return await self._execute_delay(step)
        if step.type == "parallel":
            return await self._execute_parallel(
                step, step_results, run_id=run_id, parent_step_index=step_index,
            )
        if step.type == "topic_link":
            return await self._execute_topic_link(step, run_id=run_id)
        if step.type == "approval":
            return await self._execute_approval(step, run_id=run_id)
        if step.type == "wait_for_condition":
            return await self._execute_wait_for_condition(step, run_id=run_id)
        raise ValueError(f"Unknown step type '{step.type}'")

    async def _execute_command(self, step: StepDef, step_results: dict[str, Any]) -> dict:
        if not step.agent_id:
            raise ValueError(f"Step '{step.name}' is missing agent_id")
        if not step.action:
            raise ValueError(f"Step '{step.name}' is missing action")

        resolved_params = resolve_params_in_step(step.params, step_results)
        timeout = float(step.timeout_s) if isinstance(step.timeout_s, str) else step.timeout_s
        response = await send_command(
            self._app,
            agent_id=step.agent_id,
            component_id=step.component_id,
            action=step.action,
            body=resolved_params,
            wait=True,
            timeout_s=timeout,
        )
        return response.get("result")

    async def _execute_delay(self, step: StepDef) -> dict:
        duration = float(step.duration_s) if isinstance(step.duration_s, str) else step.duration_s
        if duration is None:
            raise ValueError(f"Delay step '{step.name}' has no duration_s")
        await asyncio.sleep(duration)
        return {"slept_s": duration}

    async def _execute_parallel(
        self,
        step: StepDef,
        step_results: dict[str, Any],
        run_id: str = "",
        parent_step_index: int = 0,
    ) -> dict:
        async def _run_sub(sub_index: int, sub: StepDef) -> tuple[str, bool, Any]:
            # Each sub-step gets its own DB record and retry logic
            step_idx = parent_step_index * 1000 + sub_index
            success, result = await self._run_step_with_retries(run_id, step_idx, sub, step_results)
            return sub.name, success, result

        results_list = await asyncio.gather(
            *[_run_sub(i, sub) for i, sub in enumerate(step.steps or [])],
            return_exceptions=True,
        )
        combined: dict[str, Any] = {}
        errors: list[str] = []
        for item in results_list:
            if isinstance(item, BaseException):
                errors.append(str(item))
            else:
                name, success, result = item
                if success:
                    combined[name] = result
                else:
                    errors.append(f"{name}: {result}")

        if errors:
            raise RuntimeError(f"Parallel sub-steps failed: {'; '.join(errors)}")
        return combined

    async def _execute_approval(self, step: StepDef, run_id: str = "") -> dict:
        """Pause execution until a human (dashboard / AI / API) approves."""
        timeout = float(step.timeout_s) if isinstance(step.timeout_s, str) else step.timeout_s
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending_approvals[run_id] = future

        await self._broadcast({
            "type": "approval_required",
            "run_id": run_id,
            "step_name": step.name,
            "message": step.message or "",
            "ts": _now().isoformat(),
        })
        log.info("Run %s waiting for approval: %s", run_id, step.message)

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_approvals.pop(run_id, None)
            raise RuntimeError(f"Approval timed out after {timeout}s") from None
        except asyncio.CancelledError:
            self._pending_approvals.pop(run_id, None)
            raise RuntimeError("Approval cancelled") from None

        await self._broadcast({
            "type": "approval_granted",
            "run_id": run_id,
            "step_name": step.name,
            "ts": _now().isoformat(),
        })
        return result

    async def _execute_wait_for_condition(self, step: StepDef, run_id: str = "") -> dict:
        """Watch a telemetry topic until a condition is met or timeout."""
        if not step.agent_id or not step.telemetry_metric or not step.condition:
            raise ValueError(f"Step '{step.name}' is missing required wait_for_condition fields")

        # Build the full MQTT topic to watch
        if step.component_id:
            topic = (
                f"lucid/agents/{step.agent_id}/components/{step.component_id}"
                f"/telemetry/{step.telemetry_metric}"
            )
        else:
            topic = f"lucid/agents/{step.agent_id}/telemetry/{step.telemetry_metric}"

        timeout = float(step.timeout_s) if isinstance(step.timeout_s, str) else step.timeout_s
        future: asyncio.Future = asyncio.get_running_loop().create_future()

        # Register a watcher on the mqtt bridge
        bridge = self._app.state.bridge
        loop = asyncio.get_running_loop()

        def _on_telemetry(payload: Any) -> None:
            if future.done():
                return
            if _check_condition(payload, step.condition):
                loop.call_soon_threadsafe(future.set_result, payload)

        watcher_id = bridge.add_telemetry_watcher(topic, _on_telemetry)
        log.info("Run %s waiting for condition on %s: %s", run_id, topic, step.condition)

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            if step.on_timeout == "continue":
                log.warning("Run %s condition timed out on %s (continuing)", run_id, topic)
                result = {"timed_out": True, "timeout_s": timeout}
            else:
                raise RuntimeError(
                    f"Condition on '{step.telemetry_metric}' timed out after {timeout}s"
                ) from None
        finally:
            bridge.remove_telemetry_watcher(watcher_id)

        return result

    async def _execute_topic_link(self, step: StepDef, run_id: str = "") -> dict:
        operation = step.operation or "create"
        if operation == "create":
            loop = asyncio.get_running_loop()
            row = await loop.run_in_executor(
                None,
                lambda: topic_link_service.create_topic_link(
                    self._app,
                    name=step.name,
                    source_topic=step.source_topic or "",
                    target_topic=step.target_topic or "",
                    select_clause=step.select_clause,
                    payload_template=step.payload_template,
                    qos=step.qos,
                    owner_type="experiment-run",
                    owner_id=run_id,
                ),
            )
            await self._broadcast({"type": "topic_link_created", "link_id": row["id"]})
            return {
                "ok": True,
                "link_id": row["id"],
                "rule_id": row["emqx_rule_id"],
                "operation": "create",
                "source_topic": row["source_topic"],
                "target_topic": row["target_topic"],
            }

        with DB.connect() as conn:
            existing = topic_link_service.find_owned_topic_link(
                conn,
                owner_type="experiment-run",
                owner_id=run_id,
                source_topic=step.source_topic or "",
                target_topic=step.target_topic or "",
            )
        if existing is None:
            raise RuntimeError(
                f"No experiment-owned topic link found for {step.source_topic} -> {step.target_topic}"
            )

        loop = asyncio.get_running_loop()
        if operation == "activate":
            row = await loop.run_in_executor(
                None,
                lambda: topic_link_service.activate_topic_link(self._app, existing["id"], ignore_lock=True),
            )
            await self._broadcast({"type": "topic_link_updated", "link_id": existing["id"]})
            return {"ok": True, "link_id": row["id"], "rule_id": row["emqx_rule_id"], "operation": "activate"}
        if operation == "deactivate":
            row = await loop.run_in_executor(
                None,
                lambda: topic_link_service.deactivate_topic_link(self._app, existing["id"], ignore_lock=True),
            )
            await self._broadcast({"type": "topic_link_updated", "link_id": existing["id"]})
            return {"ok": True, "link_id": row["id"], "rule_id": row["emqx_rule_id"], "operation": "deactivate"}
        if operation == "delete":
            row = await loop.run_in_executor(
                None,
                lambda: topic_link_service.delete_topic_link(self._app, existing["id"], ignore_lock=True),
            )
            await self._broadcast({"type": "topic_link_deleted", "link_id": existing["id"]})
            return {"ok": True, "link_id": row["id"], "rule_id": row["emqx_rule_id"], "operation": "delete"}

        raise ValueError(f"Unknown topic_link operation '{operation}'")

    async def _cleanup_topic_links(self, run_id: str) -> None:
        loop = asyncio.get_running_loop()
        removed = await loop.run_in_executor(
            None,
            lambda: topic_link_service.delete_owned_topic_links(self._app, "experiment-run", run_id),
        )
        for row in removed:
            await self._broadcast({"type": "topic_link_deleted", "link_id": row["id"]})

    async def _abort_run(self, run_id: str, error: str) -> None:
        await self._cleanup_topic_links(run_id)
        ended_at = _now()
        await self._db(self._sync_update_run, run_id, STATUS_FAILED, None, ended_at, error)
        await self._broadcast({"type": "experiment_failed", "run_id": run_id, "error": error, "ts": ended_at.isoformat()})
        log.error("Experiment run %s failed: %s", run_id, error)
        self._cancel_flags.pop(run_id, None)

    def _step_request_payload(self, step: StepDef, step_results: dict[str, Any]) -> Any:
        if step.type == "command":
            return {
                "agent_id": step.agent_id,
                "component_id": step.component_id,
                "action": step.action,
                "params": resolve_params_in_step(step.params, step_results),
                "timeout_s": step.timeout_s,
            }
        if step.type == "delay":
            return {"duration_s": step.duration_s}
        if step.type == "parallel":
            return {"steps": [sub.model_dump() for sub in (step.steps or [])]}
        if step.type == "topic_link":
            return {
                "operation": step.operation,
                "source_topic": step.source_topic,
                "target_topic": step.target_topic,
                "select_clause": step.select_clause,
                "payload_template": step.payload_template,
                "qos": step.qos,
            }
        if step.type == "approval":
            return {"message": step.message}
        if step.type == "wait_for_condition":
            return {
                "agent_id": step.agent_id,
                "component_id": step.component_id,
                "telemetry_metric": step.telemetry_metric,
                "condition": step.condition,
                "on_timeout": step.on_timeout,
            }
        return None

    async def _db(self, fn, *args) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, fn, *args)

    def _sync_update_run(
        self,
        run_id: str,
        status: str,
        started_at: datetime | None,
        ended_at: datetime | None,
        error: str | None,
    ) -> None:
        with DB.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE experiment_runs
                    SET status = %s,
                        started_at = COALESCE(%s, started_at),
                        ended_at = COALESCE(%s, ended_at),
                        error = CASE WHEN %s IS NULL THEN error ELSE %s END
                    WHERE id = %s
                    """,
                    (status, started_at, ended_at, error, error, run_id),
                )
            conn.commit()

    def _sync_insert_step_start(
        self,
        run_id: str,
        step_index: int,
        step: StepDef,
        attempt: int,
        request_payload: Any,
    ) -> int:
        with DB.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO experiment_steps (
                        run_id, step_index, step_name, agent_id, component_id,
                        action, request_payload, status, attempt, started_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        run_id,
                        step_index,
                        step.name,
                        step.agent_id,
                        step.component_id,
                        step.action or f"{step.type}:{getattr(step, 'operation', '')}".rstrip(":"),
                        psycopg2.extras.Json(request_payload) if request_payload is not None else None,
                        STATUS_RUNNING,
                        attempt,
                        _now(),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return row[0]

    def _sync_update_step_done(
        self,
        step_id: int,
        status: str,
        response: Any,
        ended_at: datetime,
        duration_ms: int,
    ) -> None:
        with DB.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE experiment_steps
                    SET status = %s,
                        response_payload = %s,
                        ended_at = %s,
                        duration_ms = %s
                    WHERE id = %s
                    """,
                    (
                        status,
                        psycopg2.extras.Json(response) if response is not None else None,
                        ended_at,
                        duration_ms,
                        step_id,
                    ),
                )
            conn.commit()

    async def _broadcast(self, event: dict) -> None:
        await broadcast_ws(self._app.state.ws_clients, event)
