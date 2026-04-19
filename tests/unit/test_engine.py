"""Unit tests for the experiment engine.

Covers:
  - Pure helper functions: _extract_field, _check_condition
  - ExperimentEngine lifecycle: run, cancel, abort, approval
  - Step execution: command, delay, parallel, wait_for_condition
  - Retry logic and on_failure behaviour
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.experiments.engine import (
    ExperimentEngine,
    _check_condition,
    _extract_field,
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_RUNNING,
)
from app.experiments.models import StepDef, TemplateDef
from tests.conftest import (
    FakeBridge,
    FakeRequestResponseManager,
    FakeWebSocketManager,
    make_app,
)


# ---------------------------------------------------------------------------
# _extract_field
# ---------------------------------------------------------------------------
class TestExtractField:
    def test_simple_key(self):
        assert _extract_field({"value": 42}, "value") == 42

    def test_nested_key(self):
        assert _extract_field({"a": {"b": {"c": "deep"}}}, "a.b.c") == "deep"

    def test_missing_key_returns_none(self):
        assert _extract_field({"a": 1}, "b") is None

    def test_missing_nested_key_returns_none(self):
        assert _extract_field({"a": {"b": 1}}, "a.c") is None

    def test_non_dict_returns_none(self):
        assert _extract_field(42, "a") is None

    def test_none_data_returns_none(self):
        assert _extract_field(None, "a") is None

    def test_json_string_auto_parse(self):
        """Strings that contain valid JSON dicts are auto-parsed mid-path."""
        data = {"value": json.dumps({"state": "ok"})}
        assert _extract_field(data, "value.state") == "ok"

    def test_nested_json_string_auto_parse(self):
        """Double-encoded JSON (ROS std_msgs/String style)."""
        inner = json.dumps({"state": "running"})
        data = {"value": {"data": inner}}
        assert _extract_field(data, "value.data.state") == "running"

    def test_json_string_non_dict_returns_none(self):
        data = {"value": json.dumps([1, 2, 3])}
        assert _extract_field(data, "value.x") is None

    def test_invalid_json_string_returns_none(self):
        data = {"value": "not json"}
        assert _extract_field(data, "value.x") is None

    def test_empty_path_segment(self):
        # Edge case: empty string field path should still traverse safely
        assert _extract_field({"": "hello"}, "") == "hello"


# ---------------------------------------------------------------------------
# _check_condition
# ---------------------------------------------------------------------------
class TestCheckCondition:
    def test_equals_match(self):
        assert _check_condition({"status": "ok"}, {"field": "status", "equals": "ok"}) is True

    def test_equals_mismatch(self):
        assert _check_condition({"status": "error"}, {"field": "status", "equals": "ok"}) is False

    def test_in_match(self):
        assert _check_condition({"v": 2}, {"field": "v", "in": [1, 2, 3]}) is True

    def test_in_mismatch(self):
        assert _check_condition({"v": 5}, {"field": "v", "in": [1, 2, 3]}) is False

    def test_not_equals_match(self):
        assert _check_condition({"s": "a"}, {"field": "s", "not_equals": "b"}) is True

    def test_not_equals_mismatch(self):
        assert _check_condition({"s": "b"}, {"field": "s", "not_equals": "b"}) is False

    def test_less_than_true(self):
        assert _check_condition({"temp": 10}, {"field": "temp", "less_than": 20}) is True

    def test_less_than_false(self):
        assert _check_condition({"temp": 30}, {"field": "temp", "less_than": 20}) is False

    def test_greater_than_true(self):
        assert _check_condition({"temp": 30}, {"field": "temp", "greater_than": 20}) is True

    def test_greater_than_false(self):
        assert _check_condition({"temp": 10}, {"field": "temp", "greater_than": 20}) is False

    def test_less_than_non_numeric_returns_false(self):
        assert _check_condition({"temp": "abc"}, {"field": "temp", "less_than": 20}) is False

    def test_greater_than_non_numeric_returns_false(self):
        assert _check_condition({"temp": None}, {"field": "temp", "greater_than": 20}) is False

    def test_missing_field_key_returns_false(self):
        assert _check_condition({"x": 1}, {}) is False

    def test_no_operator_returns_false(self):
        assert _check_condition({"x": 1}, {"field": "x"}) is False

    def test_nested_field_equals(self):
        payload = {"value": {"state": "ready"}}
        assert _check_condition(payload, {"field": "value.state", "equals": "ready"}) is True

    def test_equals_with_numeric(self):
        assert _check_condition({"n": 42}, {"field": "n", "equals": 42}) is True

    def test_less_than_with_string_number(self):
        """String values that are numeric should still compare correctly."""
        assert _check_condition({"temp": "5"}, {"field": "temp", "less_than": 10}) is True


# ---------------------------------------------------------------------------
# ExperimentEngine helpers
# ---------------------------------------------------------------------------
def _make_template(steps: list[dict], params: dict | None = None) -> TemplateDef:
    """Build a minimal TemplateDef for testing."""
    return TemplateDef(
        id="test-tmpl",
        name="Test Template",
        steps=[StepDef(**s) for s in steps],
        parameters={k: {"type": "string", "default": v} for k, v in (params or {}).items()},
    )


def _patch_db():
    """Patch all synchronous DB calls in the engine so they no-op."""
    return patch.multiple(
        "app.experiments.engine.DB",
        connect=MagicMock(return_value=MagicMock(
            __enter__=MagicMock(return_value=MagicMock(
                cursor=MagicMock(return_value=MagicMock(
                    __enter__=MagicMock(return_value=MagicMock(
                        execute=MagicMock(),
                        fetchone=MagicMock(return_value=(1,)),
                    )),
                    __exit__=MagicMock(return_value=False),
                )),
                commit=MagicMock(),
            )),
            __exit__=MagicMock(return_value=False),
        )),
    )


def _make_engine(fake_app=None, rrm_result=None):
    """Create engine with a fake app."""
    if fake_app is None:
        rrm = FakeRequestResponseManager(rrm_result or {"ok": True})
        fake_app = make_app(rrm=rrm)
    return ExperimentEngine(fake_app), fake_app


# ---------------------------------------------------------------------------
# ExperimentEngine.run — single delay step (simplest happy path)
# ---------------------------------------------------------------------------
class TestEngineRunDelay:
    @pytest.mark.asyncio
    async def test_single_delay_step_completes(self):
        engine, app = _make_engine()
        template = _make_template([
            {"name": "wait", "type": "delay", "duration_s": 0.01},
        ])

        with _patch_db():
            await engine.run("run-1", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_started" in types
        assert "step_started" in types
        assert "step_completed" in types
        assert "experiment_completed" in types

    @pytest.mark.asyncio
    async def test_delay_step_result_contains_slept_s(self):
        engine, app = _make_engine()
        template = _make_template([
            {"name": "wait", "type": "delay", "duration_s": 0.01},
        ])

        with _patch_db():
            await engine.run("run-2", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        completed_events = [e for e in ws.events if e["type"] == "step_completed"]
        assert len(completed_events) == 1
        assert completed_events[0]["result"]["slept_s"] == 0.01


# ---------------------------------------------------------------------------
# ExperimentEngine.run — command step
# ---------------------------------------------------------------------------
class TestEngineRunCommand:
    @pytest.mark.asyncio
    async def test_command_step_sends_command_and_completes(self):
        rrm_result = {"ok": True, "data": "hello"}
        engine, app = _make_engine(rrm_result=rrm_result)
        template = _make_template([
            {
                "name": "ping",
                "type": "command",
                "agent_id": "agent-01",
                "action": "ping",
                "timeout_s": 1.0,
            },
        ])

        with _patch_db(), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"result": {"ok": True, "data": "hello"}}
            await engine.run("run-cmd", template, {})

        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args
        assert call_kwargs.kwargs["agent_id"] == "agent-01"
        assert call_kwargs.kwargs["action"] == "ping"
        assert call_kwargs.kwargs["wait"] is True

    @pytest.mark.asyncio
    async def test_command_step_missing_agent_id_fails(self):
        engine, app = _make_engine()
        template = _make_template([
            {"name": "bad", "type": "command", "action": "do-thing"},
        ])

        with _patch_db(), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.side_effect = ValueError("Step 'bad' is missing agent_id")
            await engine.run("run-bad-cmd", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_failed" in types

    @pytest.mark.asyncio
    async def test_command_step_with_component_id(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "led",
                "type": "command",
                "agent_id": "agent-01",
                "component_id": "led-strip",
                "action": "set-color",
                "params": {"color": "red"},
                "timeout_s": 1.0,
            },
        ])

        with _patch_db(), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"result": {"ok": True}}
            await engine.run("run-led", template, {})

        call_kwargs = mock_send.call_args
        assert call_kwargs.kwargs["component_id"] == "led-strip"
        assert call_kwargs.kwargs["body"] == {"color": "red"}


# ---------------------------------------------------------------------------
# ExperimentEngine.run — parallel steps
# ---------------------------------------------------------------------------
class TestEngineRunParallel:
    @pytest.mark.asyncio
    async def test_parallel_steps_all_succeed(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "parallel-group",
                "type": "parallel",
                "steps": [
                    {"name": "d1", "type": "delay", "duration_s": 0.01},
                    {"name": "d2", "type": "delay", "duration_s": 0.01},
                ],
            },
        ])

        with _patch_db():
            await engine.run("run-par", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_completed" in types
        assert types.count("step_completed") >= 3  # 2 sub-steps + 1 parent

    @pytest.mark.asyncio
    async def test_parallel_step_sub_failure_fails_parent(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "par",
                "type": "parallel",
                "steps": [
                    {"name": "ok-step", "type": "delay", "duration_s": 0.01},
                    {
                        "name": "bad-step",
                        "type": "command",
                        "agent_id": "a1",
                        "action": "fail",
                        "timeout_s": 0.5,
                    },
                ],
            },
        ])

        with _patch_db(), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.side_effect = RuntimeError("oops")
            await engine.run("run-par-fail", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        # The parallel step itself should fail, aborting the experiment
        assert "experiment_failed" in types


# ---------------------------------------------------------------------------
# ExperimentEngine.cancel
# ---------------------------------------------------------------------------
class TestEngineCancel:
    @pytest.mark.asyncio
    async def test_cancel_sets_flag_and_aborts(self):
        engine, app = _make_engine()
        template = _make_template([
            {"name": "long-wait", "type": "delay", "duration_s": 10.0},
        ])

        async def cancel_after_start():
            # Give the engine a moment to start
            await asyncio.sleep(0.05)
            engine.cancel("run-cancel")

        with _patch_db():
            await asyncio.gather(
                engine.run("run-cancel", template, {}),
                cancel_after_start(),
            )

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_cancelled" in types

    @pytest.mark.asyncio
    async def test_cancel_before_run_has_no_effect(self):
        """run() resets the cancel flag, so a pre-cancel is harmless."""
        engine, app = _make_engine()
        template = _make_template([
            {"name": "step1", "type": "delay", "duration_s": 0.01},
        ])

        engine.cancel("run-pre")

        with _patch_db():
            await engine.run("run-pre", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        # The run proceeds normally because run() clears the cancel flag
        assert "experiment_completed" in types
        assert "experiment_cancelled" not in types


# ---------------------------------------------------------------------------
# ExperimentEngine.run — retries
# ---------------------------------------------------------------------------
class TestEngineRetries:
    @pytest.mark.asyncio
    async def test_step_retries_on_failure(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "flaky",
                "type": "command",
                "agent_id": "a1",
                "action": "flaky-cmd",
                "retries": 2,
                "timeout_s": 0.5,
            },
        ])

        call_count = 0

        async def flaky_send(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError(f"Fail attempt {call_count}")
            return {"result": {"ok": True}}

        with _patch_db(), \
             patch("app.experiments.engine.send_command", side_effect=flaky_send):
            await engine.run("run-retry", template, {})

        assert call_count == 3

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_completed" in types
        assert types.count("step_failed") == 2
        assert types.count("step_completed") == 1

    @pytest.mark.asyncio
    async def test_retries_exhausted_aborts(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "always-fail",
                "type": "command",
                "agent_id": "a1",
                "action": "bad",
                "retries": 1,
                "on_failure": "abort",
                "timeout_s": 0.5,
            },
        ])

        with _patch_db(), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.side_effect = RuntimeError("always fails")
            await engine.run("run-exhaust", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_failed" in types


# ---------------------------------------------------------------------------
# ExperimentEngine.run — on_failure=continue
# ---------------------------------------------------------------------------
class TestEngineOnFailureContinue:
    @pytest.mark.asyncio
    async def test_on_failure_continue_proceeds_to_next_step(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "fail-ok",
                "type": "command",
                "agent_id": "a1",
                "action": "risky",
                "on_failure": "continue",
                "timeout_s": 0.5,
            },
            {"name": "next", "type": "delay", "duration_s": 0.01},
        ])

        with _patch_db(), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.side_effect = RuntimeError("oops")
            await engine.run("run-cont", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_completed" in types
        # The failed step should be reported
        step_names = [e.get("step_name") for e in ws.events if e["type"] == "step_failed"]
        assert "fail-ok" in step_names
        # The next step should also run
        completed_names = [e.get("step_name") for e in ws.events if e["type"] == "step_completed"]
        assert "next" in completed_names


# ---------------------------------------------------------------------------
# ExperimentEngine — approval step
# ---------------------------------------------------------------------------
class TestEngineApproval:
    @pytest.mark.asyncio
    async def test_approval_step_waits_and_resolves(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "approve-step",
                "type": "approval",
                "message": "Please approve",
                "timeout_s": 5.0,
            },
        ])

        async def approve_soon():
            await asyncio.sleep(0.05)
            engine.approve("run-approve")

        with _patch_db():
            await asyncio.gather(
                engine.run("run-approve", template, {}),
                approve_soon(),
            )

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "approval_required" in types
        assert "approval_granted" in types
        assert "experiment_completed" in types

    @pytest.mark.asyncio
    async def test_approval_timeout_fails_step(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "approve-timeout",
                "type": "approval",
                "message": "Approve fast!",
                "timeout_s": 0.1,
            },
        ])

        with _patch_db():
            await engine.run("run-approve-to", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_failed" in types

    def test_approve_no_pending_raises(self):
        engine, _ = _make_engine()
        with pytest.raises(ValueError, match="No pending approval"):
            engine.approve("run-nonexistent")

    @pytest.mark.asyncio
    async def test_cancel_during_approval_cancels_run(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "approve-cancel",
                "type": "approval",
                "message": "Waiting...",
                "timeout_s": 10.0,
            },
        ])

        async def cancel_soon():
            await asyncio.sleep(0.05)
            engine.cancel("run-approve-cancel")

        with _patch_db():
            await asyncio.gather(
                engine.run("run-approve-cancel", template, {}),
                cancel_soon(),
            )

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        # Should either be cancelled or failed (approval cancelled raises RuntimeError)
        assert "experiment_failed" in types or "experiment_cancelled" in types


# ---------------------------------------------------------------------------
# ExperimentEngine — wait_for_condition step
# ---------------------------------------------------------------------------
class TestEngineWaitForCondition:
    @pytest.mark.asyncio
    async def test_condition_met_immediately(self):
        bridge = FakeBridge()
        engine, app = _make_engine(fake_app=make_app(bridge=bridge))
        template = _make_template([
            {
                "name": "wait-cond",
                "type": "wait_for_condition",
                "agent_id": "a1",
                "telemetry_metric": "cpu",
                "condition": {"field": "value", "greater_than": 50},
                "timeout_s": 2.0,
            },
        ])

        async def trigger_telemetry():
            await asyncio.sleep(0.05)
            # Simulate the bridge calling the watcher
            for _wid, (topic, cb) in list(bridge._watchers.items()):
                cb({"value": 75})

        with _patch_db():
            await asyncio.gather(
                engine.run("run-cond", template, {}),
                trigger_telemetry(),
            )

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_completed" in types

    @pytest.mark.asyncio
    async def test_condition_timeout_on_abort(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "wait-timeout",
                "type": "wait_for_condition",
                "agent_id": "a1",
                "telemetry_metric": "cpu",
                "condition": {"field": "value", "greater_than": 50},
                "timeout_s": 0.1,
                "on_timeout": "abort",
            },
        ])

        with _patch_db():
            await engine.run("run-cond-to", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_failed" in types

    @pytest.mark.asyncio
    async def test_condition_timeout_on_continue(self):
        engine, app = _make_engine()
        template = _make_template([
            {
                "name": "wait-cont",
                "type": "wait_for_condition",
                "agent_id": "a1",
                "telemetry_metric": "cpu",
                "condition": {"field": "value", "equals": "impossible"},
                "timeout_s": 0.1,
                "on_timeout": "continue",
            },
            {"name": "after", "type": "delay", "duration_s": 0.01},
        ])

        with _patch_db():
            await engine.run("run-cond-cont", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_completed" in types
        completed_names = [e.get("step_name") for e in ws.events if e["type"] == "step_completed"]
        assert "wait-cont" in completed_names
        assert "after" in completed_names

    @pytest.mark.asyncio
    async def test_condition_with_component_id_builds_correct_topic(self):
        bridge = FakeBridge()
        engine, app = _make_engine(fake_app=make_app(bridge=bridge))
        template = _make_template([
            {
                "name": "wait-comp",
                "type": "wait_for_condition",
                "agent_id": "a1",
                "component_id": "sensor-01",
                "telemetry_metric": "temp",
                "condition": {"field": "value", "less_than": 100},
                "timeout_s": 2.0,
            },
        ])

        async def trigger_telemetry():
            await asyncio.sleep(0.05)
            for _wid, (topic, cb) in list(bridge._watchers.items()):
                assert topic == "lucid/agents/a1/components/sensor-01/telemetry/temp"
                cb({"value": 50})

        with _patch_db():
            await asyncio.gather(
                engine.run("run-comp-cond", template, {}),
                trigger_telemetry(),
            )

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_completed" in types


# ---------------------------------------------------------------------------
# ExperimentEngine._step_request_payload
# ---------------------------------------------------------------------------
class TestStepRequestPayload:
    def test_command_payload(self):
        engine, _ = _make_engine()
        step = StepDef(name="cmd", type="command", agent_id="a1", action="do", params={"k": "v"}, timeout_s=5)
        payload = engine._step_request_payload(step, {})
        assert payload["agent_id"] == "a1"
        assert payload["action"] == "do"
        assert payload["params"] == {"k": "v"}
        assert payload["timeout_s"] == 5

    def test_delay_payload(self):
        engine, _ = _make_engine()
        step = StepDef(name="wait", type="delay", duration_s=2.0)
        payload = engine._step_request_payload(step, {})
        assert payload == {"duration_s": 2.0}

    def test_approval_payload(self):
        engine, _ = _make_engine()
        step = StepDef(name="approve", type="approval", message="Check it")
        payload = engine._step_request_payload(step, {})
        assert payload == {"message": "Check it"}

    def test_wait_for_condition_payload(self):
        engine, _ = _make_engine()
        step = StepDef(
            name="wfc",
            type="wait_for_condition",
            agent_id="a1",
            telemetry_metric="cpu",
            condition={"field": "value", "equals": 1},
        )
        payload = engine._step_request_payload(step, {})
        assert payload["agent_id"] == "a1"
        assert payload["telemetry_metric"] == "cpu"
        assert payload["condition"]["field"] == "value"

    def test_topic_link_payload(self):
        engine, _ = _make_engine()
        step = StepDef(
            name="tl",
            type="topic_link",
            source_topic="a/b",
            target_topic="c/d",
            operation="create",
        )
        payload = engine._step_request_payload(step, {})
        assert payload["operation"] == "create"
        assert payload["source_topic"] == "a/b"
        assert payload["target_topic"] == "c/d"

    def test_unknown_type_returns_none(self):
        engine, _ = _make_engine()
        # Manually construct a step with an unknown type (bypass validator)
        step = StepDef.model_construct(name="weird", type="unknown")
        payload = engine._step_request_payload(step, {})
        assert payload is None


# ---------------------------------------------------------------------------
# ExperimentEngine — multi-step runs
# ---------------------------------------------------------------------------
class TestEngineMultiStep:
    @pytest.mark.asyncio
    async def test_three_delay_steps_execute_in_order(self):
        engine, app = _make_engine()
        template = _make_template([
            {"name": "d1", "type": "delay", "duration_s": 0.01},
            {"name": "d2", "type": "delay", "duration_s": 0.01},
            {"name": "d3", "type": "delay", "duration_s": 0.01},
        ])

        with _patch_db():
            await engine.run("run-multi", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        completed = [e for e in ws.events if e["type"] == "step_completed"]
        assert len(completed) == 3
        assert [e["step_name"] for e in completed] == ["d1", "d2", "d3"]
        assert [e["step_index"] for e in completed] == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_abort_at_second_step_skips_third(self):
        engine, app = _make_engine()
        template = _make_template([
            {"name": "s1", "type": "delay", "duration_s": 0.01},
            {
                "name": "s2",
                "type": "command",
                "agent_id": "a1",
                "action": "fail",
                "on_failure": "abort",
                "timeout_s": 0.5,
            },
            {"name": "s3", "type": "delay", "duration_s": 0.01},
        ])

        with _patch_db(), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.side_effect = RuntimeError("boom")
            await engine.run("run-abort-mid", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_failed" in types
        # s3 should never have started
        started_names = [e.get("step_name") for e in ws.events if e["type"] == "step_started"]
        assert "s3" not in started_names


# ---------------------------------------------------------------------------
# ExperimentEngine.run — unknown step type
# ---------------------------------------------------------------------------
class TestEngineUnknownStepType:
    @pytest.mark.asyncio
    async def test_unknown_step_type_aborts_run(self):
        engine, app = _make_engine()
        # Use model_construct to bypass validator
        bad_step = StepDef.model_construct(name="mystery", type="wormhole")
        template = TemplateDef(
            id="bad-tmpl", name="Bad", steps=[bad_step],
        )

        with _patch_db():
            await engine.run("run-unknown", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_failed" in types


# ---------------------------------------------------------------------------
# ExperimentEngine — broadcast event structure
# ---------------------------------------------------------------------------
class TestBroadcastEventStructure:
    @pytest.mark.asyncio
    async def test_started_event_has_required_fields(self):
        engine, app = _make_engine()
        template = _make_template([
            {"name": "d", "type": "delay", "duration_s": 0.01},
        ])

        with _patch_db():
            await engine.run("run-evt", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        started = [e for e in ws.events if e["type"] == "experiment_started"][0]
        assert "run_id" in started
        assert "ts" in started
        assert started["run_id"] == "run-evt"

    @pytest.mark.asyncio
    async def test_step_completed_event_has_duration(self):
        engine, app = _make_engine()
        template = _make_template([
            {"name": "d", "type": "delay", "duration_s": 0.01},
        ])

        with _patch_db():
            await engine.run("run-dur", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        completed = [e for e in ws.events if e["type"] == "step_completed"][0]
        assert "duration_ms" in completed
        assert isinstance(completed["duration_ms"], int)
        assert completed["duration_ms"] >= 0

    @pytest.mark.asyncio
    async def test_completed_event_has_required_fields(self):
        engine, app = _make_engine()
        template = _make_template([
            {"name": "d", "type": "delay", "duration_s": 0.01},
        ])

        with _patch_db():
            await engine.run("run-done", template, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        done = [e for e in ws.events if e["type"] == "experiment_completed"][0]
        assert done["run_id"] == "run-done"
        assert "ts" in done


# ---------------------------------------------------------------------------
# ExperimentEngine — template step type
# ---------------------------------------------------------------------------
def _child_template_def(steps: list[dict], params: dict | None = None) -> dict:
    """Return a raw template definition dict as stored in the DB."""
    return {
        "id": "child-tmpl",
        "name": "Child Template",
        "version": "1.0.0",
        "parameters": {
            k: {"type": "string", "default": v, "required": v is None}
            for k, v in (params or {}).items()
        },
        "steps": steps,
    }


class TestEngineTemplateStep:
    @pytest.mark.asyncio
    async def test_template_step_executes_child_steps(self):
        """A template step should inline-execute all child template steps."""
        engine, app = _make_engine()

        child_def = _child_template_def([
            {"name": "wait_a", "type": "delay", "duration_s": 0.01},
            {"name": "wait_b", "type": "delay", "duration_s": 0.01},
        ])
        parent = _make_template([
            {"name": "run_child", "type": "template", "template_id": "child-tmpl"},
        ])

        with _patch_db(), \
             patch.object(engine, "_sync_load_template", return_value=child_def):
            await engine.run("run-tmpl-1", parent, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_completed" in types
        # Parent template step + 2 child delay steps = 3 step_completed events
        completed = [e for e in ws.events if e["type"] == "step_completed"]
        assert len(completed) == 3

    @pytest.mark.asyncio
    async def test_template_step_passes_params_to_child(self):
        """template_params should substitute into the child template."""
        engine, app = _make_engine()

        child_def = _child_template_def(
            [{"name": "cmd", "type": "command", "agent_id": "${target}", "action": "ping"}],
            params={"target": None},
        )
        parent = _make_template([
            {
                "name": "run_child",
                "type": "template",
                "template_id": "child-tmpl",
                "template_params": {"target": "robot-7"},
            },
        ])

        with _patch_db(), \
             patch.object(engine, "_sync_load_template", return_value=child_def), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"result": {"ok": True}}
            await engine.run("run-tmpl-params", parent, {})

        assert mock_send.call_args.kwargs["agent_id"] == "robot-7"

    @pytest.mark.asyncio
    async def test_template_step_missing_template_fails(self):
        """If the referenced template_id doesn't exist, the run should fail."""
        engine, app = _make_engine()
        parent = _make_template([
            {"name": "run_child", "type": "template", "template_id": "nonexistent"},
        ])

        with _patch_db(), \
             patch.object(engine, "_sync_load_template", return_value=None):
            await engine.run("run-tmpl-missing", parent, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_failed" in types

    @pytest.mark.asyncio
    async def test_template_step_child_abort_propagates(self):
        """If a child step fails with on_failure=abort, the parent run should fail."""
        engine, app = _make_engine()

        child_def = _child_template_def([
            {"name": "bad_cmd", "type": "command", "agent_id": "x", "action": "boom", "on_failure": "abort"},
        ])
        parent = _make_template([
            {"name": "run_child", "type": "template", "template_id": "child-tmpl"},
        ])

        with _patch_db(), \
             patch.object(engine, "_sync_load_template", return_value=child_def), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.side_effect = RuntimeError("kaboom")
            await engine.run("run-tmpl-abort", parent, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_failed" in types

    @pytest.mark.asyncio
    async def test_template_step_child_continue_on_failure(self):
        """A child step with on_failure=continue should not abort the run."""
        engine, app = _make_engine()

        child_def = _child_template_def([
            {"name": "flaky", "type": "command", "agent_id": "x", "action": "boom", "on_failure": "continue"},
            {"name": "ok_step", "type": "delay", "duration_s": 0.01},
        ])
        parent = _make_template([
            {"name": "run_child", "type": "template", "template_id": "child-tmpl"},
        ])

        with _patch_db(), \
             patch.object(engine, "_sync_load_template", return_value=child_def), \
             patch("app.experiments.engine.send_command", new_callable=AsyncMock) as mock_send:
            mock_send.side_effect = RuntimeError("kaboom")
            await engine.run("run-tmpl-continue", parent, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_completed" in types

    @pytest.mark.asyncio
    async def test_template_step_cancellation(self):
        """Cancelling mid-template should stop execution."""
        engine, app = _make_engine()

        child_def = _child_template_def([
            {"name": "long_wait", "type": "delay", "duration_s": 10.0},
        ])
        parent = _make_template([
            {"name": "run_child", "type": "template", "template_id": "child-tmpl"},
        ])

        async def cancel_after_start():
            await asyncio.sleep(0.05)
            engine.cancel("run-tmpl-cancel")

        with _patch_db(), \
             patch.object(engine, "_sync_load_template", return_value=child_def):
            await asyncio.gather(
                engine.run("run-tmpl-cancel", parent, {}),
                cancel_after_start(),
            )

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_cancelled" in types

    @pytest.mark.asyncio
    async def test_template_step_sub_results_returned(self):
        """The template step result should contain all child step results."""
        engine, app = _make_engine()

        child_def = _child_template_def([
            {"name": "w1", "type": "delay", "duration_s": 0.01},
            {"name": "w2", "type": "delay", "duration_s": 0.01},
        ])
        parent = _make_template([
            {"name": "child", "type": "template", "template_id": "child-tmpl"},
        ])

        with _patch_db(), \
             patch.object(engine, "_sync_load_template", return_value=child_def):
            await engine.run("run-tmpl-results", parent, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        # The parent template step's completed event should have the sub-results
        parent_completed = [
            e for e in ws.events
            if e["type"] == "step_completed" and e["step_name"] == "child"
        ]
        assert len(parent_completed) == 1
        result = parent_completed[0]["result"]
        assert "w1" in result
        assert "w2" in result
        assert result["w1"]["slept_s"] == 0.01

    @pytest.mark.asyncio
    async def test_template_step_request_payload(self):
        """The step_started request_payload should contain template_id and params."""
        engine, app = _make_engine()

        child_def = _child_template_def(
            [{"name": "d", "type": "delay", "duration_s": 0.01}],
            params={"x": "default_val"},
        )
        parent = _make_template([
            {
                "name": "t",
                "type": "template",
                "template_id": "child-tmpl",
                "template_params": {"x": "override"},
            },
        ])

        step_payloads = []
        original_insert = engine._sync_insert_step_start

        def _capture_insert(run_id, step_index, step, attempt, request_payload):
            step_payloads.append((step.name, request_payload))
            return 1  # fake step id

        with _patch_db(), \
             patch.object(engine, "_sync_load_template", return_value=child_def), \
             patch.object(engine, "_sync_insert_step_start", side_effect=_capture_insert):
            await engine.run("run-tmpl-payload", parent, {})

        template_payloads = [p for name, p in step_payloads if name == "t"]
        assert len(template_payloads) == 1
        assert template_payloads[0]["template_id"] == "child-tmpl"
        assert template_payloads[0]["template_params"]["x"] == "override"


class TestTemplateStepValidation:
    def test_template_step_requires_template_id(self):
        with pytest.raises(ValueError, match="must specify 'template_id'"):
            StepDef(name="bad", type="template")

    def test_template_step_valid(self):
        step = StepDef(name="ok", type="template", template_id="my-template")
        assert step.template_id == "my-template"

    def test_template_step_with_params(self):
        step = StepDef(
            name="ok",
            type="template",
            template_id="my-template",
            template_params={"agent": "robot-1"},
        )
        assert step.template_params == {"agent": "robot-1"}


class TestTemplateStepRecursionGuard:
    @pytest.mark.asyncio
    async def test_recursive_template_fails_with_depth_error(self):
        """A template that includes itself should fail, not loop forever."""
        engine, app = _make_engine()

        # Child template references itself
        recursive_def = _child_template_def([
            {"name": "recurse", "type": "template", "template_id": "child-tmpl"},
        ])
        parent = _make_template([
            {"name": "start", "type": "template", "template_id": "child-tmpl"},
        ])

        with _patch_db(), \
             patch.object(engine, "_sync_load_template", return_value=recursive_def):
            await engine.run("run-recursive", parent, {})

        ws: FakeWebSocketManager = app.state.ws_mgr
        types = [e["type"] for e in ws.events]
        assert "experiment_failed" in types
        failed = [e for e in ws.events if e["type"] == "experiment_failed"][0]
        assert "nesting too deep" in failed["error"]
