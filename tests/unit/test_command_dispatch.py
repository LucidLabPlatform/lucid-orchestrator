"""Unit tests for app/command_dispatch.py pure functions and send_command."""
from __future__ import annotations

import re
import uuid
from unittest.mock import MagicMock, patch

import pytest

from app.command_dispatch import command_payload, command_topic, send_command


# ── TestCommandTopic ───────────────────────────────────────────────────────────

class TestCommandTopic:
    def test_agent_simple_action(self):
        assert command_topic("a1", "ping") == "lucid/agents/a1/cmd/ping"

    def test_agent_slash_action(self):
        assert command_topic("a1", "cfg/set") == "lucid/agents/a1/cmd/cfg/set"

    def test_agent_multi_slash_action(self):
        assert command_topic("a1", "cfg/logging/set") == "lucid/agents/a1/cmd/cfg/logging/set"

    def test_component_action(self):
        assert command_topic("a1", "set-color", component_id="led_strip") == \
            "lucid/agents/a1/components/led_strip/cmd/set-color"

    def test_component_slash_action(self):
        assert command_topic("a1", "effect/glow", component_id="led_strip") == \
            "lucid/agents/a1/components/led_strip/cmd/effect/glow"

    def test_underscore_normalization(self):
        # underscore in action should become hyphen
        assert command_topic("a1", "set_color") == "lucid/agents/a1/cmd/set-color"


# ── TestCommandPayload ─────────────────────────────────────────────────────────

class TestCommandPayload:
    def test_no_body(self):
        result = command_payload("ping", {}, "req-1")
        assert result == {"action": "ping", "request_id": "req-1"}

    def test_with_body(self):
        result = command_payload("set-color", {"color": {"r": 255, "g": 0, "b": 0}}, "req-2")
        assert result["color"] == {"r": 255, "g": 0, "b": 0}
        assert result["action"] == "set-color"
        assert result["request_id"] == "req-2"

    def test_body_cannot_override_action(self):
        # action in body should be overwritten by the action parameter
        result = command_payload("ping", {"action": "SHOULD_BE_OVERWRITTEN"}, "req-3")
        assert result["action"] == "ping"


# ── TestSendCommand ────────────────────────────────────────────────────────────

@pytest.fixture
def mock_db():
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    with patch("app.command_dispatch.DB") as db:
        db.connect.return_value = mock_conn
        db.ensure_agent = MagicMock()
        db.ensure_component = MagicMock()
        yield db, mock_conn


class TestSendCommand:
    @pytest.mark.asyncio
    async def test_fire_and_forget(self, fake_app, mock_db):
        result = await send_command(fake_app, agent_id="a1", action="ping")
        bridge = fake_app.state.bridge
        assert len(bridge.published) == 1
        topic, payload = bridge.published[0]
        assert topic == "lucid/agents/a1/cmd/ping"
        assert payload["action"] == "ping"
        assert "request_id" in result

    @pytest.mark.asyncio
    async def test_wait_mode(self, fake_app, mock_db):
        result = await send_command(fake_app, agent_id="a1", action="ping", wait=True)
        bridge = fake_app.state.bridge
        # In wait mode bridge.publish should NOT be called directly
        assert len(bridge.published) == 0
        assert result["result"] == {"ok": True}

    @pytest.mark.asyncio
    async def test_auto_request_id(self, fake_app, mock_db):
        result = await send_command(fake_app, agent_id="a1", action="ping")
        rid = result["request_id"]
        # Must be a valid UUID
        uuid.UUID(rid)

    @pytest.mark.asyncio
    async def test_provided_request_id(self, fake_app, mock_db):
        result = await send_command(
            fake_app, agent_id="a1", action="ping", body={"request_id": "my-id"}
        )
        assert result["request_id"] == "my-id"
        _, payload = fake_app.state.bridge.published[0]
        assert payload["request_id"] == "my-id"

    @pytest.mark.asyncio
    async def test_component_dispatch(self, fake_app, mock_db):
        result = await send_command(
            fake_app, agent_id="a1", action="set-color", component_id="led_strip",
            body={"color": {"r": 255, "g": 0, "b": 0}},
        )
        topic, payload = fake_app.state.bridge.published[0]
        assert topic == "lucid/agents/a1/components/led_strip/cmd/set-color"
        assert payload["color"] == {"r": 255, "g": 0, "b": 0}

    @pytest.mark.asyncio
    async def test_agent_dispatch_does_not_call_ensure_agent(self, fake_app, mock_db):
        # Under the consolidated mqtt_users model the agent row must already
        # exist (sync owns it); the dispatch path does not pre-create it.
        db, conn = mock_db
        await send_command(fake_app, agent_id="a1", action="ping")
        db.ensure_agent.assert_not_called()

    @pytest.mark.asyncio
    async def test_db_ensure_component_called(self, fake_app, mock_db):
        db, conn = mock_db
        await send_command(
            fake_app, agent_id="a1", action="ping", component_id="led_strip"
        )
        db.ensure_component.assert_called_once()
        args = db.ensure_component.call_args[0]
        assert args[1] == "a1"
        assert args[2] == "led_strip"
