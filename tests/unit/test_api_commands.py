"""HTTP integration tests — every UI command button → orchestrator → MQTT bridge."""
from __future__ import annotations

import pytest

# All tests use the `api_client` fixture from conftest.py (FakeBridge injected, DB patched).


# ── Agent-level commands ──────────────────────────────────────────────────────

class TestAgentCommands:
    def test_ping(self, api_client, bridge):
        res = api_client.post("/agents/agent1/cmd/ping", json={})
        assert res.status_code == 200
        data = res.json()
        assert "request_id" in data
        assert data["topic"] == "lucid/agents/agent1/cmd/ping"
        topic, payload = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/ping"
        assert payload["action"] == "ping"

    def test_restart(self, api_client, bridge):
        res = api_client.post("/agents/agent1/cmd/restart", json={})
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/restart"

    def test_refresh(self, api_client, bridge):
        res = api_client.post("/agents/agent1/cmd/refresh", json={})
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/refresh"

    def test_cfg_set(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/cmd/cfg/set",
            json={"set": {"heartbeat_s": 60}},
        )
        assert res.status_code == 200
        topic, payload = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/cfg/set"
        assert payload["set"]["heartbeat_s"] == 60

    def test_cfg_logging_set(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/cmd/cfg/logging/set",
            json={"set": {"log_level": "DEBUG"}},
        )
        assert res.status_code == 200
        topic, payload = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/cfg/logging/set"
        assert payload["set"]["log_level"] == "DEBUG"

    def test_cfg_telemetry_set(self, api_client, bridge):
        body = {"set": {"cpu_percent": {"enabled": True, "interval_s": 5}}}
        res = api_client.post("/agents/agent1/cmd/cfg/telemetry/set", json=body)
        assert res.status_code == 200
        topic, payload = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/cfg/telemetry/set"
        assert payload["set"]["cpu_percent"]["enabled"] is True

    def test_components_enable(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/cmd/components/enable",
            json={"component_id": "led_strip"},
        )
        assert res.status_code == 200
        topic, payload = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/components/enable"
        assert payload["component_id"] == "led_strip"

    def test_components_disable(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/cmd/components/disable",
            json={"component_id": "led_strip"},
        )
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/components/disable"

    def test_components_install(self, api_client, bridge):
        body = {"component_id": "led_strip", "source": {"url": "https://example.com/pkg.whl"}}
        res = api_client.post("/agents/agent1/cmd/components/install", json=body)
        assert res.status_code == 200
        topic, payload = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/components/install"
        assert payload["component_id"] == "led_strip"

    def test_core_upgrade(self, api_client, bridge):
        body = {"source": {"url": "https://example.com/core.whl"}}
        res = api_client.post("/agents/agent1/cmd/core/upgrade", json=body)
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/cmd/core/upgrade"


# ── Component-level commands ──────────────────────────────────────────────────

class TestComponentCommands:
    def test_component_ping(self, api_client, bridge):
        res = api_client.post("/agents/agent1/components/led_strip/cmd/ping", json={})
        assert res.status_code == 200
        topic, payload = bridge.published[0]
        assert topic == "lucid/agents/agent1/components/led_strip/cmd/ping"
        assert payload["action"] == "ping"

    def test_component_reset(self, api_client, bridge):
        res = api_client.post("/agents/agent1/components/led_strip/cmd/reset", json={})
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/components/led_strip/cmd/reset"

    def test_component_clear(self, api_client, bridge):
        res = api_client.post("/agents/agent1/components/led_strip/cmd/clear", json={})
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/components/led_strip/cmd/clear"

    def test_set_color(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/components/led_strip/cmd/set-color",
            json={"color": {"r": 255, "g": 0, "b": 0}},
        )
        assert res.status_code == 200
        topic, payload = bridge.published[0]
        assert topic == "lucid/agents/agent1/components/led_strip/cmd/set-color"
        assert payload["color"]["r"] == 255

    def test_effect_glow(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/components/led_strip/cmd/effect/glow",
            json={"color": {"r": 0, "g": 255, "b": 0}, "speed": 1.0},
        )
        assert res.status_code == 200
        topic, payload = bridge.published[0]
        # slash in action must be preserved
        assert topic == "lucid/agents/agent1/components/led_strip/cmd/effect/glow"
        assert payload["speed"] == 1.0

    def test_effect_rainbow(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/components/led_strip/cmd/effect/rainbow",
            json={"speed": 1.0},
        )
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/components/led_strip/cmd/effect/rainbow"

    def test_component_cfg_set(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/components/led_strip/cmd/cfg/set",
            json={"set": {"key": "value"}},
        )
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/components/led_strip/cmd/cfg/set"

    def test_component_cfg_logging_set(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/components/led_strip/cmd/cfg/logging/set",
            json={"set": {"log_level": "INFO"}},
        )
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/components/led_strip/cmd/cfg/logging/set"

    def test_component_cfg_telemetry_set(self, api_client, bridge):
        res = api_client.post(
            "/agents/agent1/components/led_strip/cmd/cfg/telemetry/set",
            json={"set": {"pixel_rgb": {"enabled": True}}},
        )
        assert res.status_code == 200
        topic, _ = bridge.published[0]
        assert topic == "lucid/agents/agent1/components/led_strip/cmd/cfg/telemetry/set"


# ── Batch commands ────────────────────────────────────────────────────────────

class TestBatchCommands:
    def test_batch_ping_two_agents(self, api_client, bridge):
        res = api_client.post(
            "/commands/batch",
            json={
                "action": "ping",
                "targets": [{"agent_id": "a1"}, {"agent_id": "a2"}],
                "payload": {},
            },
        )
        assert res.status_code == 200
        data = res.json()
        assert data["summary"]["total"] == 2
        assert data["summary"]["success"] == 2
        assert data["summary"]["failed"] == 0
        # Both agents must have been published to
        topics = [t for t, _ in bridge.published]
        assert "lucid/agents/a1/cmd/ping" in topics
        assert "lucid/agents/a2/cmd/ping" in topics

    def test_batch_component_command(self, api_client, bridge):
        res = api_client.post(
            "/commands/batch",
            json={
                "action": "clear",
                "targets": [
                    {"agent_id": "a1", "component_id": "led_strip"},
                    {"agent_id": "a2", "component_id": "led_strip"},
                ],
                "payload": {},
            },
        )
        assert res.status_code == 200
        data = res.json()
        assert data["summary"]["success"] == 2
        topics = [t for t, _ in bridge.published]
        assert "lucid/agents/a1/components/led_strip/cmd/clear" in topics
        assert "lucid/agents/a2/components/led_strip/cmd/clear" in topics
