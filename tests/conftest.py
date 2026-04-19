"""Shared fixtures for lucid-orchestrator tests."""
from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


def pytest_configure(config):
    """Set required env vars before any app module is imported during collection."""
    os.environ.setdefault("LUCID_DB_URL", "postgresql://fake:fake@localhost/fake")
    os.environ.setdefault("EMQX_API_URL", "http://localhost:18083")
    os.environ.setdefault("EMQX_API_USERNAME", "fake")
    os.environ.setdefault("EMQX_API_PASSWORD", "fake")
    os.environ.setdefault("LUCID_MQTT_USERNAME", "fake")
    os.environ.setdefault("LUCID_AUTH_URL", "http://localhost:4000")


class FakeWebSocketManager:
    """Collects broadcast calls for assertion."""

    def __init__(self) -> None:
        self.events: list[dict] = []

    async def broadcast(self, event: dict) -> None:
        self.events.append(event)


class FakeBridge:
    """Minimal stand-in for MqttBridge used by engine tests."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []
        self._watchers: dict[int, tuple[str, object]] = {}
        self._next_watcher_id = 0

    def publish(self, topic: str, payload: dict) -> None:
        self.published.append((topic, payload))

    def add_telemetry_watcher(self, topic: str, callback) -> int:
        wid = self._next_watcher_id
        self._next_watcher_id += 1
        self._watchers[wid] = (topic, callback)
        return wid

    def remove_telemetry_watcher(self, watcher_id: int) -> None:
        self._watchers.pop(watcher_id, None)


class FakeRequestResponseManager:
    """Always resolves immediately with a canned result."""

    def __init__(self, result: dict | None = None) -> None:
        self._result = result or {"ok": True}

    async def send_and_wait(self, bridge, topic, payload, timeout_s=30.0) -> dict:
        return self._result


def make_app(
    ws_mgr: FakeWebSocketManager | None = None,
    bridge: FakeBridge | None = None,
    rrm: FakeRequestResponseManager | None = None,
) -> SimpleNamespace:
    """Build a fake ``app`` object matching what ExperimentEngine expects."""
    ws = ws_mgr or FakeWebSocketManager()
    br = bridge or FakeBridge()
    rm = rrm or FakeRequestResponseManager()
    return SimpleNamespace(state=SimpleNamespace(ws_mgr=ws, bridge=br, rrm=rm))


@pytest.fixture
def ws_mgr():
    return FakeWebSocketManager()


@pytest.fixture
def bridge():
    return FakeBridge()


@pytest.fixture
def rrm():
    return FakeRequestResponseManager()


@pytest.fixture
def fake_app(ws_mgr, bridge, rrm):
    return make_app(ws_mgr=ws_mgr, bridge=bridge, rrm=rrm)


@pytest.fixture
def api_client(bridge, rrm, ws_mgr):
    """TestClient with FakeBridge injected; DB patched to no-op."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from unittest.mock import MagicMock, patch
    from app.routes.api import router

    test_app = FastAPI()
    test_app.include_router(router)
    test_app.state.bridge = bridge
    test_app.state.rrm = rrm
    test_app.state.ws_mgr = ws_mgr

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor = MagicMock(return_value=mock_conn)
    mock_conn.__iter__ = MagicMock(return_value=iter([]))
    mock_conn.fetchall = MagicMock(return_value=[])
    mock_conn.fetchone = MagicMock(return_value=None)

    with patch("app.command_dispatch.DB") as mock_db:
        mock_db.connect.return_value = mock_conn
        mock_db.ensure_agent = MagicMock()
        mock_db.ensure_component = MagicMock()
        with TestClient(test_app) as client:
            yield client
