"""MQTT request-response correlation for LUCID experiment commands.

The RequestResponseManager maps outgoing request_ids to asyncio Futures.
When an evt/* message arrives with a matching request_id, the Future is
resolved from the paho-mqtt thread using call_soon_threadsafe.

Usage:
    # Wiring (once, at startup)
    rrm = RequestResponseManager()
    bridge = MqttBridge(event_queue, rrm)

    # Sending a command and waiting for the result (from async context)
    result = await rrm.send_and_wait(
        bridge=bridge,
        topic="lucid/agents/robot-01/cmd/ping",
        payload={},
        timeout_s=10.0,
    )
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

log = logging.getLogger(__name__)


class RequestResponseManager:
    """Correlates published MQTT commands with their evt/* responses."""

    def __init__(self) -> None:
        self._pending: dict[str, asyncio.Future[Any]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Register the running event loop. Called once at startup."""
        self._loop = loop

    async def send_and_wait(
        self,
        bridge: Any,           # MqttBridge — typed as Any to avoid circular import
        topic: str,
        payload: dict,
        timeout_s: float = 30.0,
    ) -> dict:
        """Publish a command and wait for its evt/* response.

        Injects a unique request_id into the payload, registers a Future,
        publishes the message, then awaits the Future with a timeout.

        Returns the response payload dict on success.
        Raises asyncio.TimeoutError if no response arrives within timeout_s.
        """
        request_id = str(payload.get("request_id") or uuid.uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()

        self._pending[request_id] = future
        try:
            bridge.publish(topic, {**payload, "request_id": request_id})
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout_s)
        except asyncio.TimeoutError:
            log.warning("Timeout waiting for response to request_id=%s on %s", request_id, topic)
            raise
        finally:
            self._pending.pop(request_id, None)

    def fail_all_pending(self, exc: Exception) -> None:
        """Fail all pending futures with exc (e.g., on MQTT disconnect).

        Safe to call from any thread.
        """
        for future in list(self._pending.values()):
            if future.done():
                continue
            loop = future.get_loop()
            loop.call_soon_threadsafe(_safe_set_exception, future, exc)
        self._pending.clear()

    def resolve_threadsafe(self, request_id: str, result: Any) -> None:
        """Resolve a pending Future from outside the event loop (paho-mqtt thread).

        Safe to call from any thread. No-ops if no matching pending request.
        """
        future = self._pending.get(request_id)
        if future is None or future.done():
            return
        loop = future.get_loop()
        loop.call_soon_threadsafe(_safe_set_result, future, result)

    @property
    def pending_count(self) -> int:
        """Number of currently outstanding requests."""
        return len(self._pending)


def _safe_set_result(future: asyncio.Future, result: Any) -> None:
    """Set future result, ignoring InvalidStateError if already done."""
    if not future.done():
        future.set_result(result)


def _safe_set_exception(future: asyncio.Future, exc: Exception) -> None:
    """Set future exception, ignoring InvalidStateError if already done."""
    if not future.done():
        future.set_exception(exc)
