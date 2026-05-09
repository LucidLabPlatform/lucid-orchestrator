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
import time
import uuid
from typing import Any

log = logging.getLogger(__name__)

# Maximum number of simultaneously tracked pending requests.
# When full, the oldest entry is evicted before registering a new one.
_PENDING_MAX = 1024


class RequestResponseManager:
    """Correlates published MQTT commands with their evt/* responses."""

    def __init__(self) -> None:
        self._pending: dict[str, asyncio.Future[Any]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws_mgr: Any | None = None  # WebSocketManager, injected after startup

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Register the running event loop. Called once at startup."""
        self._loop = loop

    def attach_ws_mgr(self, ws_mgr: Any) -> None:
        """Register the WebSocketManager for command_unanswered broadcasts."""
        self._ws_mgr = ws_mgr

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

        Five seconds before *timeout_s* fires, a ``command_unanswered`` WS
        event is broadcast so the operator dashboard can surface the stall
        before the full timeout elapses.

        Returns the response payload dict on success.
        Raises asyncio.TimeoutError if no response arrives within timeout_s.
        """
        raw_id = payload.get("request_id")
        request_id = str(uuid.uuid4()) if not raw_id or raw_id == "auto" else str(raw_id)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()

        # Evict the oldest pending entry if the dict is at capacity.
        if len(self._pending) >= _PENDING_MAX:
            oldest_id = next(iter(self._pending))
            log.warning(
                "RequestResponseManager at capacity (%d); evicting oldest request_id=%s",
                _PENDING_MAX,
                oldest_id,
            )
            evicted = self._pending.pop(oldest_id)
            if not evicted.done():
                evicted.cancel()

        self._pending[request_id] = future
        started_at = time.monotonic()
        try:
            bridge.publish(topic, {**payload, "request_id": request_id})

            warn_after = max(timeout_s - 5.0, timeout_s * 0.8)
            done, _ = await asyncio.wait(
                [asyncio.ensure_future(asyncio.shield(future))],
                timeout=warn_after,
            )
            if not done:
                # Broadcast an early warning before the hard timeout fires.
                elapsed = time.monotonic() - started_at
                await self._broadcast_unanswered(topic, request_id, elapsed, timeout_s)
                # Wait for the remaining time.
                remaining = timeout_s - elapsed
                done, _ = await asyncio.wait(
                    [asyncio.ensure_future(asyncio.shield(future))],
                    timeout=max(remaining, 0),
                )
                if not done:
                    log.warning(
                        "Timeout waiting for response to request_id=%s on %s",
                        request_id,
                        topic,
                    )
                    raise asyncio.TimeoutError

            return future.result()
        except asyncio.TimeoutError:
            raise
        finally:
            self._pending.pop(request_id, None)

    async def _broadcast_unanswered(
        self,
        topic: str,
        request_id: str,
        elapsed_s: float,
        timeout_s: float,
    ) -> None:
        """Broadcast a command_unanswered WS event if a WebSocketManager is wired."""
        if self._ws_mgr is None:
            return
        # Parse agent_id and action from the topic for context.
        # Topic form: lucid/agents/<id>/[components/<cid>/]cmd/<action...>
        parts = topic.split("/")
        try:
            agent_id = parts[2]
            action = "/".join(parts[parts.index("cmd") + 1:])
        except (IndexError, ValueError):
            agent_id = ""
            action = topic
        try:
            await self._ws_mgr.broadcast({
                "type": "command_unanswered",
                "agent_id": agent_id,
                "action": action,
                "request_id": request_id,
                "elapsed_s": round(elapsed_s, 2),
                "timeout_s": timeout_s,
            })
        except Exception:  # noqa: BLE001
            log.debug("Failed to broadcast command_unanswered event", exc_info=True)

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
