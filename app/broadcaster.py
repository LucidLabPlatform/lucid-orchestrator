"""Async broadcaster: drains the MQTT event queue and fans out to WebSocket clients.

Architecture note
-----------------
Database persistence is handled entirely by the EMQX Rule Engine
(``services/emqx/setup_rules.py``).  This module exists solely to push
live events to the browser dashboard via WebSocket.

The ``Broadcaster`` runs as a single asyncio task inside the FastAPI process.
It blocks on ``queue.Queue.get`` with a 0.1-second timeout using
``run_in_executor`` so that the event loop is not starved — the executor thread
parks, and the loop stays free for HTTP/WebSocket handlers.

WebSocket client lifecycle
--------------------------
Connected clients are tracked in a plain ``set`` that is shared with the
``/api/ws`` WebSocket endpoint (see ``app/routes/api.py``).  When a send
fails (e.g. client navigated away), the dead socket is silently removed from
the set.  No reconnection logic is implemented here; the browser JS handles
reconnecting.
"""
import asyncio
import json
import logging
import queue
from datetime import datetime, timezone

from app.mqtt_bridge import MqttEvent

log = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Broadcaster:
    """Drains the MQTT event queue and pushes events to all connected WebSocket clients.

    Attributes:
        _q:          Thread-safe queue populated by ``MqttBridge._on_message``.
        _ws_clients: Shared set of live ``WebSocket`` objects.
        _running:    Loop control flag; set to ``False`` by ``stop()``.
    """

    def __init__(self, event_queue: queue.Queue, ws_clients: set) -> None:
        """
        Args:
            event_queue: Queue of ``MqttEvent`` objects produced by the MQTT bridge.
            ws_clients:  Mutable set of active FastAPI ``WebSocket`` connections.
        """
        self._q = event_queue
        self._ws_clients = ws_clients
        self._running = False

    async def run(self) -> None:
        """Main loop — run as an asyncio task.

        Blocks on the queue with a 0.1 s timeout via ``run_in_executor`` so the
        event loop stays responsive.  Exceptions from ``_handle`` are logged
        but do not terminate the loop.
        """
        self._running = True
        while self._running:
            try:
                # Block in a thread so the event loop is not starved
                event: MqttEvent = await asyncio.get_event_loop().run_in_executor(
                    None, self._q.get, True, 0.1
                )
            except queue.Empty:
                continue
            except Exception:
                continue
            try:
                await self._handle(event)
            except Exception as exc:
                log.exception("Broadcaster error on %s: %s", event.topic, exc)

    def stop(self) -> None:
        """Signal the run loop to exit after the current iteration."""
        self._running = False

    async def _handle(self, event: MqttEvent) -> None:
        """Convert an ``MqttEvent`` to a JSON WebSocket frame and broadcast it.

        Args:
            event: Parsed MQTT event from the bridge.

        Side effects:
            Calls ``_broadcast`` which sends to all connected WebSocket clients
            and removes any that have disconnected.
        """
        ts = _now()
        ws_event = {
            "type":         "mqtt",
            "topic":        event.topic,
            "agent_id":     event.agent_id,
            "component_id": event.component_id,
            "topic_type":   event.topic_type,
            "scope":        event.scope,
            "payload":      event.payload,
            "ts":           ts.isoformat(),
        }
        await self._broadcast(ws_event)

    async def _broadcast(self, event: dict) -> None:
        """Send a JSON-serialised event to every connected WebSocket client.

        Dead clients (those that raise on send) are collected and removed from
        ``_ws_clients`` in one batch after the send loop completes, avoiding
        mutation during iteration.

        Args:
            event: Dict that will be serialised to JSON and sent as a text frame.
        """
        if not self._ws_clients:
            return
        msg = json.dumps(event)
        dead = set()
        for ws in list(self._ws_clients):
            try:
                await asyncio.wait_for(ws.send_text(msg), timeout=2.0)
            except Exception:
                dead.add(ws)
        # Remove dead clients after iteration to avoid mutating the set mid-loop
        self._ws_clients -= dead
