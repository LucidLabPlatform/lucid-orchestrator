"""Thread-safe WebSocket client manager with parallel broadcasting.

Replaces the bare ``set`` that was previously shared between the broadcaster
and the ``/api/ws`` endpoint.  All mutations are protected by an
``asyncio.Lock`` and sends happen concurrently via ``asyncio.gather``.
"""
from __future__ import annotations

import asyncio
import json
import logging

from starlette.websockets import WebSocket

log = logging.getLogger(__name__)


class WebSocketManager:
    """Manages connected WebSocket clients with safe concurrent access."""

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.add(ws)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)

    async def broadcast(self, event: dict) -> None:
        """Send *event* to every connected client in parallel.

        Dead clients are collected and removed after all sends complete.
        """
        async with self._lock:
            if not self._clients:
                return
            clients = list(self._clients)

        msg = json.dumps(event)

        async def _send(ws: WebSocket) -> WebSocket | None:
            try:
                await asyncio.wait_for(ws.send_text(msg), timeout=2.0)
            except Exception:
                return ws
            return None

        results = await asyncio.gather(*(_send(ws) for ws in clients))
        dead = {ws for ws in results if ws is not None}

        if dead:
            async with self._lock:
                self._clients -= dead
