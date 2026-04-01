from __future__ import annotations

import asyncio
import json


async def broadcast_ws(ws_clients: set, event: dict) -> None:
    msg = json.dumps(event)
    dead = set()
    for ws in list(ws_clients):
        try:
            await asyncio.wait_for(ws.send_text(msg), timeout=2.0)
        except Exception:
            dead.add(ws)
    ws_clients -= dead
