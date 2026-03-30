"""MQTT bridge: subscribes to all LUCID agent topics and enqueues parsed events.

Overview
--------
``MqttBridge`` wraps a ``paho.mqtt.client.Client`` (MQTTv5).  After calling
``start()``, paho's network loop runs in its own daemon thread.  Incoming
messages are parsed by ``parse_topic`` and placed on a ``queue.Queue`` that
the asyncio ``Broadcaster`` drains from the main process.

Request-response correlation
----------------------------
If a ``RequestResponseManager`` (``rrm``) is provided at construction time,
any incoming ``evt/*`` message whose JSON payload contains a ``request_id``
field will call ``rrm.resolve_threadsafe(request_id, payload)`` before
placing the event on the queue.  This resolves the corresponding
``asyncio.Future`` from the paho thread using
``loop.call_soon_threadsafe``, which is the only safe way to touch asyncio
primitives from outside the event loop.

Topic parsing
-------------
``parse_topic`` returns ``(agent_id, component_id | None, topic_type)``
for every topic matching the LUCID namespace ``lucid/agents/<id>/…``.
Returns ``None`` for any topic outside this namespace.

MQTT credentials are read from environment variables at module import time:
    LUCID_MQTT_HOST        (default: "localhost")
    LUCID_MQTT_PORT        (default: 1883)
    LUCID_MQTT_USERNAME    (default: "central-command")
    LUCID_MQTT_PASSWORD    (default: "")
"""
from __future__ import annotations

import json
import logging
import os
import queue
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import paho.mqtt.client as mqtt

if TYPE_CHECKING:
    from app.experiments.request_response import RequestResponseManager

log = logging.getLogger(__name__)

MQTT_HOST = os.environ["LUCID_MQTT_HOST"]
MQTT_PORT = int(os.environ["LUCID_MQTT_PORT"])
MQTT_USERNAME = os.environ["LUCID_MQTT_USERNAME"]
MQTT_PASSWORD = os.environ["LUCID_MQTT_PASSWORD"]

CC_SUBSCRIPTIONS = [
    ("lucid/agents/+/metadata", 1),
    ("lucid/agents/+/status", 1),
    ("lucid/agents/+/state", 1),
    ("lucid/agents/+/cfg", 1),
    ("lucid/agents/+/cfg/logging", 1),
    ("lucid/agents/+/cfg/telemetry", 1),
    ("lucid/agents/+/logs", 0),
    ("lucid/agents/+/telemetry/#", 0),
    ("lucid/agents/+/evt/#", 1),
    ("lucid/agents/+/components/+/metadata", 1),
    ("lucid/agents/+/components/+/status", 1),
    ("lucid/agents/+/components/+/state", 1),
    ("lucid/agents/+/components/+/cfg", 1),
    ("lucid/agents/+/components/+/cfg/logging", 1),
    ("lucid/agents/+/components/+/cfg/telemetry", 1),
    ("lucid/agents/+/components/+/logs", 0),
    ("lucid/agents/+/components/+/telemetry/#", 0),
    ("lucid/agents/+/components/+/evt/#", 1),
]


@dataclass
class MqttEvent:
    topic: str
    payload: Any          # parsed JSON dict/list or None
    raw: bytes
    agent_id: str
    component_id: str | None  # None for agent-level topics
    topic_type: str           # "status", "cfg/logging", "telemetry/cpu", "evt/ping/result", …
    scope: str                # "agent" | "component"


def parse_topic(topic: str) -> tuple[str, str | None, str] | None:
    """Return (agent_id, component_id, topic_type) or None if unrecognised."""
    parts = topic.split("/")
    # lucid/agents/<id>/...
    if len(parts) < 4 or parts[0] != "lucid" or parts[1] != "agents":
        return None
    agent_id = parts[2]
    rest = parts[3:]

    if rest and rest[0] == "components" and len(rest) >= 3:
        # lucid/agents/<id>/components/<cid>/...
        component_id = rest[1]
        topic_type = "/".join(rest[2:])
        return agent_id, component_id, topic_type

    topic_type = "/".join(rest)
    return agent_id, None, topic_type


class MqttBridge:
    """Paho MQTT client wrapper that subscribes to all LUCID agent topics.

    Attributes:
        _q:      Queue shared with the asyncio ``Broadcaster``.
        _rrm:    Optional ``RequestResponseManager`` for cmd ↔ evt correlation.
        _client: Underlying paho ``mqtt.Client`` instance.
    """

    def __init__(
        self,
        event_queue: queue.Queue,
        rrm: RequestResponseManager | None = None,
        client_id: str = "central-command",
    ) -> None:
        """
        Args:
            event_queue: Queue to place ``MqttEvent`` objects on (thread-safe).
            rrm:         Optional manager for request/response correlation.
            client_id:   MQTT client identifier sent to the broker.
        """
        self._q = event_queue
        self._rrm = rrm
        self._client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv5,
        )
        self._client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

    def start(self) -> None:
        """Connect to the broker and start the paho network loop in a daemon thread."""
        self._client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self._client.loop_start()

    def stop(self) -> None:
        """Stop the paho loop thread and disconnect cleanly from the broker."""
        self._client.loop_stop()
        self._client.disconnect()

    def publish(self, topic: str, payload: dict, qos: int = 1, retain: bool = False) -> None:
        """Publish a JSON-serialised payload to ``topic``.

        Args:
            topic:   Full MQTT topic string (must follow the LUCID topic schema).
            payload: Dict that will be JSON-serialised before publishing.
            qos:     MQTT QoS level (0 or 1; default 1 for command topics).
            retain:  Whether the broker should retain the message.
        """
        self._client.publish(topic, json.dumps(payload), qos=qos, retain=retain)

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        """Paho callback invoked after the TCP connection and CONNECT handshake.

        On success (reason_code == 0), subscribes to all 18 CC_SUBSCRIPTIONS
        patterns so no messages are missed during reconnect windows.
        """
        if reason_code == 0:
            log.info("MQTT connected")
            client.subscribe(CC_SUBSCRIPTIONS)
        else:
            log.error("MQTT connect failed: %s", reason_code)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        """Paho callback on disconnect.

        Fails all pending RRM futures so experiment steps don't hang
        indefinitely waiting for responses that will never arrive.
        """
        log.warning("MQTT disconnected: %s", reason_code)
        if self._rrm is not None:
            self._rrm.fail_all_pending(ConnectionError(f"MQTT disconnected: {reason_code}"))

    def _on_message(self, client, userdata, msg):
        """Paho callback invoked for every subscribed incoming message.

        1. Parses the topic into (agent_id, component_id, topic_type).
        2. JSON-decodes the payload; falls back to a decoded string on error.
        3. If an RRM is wired and the topic is ``evt/*``, resolves the matching
           pending Future via ``call_soon_threadsafe`` (thread-safe bridge to asyncio).
        4. Constructs an ``MqttEvent`` and places it on the queue.
           Drops the event silently if the queue is full (back-pressure).

        Args:
            client:   The paho Client instance (unused, paho convention).
            userdata: User-supplied data (unused).
            msg:      Paho ``MQTTMessage`` with ``.topic`` and ``.payload`` attributes.
        """
        parsed = parse_topic(msg.topic)
        if parsed is None:
            return
        agent_id, component_id, topic_type = parsed

        try:
            payload = json.loads(msg.payload) if msg.payload else None
        except (json.JSONDecodeError, ValueError):
            # Non-JSON payloads are stored as strings for inspection
            payload = msg.payload.decode(errors="replace")

        # Resolve pending request-response futures for evt/* messages.
        # resolve_threadsafe uses call_soon_threadsafe because paho runs in a
        # separate thread and asyncio Futures are not thread-safe.
        if self._rrm is not None and topic_type.startswith("evt/"):
            request_id = (payload or {}).get("request_id") if isinstance(payload, dict) else None
            if request_id:
                self._rrm.resolve_threadsafe(request_id, payload)

        event = MqttEvent(
            topic=msg.topic,
            payload=payload,
            raw=msg.payload,
            agent_id=agent_id,
            component_id=component_id,
            topic_type=topic_type,
            scope="component" if component_id else "agent",
        )
        try:
            self._q.put_nowait(event)
        except queue.Full:
            # Drop the event rather than block the paho network thread
            log.warning("Event queue full — dropping %s", msg.topic)
