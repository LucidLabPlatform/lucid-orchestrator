"""MQTT ↔ HTTP bridge for the lucid-esp-agent voice round trip.

The ESP firmware speaks MQTT only; lucid-voice and lucid-ai are HTTP-only.
This module subscribes to a single MQTT cmd topic per agent, runs the
STT → chat → TTS HTTP fan-out internally, and publishes the result back
on the matching evt topic.

Wire contract (mirrors agent's contract style):

  cmd  lucid/agents/<id>/components/ai_session/cmd/voice_round_trip
       { "request_id": "<uuid>",
         "audio_b64":  "<base64 wav bytes>",
         "session_id": "<uuid>",
         "audio_format": "wav" }

  evt  lucid/agents/<id>/components/ai_session/evt/voice_round_trip/result
       { "request_id": "<uuid>",
         "ok": <bool>,
         "error": "<string>",                  # only meaningful when ok=false
         "transcript": "<str>",                # only on ok=true
         "ai_text":    "<str>",                # only on ok=true
         "audio_b64":  "<base64 wav>",         # only on ok=true
         "audio_format": "wav",
         "audio_sample_rate_hz": 22050 }

QoS 1 / retain false on both directions.  EMQX's `mqtt.max_packet_size`
must be ≥ 4 MB to fit a worst-case TTS reply (configured in compose.yaml).
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)


VOICE_HOST = os.environ.get("LUCID_VOICE_HOST", "lucid-voice")
VOICE_PORT = int(os.environ.get("LUCID_VOICE_PORT", "5100"))
AI_HOST = os.environ.get("LUCID_AI_HOST", "lucid-ai")
AI_PORT = int(os.environ.get("LUCID_AI_PORT", "5000"))

# Per-stage timeouts.  Together they bound the worst-case time before the
# ESP gets a result event back (and stops showing "Thinking…").
STT_TIMEOUT_S = 30.0
AI_TIMEOUT_S = 90.0
TTS_TIMEOUT_S = 30.0


def _result_topic(agent_id: str) -> str:
    return (
        f"lucid/agents/{agent_id}"
        f"/components/ai_session/evt/voice_round_trip/result"
    )


def _log_bridge_stage(stage: str, t0: float, request_id: str, extra: dict) -> None:
    """Emit a voice_stage_done JSON log line for a bridge stage."""
    log.info(json.dumps({
        "event": "voice_stage_done",
        "stage": stage,
        "elapsed_ms": int((time.monotonic() - t0) * 1000),
        "request_id": request_id,
        "extra": extra,
    }))


class VoiceBridge:
    """Owns the HTTP fan-out for one orchestrator instance.

    Lives for the lifetime of the FastAPI app; instantiated once in
    ``main.py``'s lifespan and handed the shared MqttBridge + httpx client.
    The MqttBridge calls ``handle_message`` from its paho thread; we marshal
    onto the asyncio loop via ``run_coroutine_threadsafe``.
    """

    def __init__(
        self,
        mqtt_bridge: Any,
        http_client: httpx.AsyncClient,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._mqtt = mqtt_bridge
        self._http = http_client
        self._loop = loop

    # Called from the paho MQTT thread.  Schedule async work on the loop.
    def handle_message(self, agent_id: str, payload: dict | None) -> None:
        if not isinstance(payload, dict):
            log.warning("voice_bridge: non-dict payload from %s", agent_id)
            return
        asyncio.run_coroutine_threadsafe(
            self._handle_async(agent_id, payload), self._loop
        )

    async def _handle_async(self, agent_id: str, payload: dict) -> None:
        request_id = str(payload.get("request_id", ""))
        result_topic = _result_topic(agent_id)
        rid_headers = {"X-Request-ID": request_id} if request_id else {}
        t_total = time.monotonic()
        try:
            audio_b64 = payload.get("audio_b64", "")
            session_id = str(payload.get("session_id") or "default")
            if not audio_b64:
                raise ValueError("missing audio_b64")

            wav_bytes = base64.b64decode(audio_b64, validate=False)
            log.info(
                "voice_bridge[%s] start request_id=%s audio_bytes=%d",
                agent_id, request_id, len(wav_bytes),
            )

            # 1) STT
            files = {"audio": ("speech.wav", wav_bytes, "audio/wav")}
            t0 = time.monotonic()
            r = await self._http.post(
                f"http://{VOICE_HOST}:{VOICE_PORT}/api/voice/stt",
                files=files,
                headers=rid_headers,
                timeout=STT_TIMEOUT_S,
            )
            r.raise_for_status()
            transcript = (r.json() or {}).get("text", "").strip()
            _log_bridge_stage("stt", t0, request_id, {"agent_id": agent_id, "audio_bytes": len(wav_bytes)})
            log.info("voice_bridge[%s] stt=%r", agent_id, transcript[:80])
            if not transcript:
                raise ValueError("stt returned empty transcript")

            # 2) AI chat (non-streaming for simplicity in v1)
            t0 = time.monotonic()
            r = await self._http.post(
                f"http://{AI_HOST}:{AI_PORT}/api/ai/chat",
                json={"message": transcript, "session_id": session_id, "request_id": request_id},
                timeout=AI_TIMEOUT_S,
            )
            r.raise_for_status()
            ai_text = (r.json() or {}).get("response", "").strip()
            _log_bridge_stage("ai_total", t0, request_id, {"agent_id": agent_id, "transcript_len": len(transcript)})
            log.info("voice_bridge[%s] ai=%r", agent_id, ai_text[:80])
            if not ai_text:
                raise ValueError("ai returned empty response")

            # 3) TTS
            t0 = time.monotonic()
            r = await self._http.post(
                f"http://{VOICE_HOST}:{VOICE_PORT}/api/voice/tts",
                json={"text": ai_text, "length_scale": 1.0},
                headers=rid_headers,
                timeout=TTS_TIMEOUT_S,
            )
            r.raise_for_status()
            tts_wav = r.content
            tts_b64 = base64.b64encode(tts_wav).decode("ascii")
            _log_bridge_stage("tts", t0, request_id, {"agent_id": agent_id, "text_len": len(ai_text), "audio_bytes": len(tts_wav)})
            log.info(
                "voice_bridge[%s] tts_bytes=%d → publishing result",
                agent_id, len(tts_wav),
            )

            self._mqtt.publish(
                result_topic,
                {
                    "request_id": request_id,
                    "ok": True,
                    "error": "",
                    "transcript": transcript,
                    "ai_text": ai_text,
                    "audio_b64": tts_b64,
                    "audio_format": "wav",
                    "audio_sample_rate_hz": 22050,
                },
                qos=1,
                retain=False,
            )
            _log_bridge_stage(
                "bridge_total", t_total, request_id,
                {"agent_id": agent_id, "audio_in_bytes": len(wav_bytes), "audio_out_bytes": len(tts_wav)},
            )
        except httpx.HTTPStatusError as exc:
            err = f"http_{exc.response.status_code}"
            log.warning("voice_bridge[%s] %s: %s", agent_id, err, exc)
            self._publish_error(result_topic, request_id, err)
        except (httpx.HTTPError, asyncio.TimeoutError) as exc:
            err = f"http_error:{type(exc).__name__}"
            log.warning("voice_bridge[%s] %s: %s", agent_id, err, exc)
            self._publish_error(result_topic, request_id, err)
        except Exception as exc:  # noqa: BLE001
            log.exception("voice_bridge[%s] unexpected error", agent_id)
            self._publish_error(result_topic, request_id, str(exc)[:120] or "internal_error")

    def _publish_error(self, topic: str, request_id: str, error: str) -> None:
        self._mqtt.publish(
            topic,
            {"request_id": request_id, "ok": False, "error": error},
            qos=1,
            retain=False,
        )
