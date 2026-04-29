"""TopicLinkManager — creates and manages EMQX Rule Engine republish rules.

Each topic link is backed by an EMQX rule that routes messages from a
source topic to a target topic, entirely inside the broker with sub-ms
latency. Central Command manages rule lifecycle via the EMQX REST API.

Auth strategy: obtain a JWT from POST /api/v5/login on first use, cache
it, and refresh transparently on 401.
"""
from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass
from typing import Any

import httpx

log = logging.getLogger(__name__)
_LINK_SQL_RE = re.compile(r'^SELECT\s+(?P<select>.+?)\s+FROM\s+"(?P<source>.+)"$', re.DOTALL)
_PASS_THROUGH_PAYLOAD = "${payload}"

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class TopicLinkDef:
    """Describes a single topic link (EMQX republish rule)."""
    name: str
    source_topic: str
    target_topic: str
    # SELECT clause for the EMQX rule SQL (default passes everything through)
    select_clause: str = "*"
    # Optional Jinja-style payload template; None = pass payload as-is
    payload_template: str | None = None
    qos: int = 0
    # Set by manager after rule creation
    emqx_rule_id: str | None = None


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

class TopicLinkManager:
    """Manages EMQX Rule Engine rules for topic-to-topic bridging.

    Intended to be instantiated once in main.py and attached to app.state.
    All public methods are sync — call them from FastAPI endpoints directly
    or via run_in_executor from async contexts.
    """

    def __init__(self, api_url: str, username: str, password: str) -> None:
        self._api = api_url.rstrip("/")
        self._username = username
        self._password = password
        self._token: str | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _login(self) -> str:
        """Obtain a fresh JWT from the EMQX management API."""
        resp = httpx.post(
            f"{self._api}/login",
            json={"username": self._username, "password": self._password},
            timeout=10,
        )
        resp.raise_for_status()
        token = resp.json().get("token")
        if not token:
            raise RuntimeError(f"EMQX login returned no token: {resp.text}")
        return token

    def _headers(self) -> dict[str, str]:
        with self._lock:
            if not self._token:
                self._token = self._login()
        return {"Authorization": f"Bearer {self._token}"}

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Make an authenticated request, refreshing JWT on 401."""
        url = f"{self._api}{path}"
        resp = httpx.request(method, url, headers=self._headers(), timeout=10, **kwargs)
        if resp.status_code == 401:
            with self._lock:
                self._token = self._login()
            resp = httpx.request(method, url, headers=self._headers(), timeout=10, **kwargs)
        return resp

    # ------------------------------------------------------------------
    # Rule construction
    # ------------------------------------------------------------------

    def _rule_payload(self, link: TopicLinkDef) -> dict[str, Any]:
        """Build the EMQX rule JSON payload for a republish action."""
        # Escape single quotes in topic for EMQX SQL
        sql = (
            f'SELECT {link.select_clause} '
            f'FROM "{link.source_topic}"'
        )
        payload_tpl = link.payload_template or _PASS_THROUGH_PAYLOAD
        return {
            "sql": sql,
            "actions": [
                {
                    "function": "republish",
                    "args": {
                        "topic": link.target_topic,
                        "payload": payload_tpl,
                        "qos": link.qos,
                        "retain": False,
                    },
                }
            ],
            "enable": True,
            "description": f"LUCID topic link: {link.name}",
        }

    def _parse_link_rule(self, rule: dict[str, Any]) -> dict[str, Any] | None:
        actions = rule.get("actions", [])
        if len(actions) != 1:
            return None
        action = actions[0]
        if not isinstance(action, dict):
            return None
        if action.get("function") != "republish":
            return None

        sql = str(rule.get("sql", "")).strip()
        match = _LINK_SQL_RE.match(sql)
        if not match:
            return None

        args = action.get("args", {})
        if not isinstance(args, dict):
            return None
        target_topic = args.get("topic")
        if not target_topic:
            return None

        description = str(rule.get("description") or "").strip()
        name = description.removeprefix("LUCID topic link: ").strip() if description else ""
        if not name:
            name = str(rule.get("id", "topic-link"))

        payload_template = args.get("payload")
        if payload_template == _PASS_THROUGH_PAYLOAD:
            payload_template = None

        return {
            "emqx_rule_id": rule.get("id"),
            "name": name,
            "source_topic": match.group("source").strip(),
            "target_topic": str(target_topic),
            "select_clause": match.group("select").strip(),
            "payload_template": payload_template,
            "qos": int(args.get("qos", 0) or 0),
            "enabled": bool(rule.get("enable", True)),
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_link(self, link: TopicLinkDef) -> str:
        """Create an EMQX republish rule. Returns the EMQX rule_id."""
        resp = self._request("POST", "/rules", json=self._rule_payload(link))
        if not resp.is_success:
            raise RuntimeError(
                f"EMQX create rule failed ({resp.status_code}): {resp.text}"
            )
        rule_id: str = resp.json()["id"]
        link.emqx_rule_id = rule_id
        log.info("Created EMQX rule %s for link '%s'", rule_id, link.name)
        return rule_id

    def activate_link(self, rule_id: str) -> None:
        """Enable an existing EMQX rule."""
        resp = self._request("PUT", f"/rules/{rule_id}", json={"enable": True})
        if not resp.is_success:
            raise RuntimeError(
                f"EMQX activate rule {rule_id} failed ({resp.status_code}): {resp.text}"
            )
        log.info("Activated EMQX rule %s", rule_id)

    def deactivate_link(self, rule_id: str) -> None:
        """Disable an existing EMQX rule (keeps it for reuse)."""
        resp = self._request("PUT", f"/rules/{rule_id}", json={"enable": False})
        if not resp.is_success:
            raise RuntimeError(
                f"EMQX deactivate rule {rule_id} failed ({resp.status_code}): {resp.text}"
            )
        log.info("Deactivated EMQX rule %s", rule_id)

    def delete_link(self, rule_id: str) -> None:
        """Delete an EMQX rule entirely."""
        resp = self._request("DELETE", f"/rules/{rule_id}")
        if not resp.is_success and resp.status_code != 404:
            raise RuntimeError(
                f"EMQX delete rule {rule_id} failed ({resp.status_code}): {resp.text}"
            )
        log.info("Deleted EMQX rule %s", rule_id)

    def get_rule_metrics(self, rule_id: str) -> dict[str, Any]:
        """Return EMQX rule execution metrics."""
        resp = self._request("GET", f"/rules/{rule_id}/metrics")
        if not resp.is_success:
            raise RuntimeError(
                f"EMQX metrics for rule {rule_id} failed ({resp.status_code}): {resp.text}"
            )
        return resp.json()

    def get_rule(self, rule_id: str) -> dict[str, Any]:
        """Return EMQX rule details (includes enabled status)."""
        resp = self._request("GET", f"/rules/{rule_id}")
        resp.raise_for_status()
        return resp.json()

    def list_links(self) -> list[dict[str, Any]]:
        """Return all broker rules that can be represented as topic links."""
        resp = self._request("GET", "/rules?limit=500")
        resp.raise_for_status()
        payload = resp.json()
        rules = payload.get("data", payload) if isinstance(payload, dict) else payload
        parsed: list[dict[str, Any]] = []
        for item in rules:
            if not isinstance(item, dict):
                continue
            link = self._parse_link_rule(item)
            if link is not None:
                parsed.append(link)
        parsed.sort(key=lambda item: (item["name"], item["emqx_rule_id"] or ""))
        return parsed
