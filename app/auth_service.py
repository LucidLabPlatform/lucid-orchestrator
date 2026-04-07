"""HTTP client for lucid-auth."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class AuthServiceError(Exception):
    status_code: int
    detail: str

    def __str__(self) -> str:
        return self.detail


class AuthService:
    def __init__(self, base_url: str | None = None) -> None:
        self._base = (base_url or os.environ["LUCID_AUTH_URL"]).rstrip("/")

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            response = httpx.request(method, f"{self._base}{path}", timeout=30.0, **kwargs)
        except httpx.HTTPError as exc:
            raise AuthServiceError(status_code=502, detail=f"lucid-auth unreachable: {exc}") from exc

        if response.is_error:
            detail: str
            try:
                payload = response.json()
                detail = payload.get("detail") or response.text
            except ValueError:
                detail = response.text or f"lucid-auth error {response.status_code}"
            raise AuthServiceError(status_code=response.status_code, detail=detail)
        return response

    def list_agents(self) -> list[dict]:
        response = self._request("GET", "/agents")
        payload = response.json()
        return payload if isinstance(payload, list) else []

    def create_agent(self, agent_id: str) -> dict:
        return self._request("POST", f"/agents/{agent_id}").json()

    def delete_agent(self, agent_id: str) -> None:
        self._request("DELETE", f"/agents/{agent_id}")

    def create_cc(self) -> dict:
        return self._request("POST", "/cc").json()

    def delete_cc(self) -> None:
        self._request("DELETE", "/cc")

    def create_observer(self, username: str) -> dict:
        return self._request("POST", f"/observers/{username}").json()

    def delete_observer(self, username: str) -> None:
        self._request("DELETE", f"/observers/{username}")

    def get_mqtt_state(self) -> dict:
        return self._request("GET", "/mqtt-state").json()
